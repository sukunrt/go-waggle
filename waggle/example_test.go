package waggle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sukunrt/go-waggle/conn"
	"github.com/sukunrt/go-waggle/pb"
)

type metadata struct {
	Available []string `json:"available"`
	Request   []string `json:"request"`
}

type pieces struct {
	Messages []message `json:"messages"`
}

type message struct {
	ObjectID  string `json:"object_id"`
	MessageID string `json:"message_id"`
	Message   []byte `json:"message"`
}

type broadcaster struct {
	Messages map[string]map[string]message // objectid => messageid => message
	Node     node
	Topic    *Topic

	t        *testing.T
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	start    sync.Once
	publish  chan publishRequest
	incoming chan incomingMessage
	received chan message
	peers    map[conn.PeerID]map[string]*peerObject
}

type publishRequest struct {
	Message message
	Done    chan struct{}
}

type peerObject struct {
	available  map[string]bool
	requested  map[string]bool
	requesting map[string]bool
	advertised int
}

func newBroadcaster(t *testing.T, n node) *broadcaster {
	t.Helper()
	topic, err := n.Waggle.NewTopic("broadcast", 0, nil)
	require.NoError(t, err)
	topic.Subscribe()
	ctx, cancel := context.WithCancel(t.Context())
	b := &broadcaster{
		Messages: make(map[string]map[string]message),
		Node:     n,
		Topic:    topic,
		t:        t,
		ctx:      ctx,
		cancel:   cancel,
		publish:  make(chan publishRequest),
		incoming: make(chan incomingMessage),
		received: make(chan message),
		peers:    make(map[conn.PeerID]map[string]*peerObject),
	}
	t.Cleanup(func() {
		b.cancel()
		b.Topic.Close()
		b.wg.Wait()
	})
	return b
}

func connectBroadcasters(a, b *broadcaster) {
	a.t.Helper()
	require.NoError(a.t, dialBroadcasters(a, b))
}

func dialBroadcasters(a, b *broadcaster) error {
	c1, err := a.Node.DialPeer(a.ctx, b.Node.P)
	if err != nil {
		return err
	}
	a.t.Cleanup(func() { c1.CloseWithError(0, "") })
	c2, err := b.Node.Accept(b.ctx)
	if err != nil {
		return err
	}
	b.t.Cleanup(func() { c2.CloseWithError(0, "") })
	a.addPeer(b.Node.P, c1)
	b.addPeer(a.Node.P, c2)
	return nil
}

func (b *broadcaster) addPeer(p conn.PeerID, c conn.Conn) {
	b.Node.Waggle.AddPeer(p, c)
	b.wg.Go(func() {
		defer b.Node.Waggle.DisconnectPeer(p, c)
		defer c.CloseWithError(0, "")
		for {
			s, err := c.AcceptStream(b.ctx)
			if err != nil {
				return
			}
			if _, err := b.Node.Waggle.HandleInboundStream(p, s); err != nil && b.ctx.Err() == nil {
				b.t.Errorf("handle inbound stream: %v", err)
			}
		}
	})
}

func (b *broadcaster) Start() {
	b.start.Do(func() {
		b.wg.Go(func() {
			for {
				p, msg := b.Topic.ReceiveMessage()
				if msg == nil {
					return
				}
				select {
				case b.incoming <- incomingMessage{P: p, Message: msg}:
				case <-b.ctx.Done():
					return
				}
			}
		})
		b.wg.Go(b.loop)
	})
}

func (b *broadcaster) Publish(objectID string, msg []byte) {
	b.t.Helper()
	require.NotEmpty(b.t, msg, "message payload must not be empty")
	req := publishRequest{Message: makeMessage(objectID, msg), Done: make(chan struct{})}
	select {
	case b.publish <- req:
	case <-b.ctx.Done():
		return
	}
	select {
	case <-req.Done:
	case <-b.ctx.Done():
	}
}

func (b *broadcaster) Receive() (objectID string, msg message) {
	select {
	case msg = <-b.received:
		msg.Message = bytes.Clone(msg.Message)
		return msg.ObjectID, msg
	case <-b.ctx.Done():
		return "", message{}
	}
}

func makeMessage(objectID string, payload []byte) message {
	digest := sha256.Sum256(payload)
	return message{
		ObjectID:  objectID,
		MessageID: hex.EncodeToString(digest[:]),
		Message:   bytes.Clone(payload),
	}
}

// The loop owns Messages and peers. The application must call Receive in a loop.
func (b *broadcaster) loop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case req := <-b.publish:
			b.store(req.Message)
			close(req.Done)
		case in := <-b.incoming:
			b.handleMessage(in)
		case <-ticker.C:
			b.sendPending()
		}
	}
}

func (b *broadcaster) store(msg message) bool {
	if b.Messages[msg.ObjectID] == nil {
		b.Messages[msg.ObjectID] = make(map[string]message)
	}
	if _, ok := b.Messages[msg.ObjectID][msg.MessageID]; ok {
		return false
	}
	b.Messages[msg.ObjectID][msg.MessageID] = msg
	return true
}

func (b *broadcaster) peerObject(p conn.PeerID, objectID string) *peerObject {
	if b.peers[p] == nil {
		b.peers[p] = make(map[string]*peerObject)
	}
	if b.peers[p][objectID] == nil {
		b.peers[p][objectID] = &peerObject{
			available:  make(map[string]bool),
			requested:  make(map[string]bool),
			requesting: make(map[string]bool),
		}
	}
	return b.peers[p][objectID]
}

func (b *broadcaster) handleMessage(in incomingMessage) {
	var md metadata
	if err := json.Unmarshal(in.Message.PiecesMetadata, &md); err != nil {
		b.t.Errorf("decode metadata: %v", err)
		return
	}
	objectID := string(in.Message.ObjectID)
	state := b.peerObject(in.P, objectID)
	for _, id := range md.Available {
		state.available[id] = true
	}
	for _, id := range md.Request {
		state.requested[id] = true
	}
	if len(in.Message.Pieces) == 0 {
		return
	}
	var batch pieces
	if err := json.Unmarshal(in.Message.Pieces, &batch); err != nil {
		b.t.Errorf("decode pieces: %v", err)
		return
	}
	for _, msg := range batch.Messages {
		if len(msg.Message) == 0 || msg.ObjectID != objectID || makeMessage(objectID, msg.Message).MessageID != msg.MessageID {
			b.t.Errorf("invalid message in pieces for object %q", objectID)
			continue
		}
		state.available[msg.MessageID] = true
		delete(state.requesting, msg.MessageID)
		if b.store(msg) {
			select {
			case b.received <- msg:
			case <-b.ctx.Done():
				return
			}
		}
	}
}

func (b *broadcaster) sendPending() {
	for _, p := range b.Topic.PeersWithCapacity() {
		for objectID := range b.Messages {
			b.peerObject(p, objectID)
		}
		for objectID, state := range b.peers[p] {
			messages := b.Messages[objectID]
			var md metadata
			for id := range messages {
				md.Available = append(md.Available, id)
			}
			for id := range state.available {
				if _, have := messages[id]; !have && !state.requesting[id] {
					md.Request = append(md.Request, id)
				}
			}
			var batch pieces
			for id := range state.requested {
				if msg, have := messages[id]; have {
					batch.Messages = append(batch.Messages, msg)
				}
			}
			if len(batch.Messages) == 0 && len(md.Request) == 0 && state.advertised == len(messages) {
				continue
			}
			encoded, err := json.Marshal(md)
			if err != nil {
				b.t.Errorf("encode metadata: %v", err)
				return
			}
			var encodedPieces []byte
			if len(batch.Messages) > 0 {
				encodedPieces, err = json.Marshal(batch)
				if err != nil {
					b.t.Errorf("encode pieces: %v", err)
					return
				}
			}
			b.Topic.SendMessage(p, &pb.ObjectPieces{
				ObjectID:       []byte(objectID),
				PiecesMetadata: encoded,
				Pieces:         encodedPieces,
			})
			state.advertised = len(messages)
			for _, id := range md.Request {
				state.requesting[id] = true
			}
			for _, msg := range batch.Messages {
				delete(state.requested, msg.MessageID)
			}
			// Send at most one message per available peer per tick.
			break
		}
	}
}

func TestBroadcasterPieces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tn := newTestNet(t, 2)
		sender := newBroadcaster(t, *tn.Nodes[0])
		receiver := newBroadcaster(t, *tn.Nodes[1])
		sender.Start()

		messages := []message{
			makeMessage("object1", []byte("first")),
			makeMessage("object1", []byte("second")),
			makeMessage("object1", []byte("third")),
		}
		var ids []string
		for _, msg := range messages {
			sender.Publish(msg.ObjectID, msg.Message)
			ids = append(ids, msg.MessageID)
		}
		connectBroadcasters(sender, receiver)
		timeout := time.AfterFunc(5*time.Second, receiver.Topic.Close)
		defer timeout.Stop()

		// Read the topic directly to check the contents of individual wire messages.
		p, advertisement := receiver.Topic.ReceiveMessage()
		require.Equal(t, sender.Node.P, p)
		require.NotNil(t, advertisement)
		var md metadata
		require.NoError(t, json.Unmarshal(advertisement.PiecesMetadata, &md))
		require.ElementsMatch(t, ids, md.Available)
		require.Empty(t, advertisement.Pieces)

		synctest.Wait()
		require.Contains(t, receiver.Topic.PeersWithCapacity(), sender.Node.P)
		request, err := json.Marshal(metadata{Request: ids})
		require.NoError(t, err)
		receiver.Topic.SendMessage(sender.Node.P, &pb.ObjectPieces{
			ObjectID:       []byte("object1"),
			PiecesMetadata: request,
		})

		p, response := receiver.Topic.ReceiveMessage()
		require.Equal(t, sender.Node.P, p)
		require.NotNil(t, response)
		require.Equal(t, []byte("object1"), response.ObjectID)
		var batch pieces
		require.NoError(t, json.Unmarshal(response.Pieces, &batch))
		require.ElementsMatch(t, messages, batch.Messages)
	})
}

func TestBroadcasterRandomNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const numBroadcasters = 100
		const numPeers = 20
		tn := newTestNet(t, numBroadcasters)
		broadcasters := make([]*broadcaster, numBroadcasters)
		messages := make([]message, numBroadcasters)
		expected := make(map[string]message, numBroadcasters)
		received := make([][]message, numBroadcasters)
		var receivers sync.WaitGroup
		for i, n := range tn.Nodes {
			b := newBroadcaster(t, *n)
			broadcasters[i] = b
			b.Start()
			messages[i] = makeMessage("object1", []byte(n.P))
			expected[messages[i].MessageID] = messages[i]
			received[i] = make([]message, numBroadcasters-1)
			receivers.Go(func() {
				for j := range received[i] {
					_, received[i][j] = b.Receive()
				}
			})
		}
		timeout := time.AfterFunc(2*time.Minute, func() {
			for _, b := range broadcasters {
				b.cancel()
			}
		})
		defer timeout.Stop()

		// A shuffled ring with ten forward links gives each node twenty distinct
		// peers, keeps the graph connected, and creates each connection only once.
		rng := rand.New(rand.NewPCG(1, 2))
		order := rng.Perm(numBroadcasters)
		for offset := 1; offset <= numPeers/2; offset++ {
			var connectors sync.WaitGroup
			errs := make([]error, numBroadcasters)
			// Each node accepts exactly one connection per round.
			for i, a := range order {
				b := order[(i+offset)%numBroadcasters]
				connectors.Go(func() {
					errs[i] = dialBroadcasters(broadcasters[a], broadcasters[b])
				})
			}
			connectors.Wait()
			for _, err := range errs {
				require.NoError(t, err)
			}
		}
		synctest.Wait()
		for i, b := range broadcasters {
			b.Node.Waggle.mx.RLock()
			peerCount := len(b.Node.Waggle.peers)
			b.Node.Waggle.mx.RUnlock()
			require.Equal(t, numPeers, peerCount, "node %d", i)
		}
		for i, b := range broadcasters {
			b.Publish(messages[i].ObjectID, messages[i].Message)
		}

		receivers.Wait()
		for i, receivedMessages := range received {
			seen := make(map[string]bool, numBroadcasters-1)
			for _, msg := range receivedMessages {
				require.NotEmpty(t, msg.ObjectID, "node %d timed out waiting for a message", i)
				require.Contains(t, expected, msg.MessageID)
				require.NotEqual(t, messages[i].MessageID, msg.MessageID, "node %d received its own message", i)
				require.NotContains(t, seen, msg.MessageID, "node %d received a duplicate", i)
				require.Equal(t, expected[msg.MessageID], msg)
				seen[msg.MessageID] = true
			}
			require.Len(t, seen, numBroadcasters-1, "node %d", i)
		}
	})
}

func TestBroadcaster(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tn := newTestNet(t, 3)
		broadcasters := make([]*broadcaster, len(tn.Nodes))
		for i, n := range tn.Nodes {
			broadcasters[i] = newBroadcaster(t, *n)
			broadcasters[i].Start()
		}

		messages := []message{
			makeMessage("object1", []byte("first")),
			makeMessage("object1", []byte("second")),
			makeMessage("object1", []byte("third")),
			makeMessage("object2", []byte("fourth")),
		}
		published := [][]int{{0, 1, 3}, {}, {0, 2}}
		for i, indexes := range published {
			for _, index := range indexes {
				msg := messages[index]
				broadcasters[i].Publish(msg.ObjectID, msg.Message)
			}
		}
		connectBroadcasters(broadcasters[0], broadcasters[1])
		connectBroadcasters(broadcasters[1], broadcasters[2])

		// Bound missing deliveries using the synctest clock.
		timeout := time.AfterFunc(5*time.Second, func() {
			for _, b := range broadcasters {
				b.cancel()
			}
		})
		defer timeout.Stop()

		expected := make([]map[string]message, len(broadcasters))
		received := make([][]message, len(broadcasters))
		var receivers sync.WaitGroup
		for i, b := range broadcasters {
			expected[i] = make(map[string]message)
			for _, msg := range messages {
				expected[i][msg.MessageID] = msg
			}
			for _, index := range published[i] {
				delete(expected[i], messages[index].MessageID)
			}
			received[i] = make([]message, len(expected[i]))
			receivers.Go(func() {
				for j := range received[i] {
					_, received[i][j] = b.Receive()
				}
			})
		}
		receivers.Wait()
		for i, receivedMessages := range received {
			for _, msg := range receivedMessages {
				require.NotEmpty(t, msg.ObjectID, "node %d timed out waiting for a message", i)
				require.Contains(t, expected[i], msg.MessageID)
				require.Equal(t, expected[i][msg.MessageID], msg)
				delete(expected[i], msg.MessageID)
			}
		}

		broadcasters[0].Publish(messages[0].ObjectID, messages[0].Message)
		time.Sleep(500 * time.Millisecond) // Allow remaining metadata exchanges to finish.
		synctest.Wait()
		for _, b := range broadcasters {
			for _, msg := range messages {
				require.Equal(t, msg, b.Messages[msg.ObjectID][msg.MessageID])
			}
			select {
			case msg := <-b.received:
				t.Fatalf("duplicate delivery: %s/%s", msg.ObjectID, msg.MessageID)
			default:
			}
		}
	})
}
