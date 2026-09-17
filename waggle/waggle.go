package waggle

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/sukunrt/go-waggle/conn"
	"github.com/sukunrt/go-waggle/pb"
	"google.golang.org/protobuf/proto"
)

type Waggle struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mx               sync.RWMutex
	topics           map[string]*Topic
	subscribedTopics map[string]*Topic
	pendingStreams   map[string]map[conn.PeerID]conn.Stream
	peers            map[conn.PeerID]conn.Conn
}

func NewWaggle() *Waggle {
	ctx, cancel := context.WithCancel(context.Background())
	return &Waggle{
		ctx:              ctx,
		cancel:           cancel,
		topics:           make(map[string]*Topic),
		subscribedTopics: make(map[string]*Topic),
		pendingStreams:   make(map[string]map[conn.PeerID]conn.Stream),
		peers:            make(map[conn.PeerID]conn.Conn),
	}
}

func (w *Waggle) Start() {
}

func (w *Waggle) Close() {
	w.cancel()
	w.wg.Wait()
}

// HandleInboundStream handles an inbound stream from the peer
//
// If a topic exists, it hands the stream to the topic, otherwise it parks the stream
// in case we interact with the peer on that topic.
func (w *Waggle) HandleInboundStream(p conn.PeerID, s conn.Stream) (topicID string, _ error) {
	now := time.Now()

	s.SetReadDeadline(now.Add(5 * time.Second))
	br := byteReader{r: s}
	length, err := binary.ReadUvarint(br)
	if err != nil {
		return "", err
	}
	if length > 1024 {
		return "", errors.New("topic length too large")
	}

	buf := make([]byte, length)
	n, err := io.ReadFull(s, buf)
	if n != int(length) {
		return "", errors.New("length mismatch")
	}

	if err := s.SetReadDeadline(time.Time{}); err != nil {
		return "", err
	}

	topicSub := &pb.TopicSubscription{}
	if err := proto.Unmarshal(buf, topicSub); err != nil {
		return "", err
	}

	topicID = string(topicSub.TopicID)
	w.mx.Lock()
	defer w.mx.Unlock()

	t, ok := w.topics[topicID]
	if !ok {
		// TODO: Limit the sizes of these maps
		tm, ok := w.pendingStreams[topicID]
		if !ok {
			tm = make(map[conn.PeerID]conn.Stream)
			w.pendingStreams[topicID] = tm
		}
		ss := tm[p]
		if ss != nil {
			ss.ResetWithError(42) // multiple streams error
		}
		tm[p] = s
	} else {
		t.HandleSendStream(p, s)
	}

	return topicID, nil
}

func (w *Waggle) openReceiveStream(c conn.Conn, topicID string) (_ conn.Stream, errorVal error) {
	s, err := c.NewStream()
	if err != nil {
		return nil, err
	}
	defer func() {
		if errorVal != nil {
			s.ResetWithError(42)
		}
	}()

	topicSub := pb.TopicSubscription{
		TopicID: []byte(topicID),
	}
	now := time.Now()
	if err := s.SetWriteDeadline(now.Add(5 * time.Second)); err != nil {
		return nil, err
	}

	buf, err := proto.Marshal(&topicSub)
	if err != nil {
		return nil, err
	}
	msgLen := make([]byte, 2)
	n := binary.PutUvarint(msgLen, uint64(len(buf)))
	msgLen = msgLen[:n]

	if _, err := s.Write(msgLen); err != nil {
		return nil, err
	}

	if _, err := s.Write(buf); err != nil {
		return nil, err
	}
	return s, nil
}

func (w *Waggle) AddPeer(p conn.PeerID, c conn.Conn) {
	// TODO: bound these goroutines
	w.mx.Lock()
	w.peers[p] = c
	var wg sync.WaitGroup
	for topicID, t := range w.subscribedTopics {
		wg.Go(func() {
			s, err := w.openReceiveStream(c, topicID)
			if err != nil {
				// TODO: handle this
				return
			}
			t.HandleReceiveStream(p, s)
		})
	}
	w.mx.Unlock()
	wg.Wait()
	return
}

func (w *Waggle) NewTopic(topicID string) (*Topic, error) {
	t := &Topic{
		w:       w,
		TopicID: topicID,
	}
	w.mx.Lock()
	defer w.mx.Unlock()
	if _, ok := w.topics[topicID]; ok {
		return nil, errors.New("topic already exists")
	}
	t.ctx, t.cancel = context.WithCancel(w.ctx)
	w.topics[topicID] = t

	t.Start()
	return t, nil
}

func (w *Waggle) subscribe(t *Topic) error {
	w.mx.Lock()
	defer w.mx.Unlock()

	if _, ok := w.subscribedTopics[t.TopicID]; ok {
		return errors.New("topic already subscribed")
	}
	w.subscribedTopics[t.TopicID] = t
	for p, c := range w.peers {
		go func() {
			s, err := w.openReceiveStream(c, t.TopicID)
			if err != nil {
				// TODO: handle this
				return
			}
			t.HandleReceiveStream(p, s)
		}()
	}
	return nil
}

func (w *Waggle) unsubscribe(t *Topic) error {
	w.mx.Lock()
	defer w.mx.Unlock()
	delete(w.subscribedTopics, t.TopicID)
	return nil
}

type Topic struct {
	TopicID string

	w      *Waggle
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	receiveStreams map[conn.PeerID]conn.Stream
	sendStreams    map[conn.PeerID]conn.Stream
}

func (t *Topic) Start() {
	t.wg.Go(t.loop)
}

func (t *Topic) Close() {
	t.cancel()
	t.wg.Wait()
}

func (t *Topic) Subscribe() {
	t.w.subscribe(t)
	// inform the loop here
	return
}

func (t *Topic) Unsubscribe() {
	t.w.unsubscribe(t)
	// TODO: reset all receive streams here.
	return
}

func (t *Topic) HandleSendStream(p conn.PeerID, s conn.Stream) {}

func (t *Topic) HandleReceiveStream(p conn.PeerID, s conn.Stream) {}

func (t *Topic) Peers() []conn.PeerID { return nil }

func (t *Topic) SendMessage(p conn.PeerID, msg []byte) {}

func (t *Topic) ReceiveMessage() (p conn.PeerID, msg []byte) { return "", nil }

func (t *Topic) loop() {
	select {
	case <-t.ctx.Done():
		return
	}
}

type byteReader struct {
	buf [1]byte
	r   io.Reader
}

func (r byteReader) ReadByte() (byte, error) {
	n, err := r.r.Read(r.buf[:])
	if n == 1 {
		return r.buf[0], nil
	}
	return 0, err
}
