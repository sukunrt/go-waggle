package waggle

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sukunrt/go-waggle/conn"
	"github.com/sukunrt/go-waggle/pb"
	"google.golang.org/protobuf/proto"
)

const (
	maxMessageLength = 10 * 1 << 20
)

// StreamKind is the kind of a topic stream.
type StreamKind int

const (
	// ControlStream carries messages without pieces. Without a data stream, it carries all messages.
	ControlStream StreamKind = iota
	// DataStream carries messages with pieces.
	DataStream
	numStreamKinds
)

func messageKind(m *pb.ObjectPieces) StreamKind {
	if len(m.Pieces) > 0 {
		return DataStream
	}
	return ControlStream
}

type streamKey struct {
	P    conn.PeerID
	Kind StreamKind
}

// TopicOption configures a topic.
type TopicOption func(*Topic)

// WithDataStream makes the topic open a separate data stream to each peer.
func WithDataStream() TopicOption {
	return func(t *Topic) { t.dataStream = true }
}

// WithControlQueueSize sets the free send slots per peer for the control stream.
// Use it with peers that open a data stream. Without one, pieces also use these slots.
func WithControlQueueSize(n int) TopicOption {
	return func(t *Topic) { t.controlQueueSize = n }
}

type topicState struct {
	Topic *Topic
	Wg    sync.WaitGroup
}

// Waggle is the object to handle waggle streams and create topics, initiate topic streams.
type Waggle struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mx               sync.RWMutex
	topics           map[string]*topicState
	subscribedTopics map[string]*Topic
	pendingStreams   map[string]map[streamKey]conn.Stream
	peers            map[conn.PeerID]conn.Conn
}

// NewWaggle returns the waggle object
func NewWaggle() *Waggle {
	ctx, cancel := context.WithCancel(context.Background())
	return &Waggle{
		ctx:              ctx,
		cancel:           cancel,
		topics:           make(map[string]*topicState),
		subscribedTopics: make(map[string]*Topic),
		pendingStreams:   make(map[string]map[streamKey]conn.Stream),
		peers:            make(map[conn.PeerID]conn.Conn),
	}
}

func (w *Waggle) Start() {
}

func (w *Waggle) Close() {
	w.cancel()
	w.wg.Wait()
	// TODO: close all topics here as well.
}

func (w *Waggle) readTopicHeader(s conn.Stream) (*pb.TopicSubscription, error) {
	s.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := byteReader{r: s}
	length, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	if length > 1024 {
		return nil, errors.New("topic length too large")
	}

	buf := make([]byte, length)
	n, err := io.ReadFull(s, buf)
	if n != int(length) {
		return nil, errors.New("length mismatch")
	}

	if err := s.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}

	topicSub := &pb.TopicSubscription{}
	if err := proto.Unmarshal(buf, topicSub); err != nil {
		return nil, err
	}

	return topicSub, nil
}

// HandleInboundStream handles an inbound waggle stream from the peer.
// If a topic exists, it hands the stream to the topic, otherwise it parks the stream
// in case we interact with the peer on that topic.
//
// If using with go-libp2p push the stream to the handler after multistream select.
func (w *Waggle) HandleInboundStream(p conn.PeerID, s conn.Stream) (topicID string, _ error) {
	topicSub, err := w.readTopicHeader(s)
	if err != nil {
		s.ResetWithError(conn.StreamErrorCodeUndecided)
		return "", err
	}

	kind := StreamKind(topicSub.GetStream())
	if kind >= numStreamKinds {
		s.ResetWithError(conn.StreamErrorCodeUndecided)
		return "", errors.New("unknown stream kind")
	}

	topicID = string(topicSub.TopicID)
	w.mx.Lock()
	defer w.mx.Unlock()

	t, ok := w.topics[topicID]
	if !ok {
		// TODO: Limit the sizes of these maps
		tm, ok := w.pendingStreams[topicID]
		if !ok {
			tm = make(map[streamKey]conn.Stream)
			w.pendingStreams[topicID] = tm
		}
		k := streamKey{P: p, Kind: kind}
		ss := tm[k]
		if ss != nil {
			// multiple streams. reset the earlier stream
			// note it's not necessary that this is the correct stream to reset
			// streams may be delivered to us out of order.
			ss.ResetWithError(conn.StreamErrorCodeUndecided)
		}
		tm[k] = s
	} else {
		t.Topic.handleSendStream(p, kind, s)
	}

	return topicID, nil
}

func (w *Waggle) openReceiveStream(c conn.Conn, topicID string, kind StreamKind) (_ conn.Stream, errorVal error) {
	s, err := c.NewStream()
	if err != nil {
		return nil, err
	}
	defer func() {
		if errorVal != nil {
			s.ResetWithError(conn.StreamErrorCodeUndecided)
		}
	}()

	topicSub := pb.TopicSubscription{
		TopicID: []byte(topicID),
	}
	if kind != ControlStream {
		topicSub.Stream = proto.Uint32(uint32(kind))
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

// AddPeer adds a peer to our waggle peer set. It opens receive streams
// for all subscribed topics in the waggle peer set.
func (w *Waggle) AddPeer(p conn.PeerID, c conn.Conn) {
	// TODO: bound these goroutines
	w.mx.Lock()
	w.peers[p] = c
	var wg sync.WaitGroup
	for topicID, t := range w.subscribedTopics {
		for _, kind := range t.streamKinds() {
			wg.Add(1)
			t.wg.Add(1)
			go func() {
				defer wg.Done()
				defer t.wg.Done()
				s, err := w.openReceiveStream(c, topicID, kind)
				if err != nil {
					// TODO: handle this
					return
				}
				t.handleReceiveStream(p, kind, s)
			}()
		}
	}
	w.mx.Unlock()

	wg.Wait()
	return
}

// DisconnectPeer removes a peer from our waggle peer set and closes all
// receive and send streams.
func (w *Waggle) DisconnectPeer(p conn.PeerID, c conn.Conn) {
	w.mx.Lock()
	defer w.mx.Unlock()

	cc, ok := w.peers[p]
	if ok && cc == c {
		delete(w.peers, p)
	}

	for topicID, m := range w.pendingStreams {
		for k, s := range m {
			if k.P == p && s.Conn() == c {
				s.ResetWithError(conn.StreamErrorCodeUndecided)
				delete(m, k)
			}
		}
		if len(m) == 0 {
			delete(w.pendingStreams, topicID)
		}
	}

	for _, t := range w.topics {
		t.Topic.DisconnectPeer(p, c)
	}
}

// NewTopic creates a waggle topic. sendQueueSize is the free send slots per peer, 0 sets the default.
// onSendComplete is called on the peer's sender goroutine after each sent message. It can be nil.
func (w *Waggle) NewTopic(topicID string, sendQueueSize int, onSendComplete func(ctx context.Context, p conn.PeerID), opts ...TopicOption) (*Topic, error) {
	if len(topicID) >= 1024 {
		return nil, errors.New("topic id too long")
	}
	if sendQueueSize < 0 {
		return nil, errors.New("send queue size is negative")
	}
	if sendQueueSize == 0 {
		sendQueueSize = defaultSendQueueSize
	}

	w.mx.Lock()
	defer w.mx.Unlock()
	if _, ok := w.topics[topicID]; ok {
		return nil, errors.New("topic already exists")
	}
	t := newTopic(w.ctx, w, topicID, sendQueueSize, onSendComplete)
	for _, o := range opts {
		o(t)
	}
	w.topics[topicID] = &topicState{Topic: t}
	t.Start()

	for k, s := range w.pendingStreams[topicID] {
		t.handleSendStream(k.P, k.Kind, s)
	}
	delete(w.pendingStreams, topicID)

	return t, nil
}

func (w *Waggle) subscribe(t *Topic) error {
	w.mx.Lock()
	defer w.mx.Unlock()

	if _, ok := w.subscribedTopics[t.TopicID]; ok {
		return errors.New("topic already subscribed")
	}
	ts, ok := w.topics[t.TopicID]
	if !ok {
		return errors.New("topic closed")
	}
	w.subscribedTopics[t.TopicID] = t
	for p, c := range w.peers {
		for _, kind := range t.streamKinds() {
			ts.Wg.Go(func() {
				s, err := w.openReceiveStream(c, t.TopicID, kind)
				if err != nil {
					// TODO: handle this
					return
				}
				t.handleReceiveStream(p, kind, s)
			})
		}
	}
	return nil
}

func (w *Waggle) unsubscribe(t *Topic) error {
	w.mx.Lock()
	defer w.mx.Unlock()
	delete(w.subscribedTopics, t.TopicID)
	return nil
}

func (w *Waggle) unregister(t *Topic) error {
	w.mx.Lock()
	delete(w.subscribedTopics, t.TopicID)
	var ts *topicState
	if tt, ok := w.topics[t.TopicID]; ok {
		delete(w.topics, t.TopicID)
		ts = tt
	}
	w.mx.Unlock()
	if ts != nil {
		ts.Wg.Wait()
	}
	return nil
}

type incomingMessage struct {
	P       conn.PeerID
	Message *pb.ObjectPieces
}

type receiveStream struct {
	P      conn.PeerID
	Kind   StreamKind
	Stream conn.Stream
}

type sendMessageRequest struct {
	P       conn.PeerID
	Message *pb.ObjectPieces
}

type unsubscribeRequest struct {
	Done chan struct{}
}

type subscribeRequest struct {
	Done chan struct{}
}

type peerDisconnected struct {
	P    conn.PeerID
	Conn conn.Conn
}

type peerCapacityRequest struct {
	P    conn.PeerID
	Kind StreamKind
	Resp chan peerCapacityResponse
}

type peersWithCapacityRequest struct {
	Kind StreamKind
	Resp chan []conn.PeerID
}

type peerCapacityResponse struct {
	Capacity int
	Found    bool
}

var (
	ErrTopicClosed  = errors.New("topic closed")
	ErrPeerNotFound = errors.New("peer not found")
)

// Topic is a waggle topic to exchange objects of a specific type. For ethereum,
// this can be used for Attestations, Blocks, etc. The semantics of the objects are
// left to the application. A waggle topic allows exchanging arbitrary objects and
// their metadata.
type Topic struct {
	TopicID string

	w          *Waggle
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	subscribed atomic.Bool
	dataStream bool

	unsubscribeCh chan unsubscribeRequest
	subscribeCh   chan subscribeRequest

	peerDisconnectCh chan peerDisconnected

	incomingMessages chan incomingMessage
	receiveStreamsCh chan receiveStream
	receiveStreams   map[streamKey]conn.Stream
	senderComplete   chan *streamSender
	onSendComplete   func(ctx context.Context, p conn.PeerID)
	sendQueueSize    int
	controlQueueSize int

	peersWithCapacityCh chan peersWithCapacityRequest
	peerCapacityCh      chan peerCapacityRequest
	sendMessageReq      chan sendMessageRequest

	streamSenderCh chan *streamSender
	streamSenders  map[streamKey]*streamSender
}

func newTopic(ctx context.Context, w *Waggle, topicID string, sendQueueSize int, onSendComplete func(ctx context.Context, p conn.PeerID)) *Topic {
	cctx, cancel := context.WithCancel(ctx)
	return &Topic{
		TopicID:             topicID,
		w:                   w,
		ctx:                 cctx,
		cancel:              cancel,
		unsubscribeCh:       make(chan unsubscribeRequest, 0),
		subscribeCh:         make(chan subscribeRequest, 0),
		peerDisconnectCh:    make(chan peerDisconnected, 1),
		incomingMessages:    make(chan incomingMessage, 32),
		receiveStreamsCh:    make(chan receiveStream, 16),
		receiveStreams:      map[streamKey]conn.Stream{},
		senderComplete:      make(chan *streamSender, 8),
		onSendComplete:      onSendComplete,
		sendQueueSize:       sendQueueSize,
		peersWithCapacityCh: make(chan peersWithCapacityRequest, 4),
		peerCapacityCh:      make(chan peerCapacityRequest, 4),
		sendMessageReq:      make(chan sendMessageRequest, 4),
		streamSenderCh:      make(chan *streamSender, 8),
		streamSenders:       map[streamKey]*streamSender{},
	}

}

func (t *Topic) queueSize(kind StreamKind) int {
	if kind == ControlStream && t.controlQueueSize > 0 {
		return t.controlQueueSize
	}
	return t.sendQueueSize
}

func (t *Topic) streamKinds() []StreamKind {
	if t.dataStream {
		return []StreamKind{ControlStream, DataStream}
	}
	return []StreamKind{ControlStream}
}

func (t *Topic) Start() {
	t.wg.Go(t.loop)
}

func (t *Topic) Close() {
	t.w.unregister(t)
	t.cancel()
	t.wg.Wait()
}

// Subscribe subscribes to receiving messages from peers.
func (t *Topic) Subscribe() {
	if t.subscribed.Load() {
		return
	}
	t.subscribed.Store(true)
	t.w.subscribe(t)
	// inform the loop here
	return
}

func (t *Topic) Unsubscribe() {
	t.w.unsubscribe(t)
	t.subscribed.Store(false)

	req := unsubscribeRequest{
		Done: make(chan struct{}),
	}
	select {
	case t.unsubscribeCh <- req:
	case <-t.ctx.Done():
		return
	}
	select {
	case <-req.Done:
	case <-t.ctx.Done():
		return
	}
	return
}

const defaultSendQueueSize = 4

func (t *Topic) handleSendStream(p conn.PeerID, kind StreamKind, s conn.Stream) {
	if t.ctx.Err() != nil {
		s.ResetWithError(conn.StreamErrorCodeUndecided)
		return
	}

	select {
	case t.streamSenderCh <- &streamSender{
		P:              p,
		Kind:           kind,
		Stream:         s,
		CompleteCh:     t.senderComplete,
		OnSendComplete: t.onSendComplete,
		msgs:           make(chan *pb.ObjectPieces, t.queueSize(kind)+4),
	}:
	case <-t.ctx.Done():
		s.ResetWithError(conn.StreamErrorCodeUndecided)
	}
}

func (t *Topic) handleReceiveStream(p conn.PeerID, kind StreamKind, s conn.Stream) {
	if t.ctx.Err() != nil {
		s.ResetWithError(conn.StreamErrorCodeUndecided)
		return
	}

	select {
	case t.receiveStreamsCh <- receiveStream{
		P:      p,
		Kind:   kind,
		Stream: s,
	}:
	case <-t.ctx.Done():
		s.ResetWithError(conn.StreamErrorCodeUndecided)
		return
	}
}

// DisconnectPeer disconnects a peer.
func (t *Topic) DisconnectPeer(p conn.PeerID, c conn.Conn) {
	if t.ctx.Err() != nil {
		return
	}

	select {
	case t.peerDisconnectCh <- peerDisconnected{
		P:    p,
		Conn: c,
	}:
	case <-t.ctx.Done():
		return
	}
}

// PeersWithCapacity returns the peers with free send slots for messages of kind.
// It must not be used concurrently with `SendMessage`
func (t *Topic) PeersWithCapacity(kind StreamKind) []conn.PeerID {
	req := peersWithCapacityRequest{Kind: kind, Resp: make(chan []conn.PeerID, 1)}
	select {
	case t.peersWithCapacityCh <- req:
	case <-t.ctx.Done():
		return nil
	}
	select {
	case peers := <-req.Resp:
		return peers
	case <-t.ctx.Done():
		return nil
	}
}

// GetPeerCapacity returns the number of free send slots for messages of kind to peer p.
func (t *Topic) GetPeerCapacity(p conn.PeerID, kind StreamKind) (int, error) {
	req := peerCapacityRequest{P: p, Kind: kind, Resp: make(chan peerCapacityResponse, 1)}
	select {
	case t.peerCapacityCh <- req:
	case <-t.ctx.Done():
		return 0, ErrTopicClosed
	}
	select {
	case resp := <-req.Resp:
		if !resp.Found {
			return 0, ErrPeerNotFound
		}
		return max(resp.Capacity, 0), nil
	case <-t.ctx.Done():
		return 0, ErrTopicClosed
	}
}

// SendMessage sends the msg to peer p. A msg with pieces goes on the data stream, if there is one.
func (t *Topic) SendMessage(p conn.PeerID, msg *pb.ObjectPieces) {
	if t.ctx.Err() != nil {
		return
	}
	// We can directly send it to the streamSender
	// But this is good enough for now. I don't expect the compute work here
	// to be too intensive
	select {
	case t.sendMessageReq <- sendMessageRequest{P: p, Message: msg}:
	case <-t.ctx.Done():
	}
}

// ReceiveMessage receives the next message from the incoming message queue.
func (t *Topic) ReceiveMessage() (conn.PeerID, *pb.ObjectPieces) {
	select {
	case msg := <-t.incomingMessages:
		return msg.P, msg.Message
	case <-t.ctx.Done():
		return "", nil
	}
}

func (t *Topic) cancelReceiveStreams() {
	for _, s := range t.receiveStreams {
		s.ResetWithError(conn.StreamErrorCodeUndecided)
	}
	clear(t.receiveStreams)
}

func (t *Topic) cancelSendStreams() {
	for _, s := range t.streamSenders {
		s.Cancel()
		s.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
	}
	clear(t.streamSenders)
}

func (t *Topic) loop() {
	var wg sync.WaitGroup

	workerDone := make(chan struct{}, 1)

	for {
		select {
		case <-t.ctx.Done():
			t.cancelReceiveStreams()
			t.cancelSendStreams()
			go func() {
				wg.Wait()
				workerDone <- struct{}{}
			}()
			t.drainTillComplete(workerDone)
			return
		case ch := <-t.unsubscribeCh:
			t.cancelReceiveStreams()
			close(ch.Done)
		case rs := <-t.receiveStreamsCh:
			t.handleReceiveStreamLoop(rs, &wg)

		case ss := <-t.streamSenderCh:
			t.handleStreamSender(ss, &wg)
		case ss := <-t.senderComplete:
			t.handleSenderComplete(ss)
		case req := <-t.sendMessageReq:
			ss := t.sender(req.P, messageKind(req.Message))
			if ss == nil {
				continue
			}
			// todo: handle dropped messages better
			ss.sendMessage(req.Message)

		case req := <-t.peersWithCapacityCh:
			req.Resp <- t.peerWithCapacity(req.Kind)
		case req := <-t.peerCapacityCh:
			var resp peerCapacityResponse
			if ss := t.sender(req.P, req.Kind); ss != nil {
				resp = peerCapacityResponse{Capacity: int(ss.free.Load()), Found: true}
			}
			req.Resp <- resp

		case pd := <-t.peerDisconnectCh:
			t.disconnectPeer(pd.P, pd.Conn)
		}
	}
}

func (t *Topic) disconnectPeer(p conn.PeerID, c conn.Conn) {
	for kind := range numStreamKinds {
		k := streamKey{P: p, Kind: kind}
		if ss, ok := t.streamSenders[k]; ok && ss.Stream.Conn() == c {
			ss.Cancel()
			ss.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
		}
		if rs, ok := t.receiveStreams[k]; ok && rs.Conn() == c {
			rs.ResetWithError(conn.StreamErrorCodeUndecided)
		}
	}
}

// sender returns the sender for messages of kind to p. Without a data stream, it returns the control stream sender.
func (t *Topic) sender(p conn.PeerID, kind StreamKind) *streamSender {
	if ss, ok := t.streamSenders[streamKey{P: p, Kind: kind}]; ok {
		return ss
	}
	return t.streamSenders[streamKey{P: p, Kind: ControlStream}]
}

func (t *Topic) peerWithCapacity(kind StreamKind) []conn.PeerID {
	peers := make([]conn.PeerID, 0, len(t.streamSenders))
	for k := range t.streamSenders {
		if k.Kind != ControlStream {
			continue
		}
		if t.sender(k.P, kind).free.Load() <= 0 {
			continue
		}
		peers = append(peers, k.P)
	}
	return peers
}

func (t *Topic) handleSenderComplete(ss *streamSender) {
	// only delete if we've not overwritten the map entry and dropped the reference
	k := streamKey{P: ss.P, Kind: ss.Kind}
	if t.streamSenders[k] == ss {
		delete(t.streamSenders, k)
	}
}

func (t *Topic) handleStreamSender(ss *streamSender, wg *sync.WaitGroup) {
	k := streamKey{P: ss.P, Kind: ss.Kind}
	ssold, ok := t.streamSenders[k]
	if ok {
		ssold.Cancel()
		ssold.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
	}
	ss.Ctx, ss.Cancel = context.WithCancel(t.ctx)
	ss.free.Store(int32(t.queueSize(ss.Kind)))
	t.streamSenders[k] = ss
	wg.Go(ss.sendLoop)
}

func (t *Topic) handleReceiveStreamLoop(rs receiveStream, wg *sync.WaitGroup) {
	if !t.subscribed.Load() {
		rs.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
	} else {
		k := streamKey{P: rs.P, Kind: rs.Kind}
		ss, ok := t.receiveStreams[k]
		if ok {
			ss.ResetWithError(conn.StreamErrorCodeUndecided)
		}
		t.receiveStreams[k] = rs.Stream
		wg.Go(func() { t.recvLoop(rs.P, rs.Stream) })
	}
}

func (t *Topic) drainTillComplete(done chan struct{}) {
	for {
		select {
		case rs := <-t.receiveStreamsCh:
			rs.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
		case ss := <-t.streamSenderCh:
			ss.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
		case <-t.peerDisconnectCh:
		case <-t.senderComplete:
		case <-t.sendMessageReq:
		case <-done:
			for {
				select {
				case rs := <-t.receiveStreamsCh:
					rs.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
				case ss := <-t.streamSenderCh:
					ss.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
				case <-t.peerDisconnectCh:
				case <-t.senderComplete:
				case <-t.sendMessageReq:
				default:
					return

				}
			}
		}
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

type streamSender struct {
	P              conn.PeerID
	Kind           StreamKind
	Stream         conn.Stream
	CompleteCh     chan *streamSender
	OnSendComplete func(ctx context.Context, p conn.PeerID)
	Ctx            context.Context
	Cancel         context.CancelFunc
	msgs           chan *pb.ObjectPieces
	free           atomic.Int32
}

func (s *streamSender) sendMessage(m *pb.ObjectPieces) {
	s.free.Add(-1)
	select {
	case s.msgs <- m:
	default:
		s.free.Add(1)
		// TODO: use slog here
		fmt.Println("msg sending failed: buffer full", s.P)
	}
}

func (s *streamSender) sendLoop() {
	defer s.Cancel()
	defer func() {
		s.CompleteCh <- s
	}()
	defer s.Stream.ResetWithError(conn.StreamErrorCodeUndecided)
	for {
		var msg *pb.ObjectPieces
		select {
		case msg = <-s.msgs:
		case <-s.Ctx.Done():
			return
		}
		buf, err := proto.Marshal(msg)
		if err != nil {
			return
		}
		msgLen := make([]byte, 8)
		n := binary.PutUvarint(msgLen, uint64(len(buf)))
		msgLen = msgLen[:n]

		s.Stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := s.Stream.Write(msgLen); err != nil {
			return
		}

		if _, err := s.Stream.Write(buf); err != nil {
			return
		}

		s.free.Add(1)
		if s.OnSendComplete != nil {
			s.OnSendComplete(s.Ctx, s.P)
		}
	}
}

func (t *Topic) recvLoop(p conn.PeerID, s conn.Stream) {
	defer s.ResetWithError(conn.StreamErrorCodeUndecided)

	buf := make([]byte, maxMessageLength)
	br := &byteReader{r: s}
	for {
		length, err := binary.ReadUvarint(br)
		if err != nil {
			return
		}
		if length > maxMessageLength {
			return // maybe just drop the message?
		}
		buf = buf[:length]
		if _, err := io.ReadFull(s, buf); err != nil {
			return
		}
		msg := &pb.ObjectPieces{}

		if err := proto.Unmarshal(buf, msg); err != nil {
			return
		}
		select {
		case t.incomingMessages <- incomingMessage{
			P:       p,
			Message: msg,
		}:
		case <-t.ctx.Done():
			return
		}
	}
}
