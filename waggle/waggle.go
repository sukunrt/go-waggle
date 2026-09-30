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

type topicState struct {
	Topic *Topic
	Wg    sync.WaitGroup
}

type Waggle struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mx               sync.RWMutex
	topics           map[string]*topicState
	subscribedTopics map[string]*Topic
	pendingStreams   map[string]map[conn.PeerID]conn.Stream
	peers            map[conn.PeerID]conn.Conn
}

func NewWaggle() *Waggle {
	ctx, cancel := context.WithCancel(context.Background())
	return &Waggle{
		ctx:              ctx,
		cancel:           cancel,
		topics:           make(map[string]*topicState),
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

// HandleInboundStream handles an inbound stream from the peer
//
// If a topic exists, it hands the stream to the topic, otherwise it parks the stream
// in case we interact with the peer on that topic.
func (w *Waggle) HandleInboundStream(p conn.PeerID, s conn.Stream) (topicID string, _ error) {
	topicSub, err := w.readTopicHeader(s)
	if err != nil {
		s.ResetWithError(42)
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
			// multiple streams. reset the earlier stream
			// note it's not necessary that this is the correct stream to reset
			// streams may be delivered to us out of order.
			ss.ResetWithError(42)
		}
		tm[p] = s
	} else {
		t.Topic.handleSendStream(p, s)
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
		wg.Add(1)
		t.wg.Add(1)
		go func() {
			defer wg.Done()
			defer t.wg.Done()
			s, err := w.openReceiveStream(c, topicID)
			if err != nil {
				// TODO: handle this
				return
			}
			t.handleReceiveStream(p, s)
		}()
	}
	w.mx.Unlock()

	wg.Wait()
	return
}

func (w *Waggle) DisconnectPeer(p conn.PeerID, c conn.Conn) {
	w.mx.Lock()
	defer w.mx.Unlock()

	cc, ok := w.peers[p]
	if ok && cc == c {
		delete(w.peers, p)
	}

	for topicID, m := range w.pendingStreams {
		s, ok := m[p]
		if ok && s.Conn() == c {
			s.ResetWithError(42)
			delete(m, p)
		}
		if len(m) == 0 {
			delete(w.pendingStreams, topicID)
		}
	}

	for _, t := range w.topics {
		t.Topic.DisconnectPeer(p, c)
	}
}

func (w *Waggle) NewTopic(topicID string) (*Topic, error) {
	if len(topicID) >= 1024 {
		return nil, errors.New("topic id too long")
	}

	w.mx.Lock()
	defer w.mx.Unlock()
	if _, ok := w.topics[topicID]; ok {
		return nil, errors.New("topic already exists")
	}
	t := newTopic(w.ctx, w, topicID)
	w.topics[topicID] = &topicState{Topic: t}
	t.Start()

	for p, s := range w.pendingStreams[topicID] {
		t.handleSendStream(p, s)
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
	ts := w.topics[t.TopicID]
	w.subscribedTopics[t.TopicID] = t
	for p, c := range w.peers {
		ts.Wg.Go(func() {
			s, err := w.openReceiveStream(c, t.TopicID)
			if err != nil {
				// TODO: handle this
				return
			}
			t.handleReceiveStream(p, s)
		})
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
	defer w.mx.Unlock()
	delete(w.subscribedTopics, t.TopicID)
	if tt, ok := w.topics[t.TopicID]; ok {
		delete(w.topics, t.TopicID)
		tt.Wg.Wait()
	}
	return nil
}

type incomingMessage struct {
	P       conn.PeerID
	Message *pb.ObjectPieces
}

type receiveStream struct {
	P      conn.PeerID
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

type Topic struct {
	TopicID string

	w          *Waggle
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	subscribed atomic.Bool

	unsubscribeCh chan unsubscribeRequest
	subscribeCh   chan subscribeRequest

	peerDisconnectCh chan peerDisconnected

	incomingMessages chan incomingMessage
	receiveStreamsCh chan receiveStream
	receiveStreams   map[conn.PeerID]conn.Stream
	sentMessage      chan *streamSender
	senderComplete   chan *streamSender
	sendBufFree      map[conn.PeerID]int

	peersWithCapacityCh chan chan []conn.PeerID
	sendMessageReq      chan sendMessageRequest

	streamSenderCh chan streamSender
	streamSenders  map[conn.PeerID]*streamSender
}

func newTopic(ctx context.Context, w *Waggle, topicID string) *Topic {
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
		receiveStreams:      map[conn.PeerID]conn.Stream{},
		sentMessage:         make(chan *streamSender, 8),
		senderComplete:      make(chan *streamSender, 8),
		sendBufFree:         map[conn.PeerID]int{},
		peersWithCapacityCh: make(chan chan []conn.PeerID, 4),
		sendMessageReq:      make(chan sendMessageRequest, 4),
		streamSenderCh:      make(chan streamSender, 8),
		streamSenders:       map[conn.PeerID]*streamSender{},
	}

}

func (t *Topic) Start() {
	t.wg.Go(t.loop)
}

func (t *Topic) Close() {
	t.w.unregister(t)
	t.cancel()
	t.wg.Wait()
}

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

const sendMessageBufferSize = 4

func (t *Topic) handleSendStream(p conn.PeerID, s conn.Stream) {
	if t.ctx.Err() != nil {
		s.ResetWithError(42)
		return
	}

	select {
	case t.streamSenderCh <- streamSender{
		P:             p,
		Stream:        s,
		SentMessageCh: t.sentMessage,
		CompleteCh:    t.senderComplete,
		msgs:          make(chan *pb.ObjectPieces, sendMessageBufferSize+4),
	}:
	case <-t.ctx.Done():
		s.ResetWithError(42)
	}
}

func (t *Topic) handleReceiveStream(p conn.PeerID, s conn.Stream) {
	if t.ctx.Err() != nil {
		s.ResetWithError(42)
		return
	}

	select {
	case t.receiveStreamsCh <- receiveStream{
		P:      p,
		Stream: s,
	}:
	case <-t.ctx.Done():
		s.ResetWithError(42)
		return
	}
}

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

func (t *Topic) Peers() []conn.PeerID {
	ch := make(chan []conn.PeerID, 1)
	select {
	case t.peersWithCapacityCh <- ch:
	case <-t.ctx.Done():
		return nil
	}
	select {
	case peers := <-ch:
		return peers
	case <-t.ctx.Done():
		return nil
	}
}

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

func (t *Topic) ReceiveMessage() (conn.PeerID, *pb.ObjectPieces) {
	select {
	case msg := <-t.incomingMessages:
		return msg.P, msg.Message
	case <-t.ctx.Done():
		return "", nil
	}
}

func (t *Topic) runReceiveStream(p conn.PeerID, s conn.Stream) {
	defer s.ResetWithError(42)

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

func (t *Topic) cancelReceiveStreams() {
	for _, s := range t.receiveStreams {
		s.ResetWithError(42)
	}
	clear(t.receiveStreams)
}

func (t *Topic) cancelSendStreams() {
	for _, s := range t.streamSenders {
		s.Cancel()
		s.Stream.ResetWithError(42)
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
		case ss := <-t.sentMessage:
			if t.streamSenders[ss.P] == ss {
				t.sendBufFree[ss.P]++
			}
		case ss := <-t.senderComplete:
			t.handleSenderComplete(ss)
		case req := <-t.sendMessageReq:
			ss, ok := t.streamSenders[req.P]
			if !ok {
				continue
			}
			t.sendBufFree[req.P]--
			// todo: handle dropped messages better
			ss.sendMessage(req.Message)

		case ch := <-t.peersWithCapacityCh:
			ch <- t.peerWithCapacity()

		case pd := <-t.peerDisconnectCh:
			t.disconnectPeer(pd.P, pd.Conn)
		}
	}
}

func (t *Topic) disconnectPeer(p conn.PeerID, c conn.Conn) {
	ss, ok := t.streamSenders[p]
	if ok {
		if ss.Stream.Conn() == c {
			ss.Cancel()
			ss.Stream.ResetWithError(42)
		}
	}
	rs, ok := t.receiveStreams[p]
	if ok {
		if rs.Conn() == c {
			rs.ResetWithError(42)
		}
	}
}

func (t *Topic) peerWithCapacity() []conn.PeerID {
	peers := make([]conn.PeerID, 0, len(t.sendBufFree))
	for p, v := range t.sendBufFree {
		if v <= 0 {
			continue
		}
		peers = append(peers, p)
	}
	return peers
}

func (t *Topic) handleSenderComplete(ss *streamSender) {
	// only delete if we've not overwritten the map entry and dropped the reference
	if t.streamSenders[ss.P] == ss {
		delete(t.streamSenders, ss.P)
		delete(t.sendBufFree, ss.P)
	}
}

func (t *Topic) handleStreamSender(ss streamSender, wg *sync.WaitGroup) {
	ssold, ok := t.streamSenders[ss.P]
	if ok {
		ssold.Cancel()
		ssold.Stream.ResetWithError(42)
	}
	ss.Ctx, ss.Cancel = context.WithCancel(t.ctx)
	t.streamSenders[ss.P] = &ss
	t.sendBufFree[ss.P] = sendMessageBufferSize
	wg.Go(ss.loop)
}

func (t *Topic) handleReceiveStreamLoop(rs receiveStream, wg *sync.WaitGroup) {
	if !t.subscribed.Load() {
		rs.Stream.ResetWithError(42)
	} else {
		ss, ok := t.receiveStreams[rs.P]
		if ok {
			ss.ResetWithError(42)
		}
		t.receiveStreams[rs.P] = rs.Stream
		wg.Go(func() { t.runReceiveStream(rs.P, rs.Stream) })
	}
}

func (t *Topic) drainTillComplete(done chan struct{}) {
	for {
		select {
		case rs := <-t.receiveStreamsCh:
			rs.Stream.ResetWithError(42)
		case ss := <-t.streamSenderCh:
			ss.Stream.ResetWithError(42)
		case <-t.peerDisconnectCh:
		case <-t.senderComplete:
		case <-t.sendMessageReq:
		case <-t.sentMessage:
		case <-done:
			for {
				select {
				case rs := <-t.receiveStreamsCh:
					rs.Stream.ResetWithError(42)
				case ss := <-t.streamSenderCh:
					ss.Stream.ResetWithError(42)
				case <-t.peerDisconnectCh:
				case <-t.senderComplete:
				case <-t.sendMessageReq:
				case <-t.sentMessage:
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
	P             conn.PeerID
	Stream        conn.Stream
	SentMessageCh chan *streamSender
	CompleteCh    chan *streamSender
	Ctx           context.Context
	Cancel        context.CancelFunc
	msgs          chan *pb.ObjectPieces
}

func (s *streamSender) sendMessage(m *pb.ObjectPieces) {
	select {
	case s.msgs <- m:
	default:
		// TODO: use slog here
		fmt.Println("msg sending failed: buffer full", s.P)
	}
}

func (s *streamSender) loop() {
	defer s.Cancel()
	defer func() {
		s.CompleteCh <- s
	}()
	defer s.Stream.ResetWithError(42)
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

		s.SentMessageCh <- s
	}
}
