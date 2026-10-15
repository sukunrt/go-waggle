package libp2p

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	msmux "github.com/multiformats/go-multistream"

	"github.com/sukunrt/go-waggle/conn"
	"github.com/sukunrt/go-waggle/waggle"
)

const (
	protocolID       = protocol.ID(conn.WaggleProtocolID)
	newStreamTimeout = 10 * time.Second
	maxWorkers       = 50
)

// ConfigureHost configures the host to plug in its waggle peers to w. When the conn that
// w uses closes, w uses a different open conn. When no conn is open, it
// removes the peer. It also sends inbound waggle streams to w.
//
// stop removes link and newly connected peers are not added to w.
func ConfigureHost(w *waggle.Waggle, h host.Host) (stop func(), err error) {
	sub, err := h.EventBus().Subscribe(new(event.EvtPeerIdentificationCompleted))
	if err != nil {
		return nil, err
	}

	closed := make(chan network.Conn, 64)
	notifiee := &network.NotifyBundle{
		DisconnectedF: func(_ network.Network, c network.Conn) { closed <- c },
	}
	h.Network().Notify(notifiee)

	h.SetStreamHandler(protocolID, func(s network.Stream) {
		w.HandleInboundStream(s.Conn().RemotePeer(), stream{s: s, rw: s})
	})

	m := &peerManager{
		waggle:     w,
		host:       h,
		semaphore:  make(chan struct{}, maxWorkers),
		doneSignal: make(chan struct{}, 1),
		conns:      make(map[peer.ID][]network.Conn),
		active:     make(map[peer.ID]network.Conn),
		busy:       make(map[peer.ID]bool),
	}
	m.wg.Go(func() { m.loop(sub, closed) })

	return sync.OnceFunc(func() {
		h.RemoveStreamHandler(protocolID)
		h.Network().StopNotify(notifiee)
		sub.Close()
		m.wg.Wait()
	}), nil
}

type peerManager struct {
	waggle    *waggle.Waggle
	host      host.Host
	wg        sync.WaitGroup
	semaphore chan struct{}

	doneMx     sync.Mutex
	done       []peer.ID
	doneSignal chan struct{}

	conns  map[peer.ID][]network.Conn
	active map[peer.ID]network.Conn
	busy   map[peer.ID]bool
}

func (m *peerManager) loop(sub event.Subscription, closed <-chan network.Conn) {
	for _, c := range m.host.Network().Conns() {
		protos, err := m.host.Peerstore().SupportsProtocols(c.RemotePeer(), protocolID)
		if err == nil && len(protos) > 0 {
			m.addConn(c)
		}
	}
	for {
		m.clearDone()
		select {
		case e, ok := <-sub.Out():
			if !ok {
				return
			}
			evt := e.(event.EvtPeerIdentificationCompleted)
			if slices.Contains(evt.Protocols, protocolID) {
				m.addConn(evt.Conn)
			} else {
				delete(m.conns, evt.Peer)
				m.sync(evt.Peer)
			}
		case c := <-closed:
			m.removeConn(c)
		case <-m.doneSignal:
		}
	}
}

func (m *peerManager) markDone(p peer.ID) {
	m.doneMx.Lock()
	m.done = append(m.done, p)
	m.doneMx.Unlock()
	select {
	case m.doneSignal <- struct{}{}:
	default:
	}
}

func (m *peerManager) clearDone() {
	m.doneMx.Lock()
	done := m.done
	m.done = nil
	m.doneMx.Unlock()
	for _, p := range done {
		delete(m.busy, p)
		m.sync(p)
	}
}

func (m *peerManager) addConn(c network.Conn) {
	// Disconnected for c can come before this call.
	if c.IsClosed() || c.Stat().Limited {
		return
	}
	p := c.RemotePeer()
	if !slices.Contains(m.conns[p], c) {
		m.conns[p] = append(m.conns[p], c)
	}
	m.sync(p)
}

func (m *peerManager) removeConn(c network.Conn) {
	p := c.RemotePeer()
	m.conns[p] = slices.DeleteFunc(m.conns[p], func(cc network.Conn) bool { return cc == c })
	if len(m.conns[p]) == 0 {
		delete(m.conns, p)
	}
	m.sync(p)
}

func (m *peerManager) sync(p peer.ID) {
	if m.busy[p] {
		return
	}
	cur, conns := m.active[p], m.conns[p]
	if cur == nil && len(conns) == 0 {
		// nothing to do
		return
	}
	if slices.Contains(conns, cur) {
		return
	}
	var next network.Conn
	if len(conns) == 0 {
		delete(m.active, p)
	} else {
		next = conns[0]
		m.active[p] = next
	}
	m.busy[p] = true

	m.semaphore <- struct{}{}
	m.wg.Go(func() {
		defer func() { <-m.semaphore }()
		if cur != nil {
			m.waggle.DisconnectPeer(p, libp2pConn{cur})
		}
		if next != nil {
			m.waggle.AddPeer(p, libp2pConn{next})
		}
		m.markDone(p)
	})
}

type libp2pConn struct {
	c network.Conn
}

var _ conn.Conn = libp2pConn{}

func (c libp2pConn) NewStream() (conn.Stream, error) {
	ctx, cancel := context.WithTimeout(context.Background(), newStreamTimeout)
	defer cancel()
	s, err := c.c.NewStream(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.SetProtocol(protocolID); err != nil {
		s.ResetWithError(conn.StreamErrorCodeUndecided)
		return nil, err
	}
	// Identify showed waggle support. Lazy negotiation saves a round trip.
	return stream{s: s, rw: msmux.NewMSSelect(s, protocolID)}, nil
}

func (c libp2pConn) AcceptStream(context.Context) (conn.Stream, error) {
	return nil, errors.New("libp2p: the host stream handler receives inbound streams")
}

func (c libp2pConn) CloseWithError(code uint64, _ string) error {
	return c.c.CloseWithError(network.ConnErrorCode(code))
}

type stream struct {
	s  network.Stream
	rw io.ReadWriteCloser
}

var _ conn.Stream = stream{}

func (s stream) Conn() conn.Conn                    { return libp2pConn{s.s.Conn()} }
func (s stream) Read(p []byte) (int, error)         { return s.rw.Read(p) }
func (s stream) Write(p []byte) (int, error)        { return s.rw.Write(p) }
func (s stream) Close() error                       { return s.rw.Close() }
func (s stream) ResetWithError(code uint64)         { s.s.ResetWithError(network.StreamErrorCode(code)) }
func (s stream) SetReadDeadline(t time.Time) error  { return s.s.SetReadDeadline(t) }
func (s stream) SetWriteDeadline(t time.Time) error { return s.s.SetWriteDeadline(t) }
func (s stream) SetDeadline(t time.Time) error      { return s.s.SetDeadline(t) }
