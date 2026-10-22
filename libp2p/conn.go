package libp2p

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/sukunrt/go-waggle/conn"
	"github.com/sukunrt/go-waggle/waggle"
)

const (
	protocolID       = protocol.ID(conn.WaggleProtocolID)
	newStreamTimeout = 10 * time.Second
	maxWorkers       = 50
)

// ConfigureHost configures the host to plug in its waggle peers to w. It adds a
// peer when identify shows waggle support and removes the peer when it is no
// longer connected. It also sends inbound waggle streams to w.
//
// stop removes link and newly connected peers are not added to w.
func ConfigureHost(w *waggle.Waggle, h host.Host) (stop func(), err error) {
	sub, err := h.EventBus().Subscribe(new(event.EvtPeerIdentificationCompleted))
	if err != nil {
		return nil, err
	}

	m := &peerManager{
		waggle:    w,
		host:      h,
		semaphore: make(chan struct{}, maxWorkers),
		signal:    make(chan struct{}, 1),
		added:     make(map[peer.ID]bool),
		busy:      make(map[peer.ID]bool),
	}
	notifiee := &network.NotifyBundle{
		DisconnectedF: func(_ network.Network, c network.Conn) { m.markClosed(c.RemotePeer()) },
	}
	h.Network().Notify(notifiee)

	h.SetStreamHandler(protocolID, func(s network.Stream) {
		w.HandleInboundStream(s.Conn().RemotePeer(), stream{Stream: s, h: h})
	})

	m.wg.Go(func() { m.loop(sub) })

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

	queueMx sync.Mutex
	closed  []peer.ID
	done    []peer.ID
	signal  chan struct{}

	added map[peer.ID]bool
	busy  map[peer.ID]bool
}

func (m *peerManager) loop(sub event.Subscription) {
	for _, p := range m.host.Network().Peers() {
		m.sync(p)
	}
	for {
		m.processQueues()
		select {
		case e, ok := <-sub.Out():
			if !ok {
				return
			}
			m.sync(e.(event.EvtPeerIdentificationCompleted).Peer)
		case <-m.signal:
		}
	}
}

func (m *peerManager) markClosed(p peer.ID) {
	m.queueMx.Lock()
	m.closed = append(m.closed, p)
	m.queueMx.Unlock()
	m.notify()
}

func (m *peerManager) markDone(p peer.ID) {
	m.queueMx.Lock()
	m.done = append(m.done, p)
	m.queueMx.Unlock()
	m.notify()
}

func (m *peerManager) notify() {
	select {
	case m.signal <- struct{}{}:
	default:
	}
}

func (m *peerManager) processQueues() {
	m.queueMx.Lock()
	closed, done := m.closed, m.done
	m.closed, m.done = nil, nil
	m.queueMx.Unlock()
	for _, p := range closed {
		m.sync(p)
	}
	for _, p := range done {
		delete(m.busy, p)
		m.sync(p)
	}
}

func (m *peerManager) sync(p peer.ID) {
	if m.busy[p] {
		return
	}
	c := libp2pConn{h: m.host, p: p}
	isConnected := m.host.Network().Connectedness(p) == network.Connected && m.supports(p)
	if isConnected {
		if m.added[p] {
			return
		}
		m.added[p] = true
		m.run(p, func() { m.waggle.AddPeer(p, c) })
	} else {
		if !m.added[p] {
			return
		}
		delete(m.added, p)
		m.run(p, func() { m.waggle.DisconnectPeer(p, c) })
	}
}

func (m *peerManager) run(p peer.ID, f func()) {
	m.busy[p] = true
	m.semaphore <- struct{}{}
	m.wg.Go(func() {
		defer func() { <-m.semaphore }()
		f()
		m.markDone(p)
	})
}

func (m *peerManager) supports(p peer.ID) bool {
	protos, err := m.host.Peerstore().SupportsProtocols(p, protocolID)
	return err == nil && len(protos) > 0
}

// libp2pConn refers to all conns to a peer. The host selects the conn for each stream.
//
// When a conn closes and the peer stays connected, the streams on that conn are not reopened.
// TODO: Fix this ^
type libp2pConn struct {
	h host.Host
	p peer.ID
}

var _ conn.Conn = libp2pConn{}

func (c libp2pConn) NewStream() (conn.Stream, error) {
	ctx, cancel := context.WithTimeout(context.Background(), newStreamTimeout)
	defer cancel()
	s, err := c.h.NewStream(network.WithNoDial(ctx, "waggle"), c.p, protocolID)
	if err != nil {
		return nil, err
	}
	return stream{Stream: s, h: c.h}, nil
}

func (c libp2pConn) AcceptStream(context.Context) (conn.Stream, error) {
	return nil, errors.New("libp2p: the host stream handler receives inbound streams")
}

func (c libp2pConn) CloseWithError(code uint64, _ string) error {
	var errs []error
	for _, cc := range c.h.Network().ConnsToPeer(c.p) {
		errs = append(errs, cc.CloseWithError(network.ConnErrorCode(code)))
	}
	return errors.Join(errs...)
}

type stream struct {
	network.Stream
	h host.Host
}

var _ conn.Stream = stream{}

func (s stream) Conn() conn.Conn            { return libp2pConn{h: s.h, p: s.Stream.Conn().RemotePeer()} }
func (s stream) ResetWithError(code uint64) { s.Stream.ResetWithError(network.StreamErrorCode(code)) }
