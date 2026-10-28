package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	golibp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"github.com/sukunrt/go-waggle/waggle"
)

type syncBuffer struct {
	mx sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.b.String()
}

func newTestNode(t *testing.T) (*waggleHost, *syncBuffer) {
	h, err := golibp2p.New(golibp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { h.Close() })
	buf := &syncBuffer{}
	wh := &waggleHost{
		Host:    h,
		logger:  slog.New(slog.NewJSONHandler(buf, nil)),
		topics:  make(map[string]*waggle.Topic),
		objects: make(map[string]map[uint64]*Object),
	}
	require.NoError(t, wh.InitWaggle())
	return wh, buf
}

func TestTwoNodes(t *testing.T) {
	const topic = "waggle-topic"
	const objectID = uint64(14549377870619113110)
	a, aLog := newTestNode(t)
	b, bLog := newTestNode(t)
	require.NoError(t, a.SubscribeToTopic(topic))
	require.NoError(t, b.SubscribeToTopic(topic))
	require.NoError(t, a.Host.Connect(context.Background(), peer.AddrInfo{ID: b.Host.ID(), Addrs: b.Host.Addrs()}))

	hasPeer := func(wh *waggleHost) func() bool {
		return func() bool { return len(wh.topics[topic].PeersWithCapacity(waggle.ControlStream)) == 1 }
	}
	require.Eventually(t, hasPeer(a), 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, hasPeer(b), 5*time.Second, 10*time.Millisecond)

	require.NoError(t, a.AddPiece(objectID, topic, 0x30))
	require.NoError(t, b.AddPiece(objectID, topic, 0xCF))
	require.NoError(t, a.Publish(objectID, topic))
	require.NoError(t, b.Publish(objectID, topic))

	done := func(buf *syncBuffer) func() bool {
		return func() bool { return strings.Contains(buf.String(), `"msg":"All parts received"`) }
	}
	require.Eventually(t, done(aLog), 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, done(bLog), 5*time.Second, 10*time.Millisecond)
	require.Contains(t, aLog.String(), `"object_id":"14549377870619113110"`)
}

func TestTenNodes(t *testing.T) {
	const topic = "waggle-topic"
	const objectID = uint64(14549377870619113110)
	const n = 10
	nodes := make([]*waggleHost, n)
	logs := make([]*syncBuffer, n)
	for i := range n {
		nodes[i], logs[i] = newTestNode(t)
		require.NoError(t, nodes[i].SubscribeToTopic(topic))
	}

	neighbors := make([]map[int]bool, n)
	for i := range n {
		neighbors[i] = make(map[int]bool)
	}
	for i := range n {
		for k := 1; k <= 3; k++ {
			j := (i + k) % n
			neighbors[i][j], neighbors[j][i] = true, true
			h := nodes[j].Host
			require.NoError(t, nodes[i].Host.Connect(context.Background(), peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}))
		}
	}
	for i := range n {
		require.Eventually(t, func() bool {
			return len(nodes[i].topics[topic].PeersWithCapacity(waggle.ControlStream)) == len(neighbors[i])
		}, 10*time.Second, 10*time.Millisecond, "node %d", i)
	}

	// Each node has one piece. Node i and node i+8 have the same piece.
	for i := range n {
		require.NoError(t, nodes[i].AddPiece(objectID, topic, 1<<(i%pieceCount)))
	}
	for i := range n {
		require.NoError(t, nodes[i].Publish(objectID, topic))
	}

	for i := range n {
		require.Eventually(t, func() bool {
			return strings.Contains(logs[i].String(), `"msg":"All parts received"`)
		}, 10*time.Second, 10*time.Millisecond, "node %d", i)
	}
}

func TestMakePieces(t *testing.T) {
	data := makePieces(5, 0x81)
	require.Len(t, data, 1+2*pieceSize)
	require.Equal(t, byte(0x81), data[0])
	o := &Object{piecesMetadata: []byte{0}, pieces: make([][]byte, pieceCount)}
	require.NoError(t, o.extend(data))
	require.Equal(t, byte(0x81), o.piecesMetadata[0])
	// Piece 7 starts at objectID + 7*128.
	require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 3, 133}, o.pieces[7][:8])
	require.Equal(t, data, o.piecesFor(0x81))
}
