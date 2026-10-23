//go:build attestation_sim

package waggle

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"maps"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/marcopolo/simnet"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
	"github.com/sukunrt/go-waggle/conn"
	"github.com/sukunrt/go-waggle/pb"
	"google.golang.org/protobuf/proto"
)

const (
	attDataSize            = 128
	attSigSize             = 100
	attPieceSize           = 4 + attSigSize
	attPeersPerSend        = 8
	attLeastSentBatch      = 20
	attMinRefill           = 5
	attRequestPeers        = 2 // Open requests per id. Tail mode can use up to attMaxReqPeers.
	attMaxReqPeers         = 4
	attMaxRequest          = 50
	attWallTimeLimit       = 10 * time.Minute
	attMaxInbound          = 20
	attTopic               = "attestations"
	attMetaHave       byte = 1
	attMetaRequest    byte = 2
	attMetaSync       byte = 3 // Full have-list. The peer sends the ids that are not in it.
	attMetaHaveSync   byte = 4 // Delta list with the sync flag. The peer sends the ids that it does not know we have.
)

// Env vars can change these values. See attEnvInt and attEnvDur.
var (
	attSmall          = os.Getenv("ATT_SMALL") != ""
	attMeshNodes      = attEnvInt("ATT_MESH", attSize(500, 50))
	attMeshDegree     = attEnvInt("ATT_DEGREE", attSize(50, 10))
	attFanoutNodes    = attEnvInt("ATT_FANOUT", attSize(4000, 400)) // One attestation per fanout node.
	attSendTick       = attEnvDur("ATT_SEND_TICK", 20*time.Millisecond)
	attEagerBatch     = attEnvInt("ATT_EAGER_BATCH", 50)
	attRequestTimeout = attEnvDur("ATT_REQ_TIMEOUT", 500*time.Millisecond)
	attSendQueue      = attEnvInt("ATT_QUEUE", 0) // 0 is the waggle default.
	// attSeed sets the topology, the regions, the fanout plan and the node rngs. Seed 1 gives the old runs.
	attSeed = uint64(attEnvInt("ATT_SEED", 1))
	// attDropHave is a test-only fault: drop this percent of delta lists. The told cursor still moves.
	attDropHave = attEnvInt("ATT_DROP_HAVE", 0)
)

func attSize(full, small int) int {
	if attSmall {
		return small
	}
	return full
}

func attEnvInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", name, err))
	}
	return n
}

func attEnvBool(name string, def bool) bool {
	d := 0
	if def {
		d = 1
	}
	return attEnvInt(name, d) != 0
}

func attEnvDur(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", name, err))
	}
	return d
}

// attConf holds the knobs that have a default per strategy. Env vars override them.
type attConf struct {
	MetaTick  time.Duration
	MetaPeers int
	MaxSends  int
	DiffPush  int
	// Hedge is the minimum age of an open request before we ask a second peer for the same id. 0 is off.
	Hedge time.Duration
	// PushRare skips the push of an id if more than PushRare peers are known to have it. -1 is off.
	PushRare int
	// AnnounceEdges is the percent of edges that carry delta lists. Both ends hash the pair of peer IDs.
	AnnounceEdges int
	// TailReq turns the hedge off when at most TailReq requested ids are missing. 0 is off.
	TailReq int
	// TailPeers is the number of open requests per id in tail mode.
	TailPeers int
	// MetaGap is the minimum time between two delta lists to the same peer. 0 is off.
	MetaGap time.Duration
	// Piggyback adds the delta list for a peer to the pieces messages to that peer.
	Piggyback bool
	// PushNow pushes new ids when they arrive, not on the send tick.
	PushNow bool
	// PickOrder picks extra request targets in the order they announced the id, not at random.
	PickOrder bool
	// MetaRotate sends delta lists to one half of the peers per tick, the other half on the next tick.
	MetaRotate bool
	// SyncPeriod is the time between two sync requests. Each goes to SyncPeers random peers. 0 is off.
	SyncPeriod time.Duration
	SyncPeers  int
	// SyncOpen adds the ids with an open request to the sync list.
	SyncOpen bool
	// SyncCap is the maximum number of ids in one sync reply. 0 is no limit.
	SyncCap int
	// SyncAge limits a sync reply to the ids that we got at least SyncAge ago.
	SyncAge time.Duration
	// SyncFlag puts the sync request as a flag on the next delta list to SyncPeers announce peers.
	SyncFlag bool
}

func attConfFor(s attStrategy) attConf {
	c := attConf{MetaTick: 250 * time.Millisecond, MetaPeers: 4, MaxSends: 2, DiffPush: 50, PushRare: -1, AnnounceEdges: 100, TailPeers: attRequestPeers, SyncPeers: 2}
	tickVar := "ATT_META_TICK"
	switch s {
	case attPull:
		c.MetaTick, tickVar = 100*time.Millisecond, "ATT_PULL_META_TICK"
	case attPPDelta:
		c.MetaTick, c.MetaPeers, c.MaxSends, c.DiffPush = 20*time.Millisecond, attMeshDegree, 2, 0
		c.Hedge, c.PushRare, c.AnnounceEdges, c.TailReq, c.TailPeers = 300*time.Millisecond, 1, 50, 30, 4
	case attPullDelta:
		c.MetaTick, c.MetaPeers, c.MaxSends, c.DiffPush = 20*time.Millisecond, attMeshDegree, 0, 0
		c.Hedge = time.Second // Longer than attRequestTimeout: one open request per id.
	case attPPSimple:
		c.MetaTick, c.MetaPeers, c.MaxSends, c.DiffPush = 20*time.Millisecond, attMeshDegree, 2, 0
		c.Hedge, c.PushRare, c.AnnounceEdges, c.Piggyback = time.Second, 1, 50, true
		c.SyncPeriod, c.SyncFlag = 500*time.Millisecond, true
	}
	c.MetaTick = attEnvDur(tickVar, c.MetaTick)
	c.MetaPeers = attEnvInt("ATT_META_PEERS", c.MetaPeers)
	c.MaxSends = attEnvInt("ATT_MAX_SENDS", c.MaxSends)
	c.DiffPush = attEnvInt("ATT_DIFF_PUSH", c.DiffPush)
	c.Hedge = attEnvDur("ATT_HEDGE", c.Hedge)
	c.PushRare = attEnvInt("ATT_PUSH_RARE", c.PushRare)
	c.AnnounceEdges = attEnvInt("ATT_ANNOUNCE_EDGES", c.AnnounceEdges)
	c.TailReq = attEnvInt("ATT_TAIL_REQ", c.TailReq)
	c.TailPeers = min(attEnvInt("ATT_TAIL_PEERS", c.TailPeers), attMaxReqPeers)
	c.MetaGap = attEnvDur("ATT_META_GAP", c.MetaGap)
	c.Piggyback = attEnvBool("ATT_PIGGYBACK", c.Piggyback)
	c.PushNow = attEnvBool("ATT_PUSH_NOW", c.PushNow)
	c.PickOrder = attEnvBool("ATT_PICK_ORDER", c.PickOrder)
	c.MetaRotate = attEnvBool("ATT_META_ROTATE", c.MetaRotate)
	c.SyncPeriod = attEnvDur("ATT_SYNC_PERIOD", c.SyncPeriod)
	c.SyncPeers = attEnvInt("ATT_SYNC_PEERS", c.SyncPeers)
	c.SyncOpen = attEnvBool("ATT_SYNC_OPEN", c.SyncOpen)
	c.SyncCap = attEnvInt("ATT_SYNC_CAP", c.SyncCap)
	c.SyncAge = attEnvDur("ATT_SYNC_AGE", c.SyncAge)
	switch m := os.Getenv("ATT_SYNC_MODE"); m {
	case "":
	case "list", "flag":
		c.SyncFlag = m == "flag"
	default:
		panic("ATT_SYNC_MODE: want list or flag")
	}
	return c
}

func attConfig(c attConf) string {
	return fmt.Sprintf("seed=%d mesh=%d degree=%d fanout=%d sendTick=%s metaTick=%s metaPeers=%d diffPush=%d maxSends=%d eagerBatch=%d reqTimeout=%s hedge=%s pushRare=%d announceEdges=%d tailReq=%d tailPeers=%d metaGap=%s piggyback=%t pushNow=%t pickOrder=%t metaRotate=%t syncPeriod=%s syncPeers=%d syncOpen=%t syncCap=%d syncAge=%s syncFlag=%t dropHave=%d queue=%d",
		attSeed, attMeshNodes, attMeshDegree, attFanoutNodes, attSendTick, c.MetaTick, c.MetaPeers,
		c.DiffPush, c.MaxSends, attEagerBatch, attRequestTimeout, c.Hedge, c.PushRare, c.AnnounceEdges, c.TailReq, c.TailPeers,
		c.MetaGap, c.Piggyback, c.PushNow, c.PickOrder, c.MetaRotate, c.SyncPeriod, c.SyncPeers, c.SyncOpen, c.SyncCap, c.SyncAge, c.SyncFlag, attDropHave, attSendQueue)
}

type attRegion int

const (
	usEast attRegion = iota
	usWest
	europe
	tokyo
	singapore
	sydney
	numRegions
)

// Share of nodes in each region, in percent.
var attRegionShare = [numRegions]int{25, 10, 35, 12, 10, 8}

// One-way latency between regions, in ms.
var attRegionLatency = [numRegions][numRegions]time.Duration{
	{5, 33, 40, 75, 110, 100},
	{33, 5, 70, 50, 85, 70},
	{40, 70, 5, 115, 80, 140},
	{75, 50, 115, 5, 35, 55},
	{110, 85, 80, 35, 5, 45},
	{100, 70, 140, 55, 45, 5},
}

var (
	attData     = bytes.Repeat([]byte{0xa7}, attDataSize)
	attObjectID = sha256.Sum256(attData)
	attSigs     = attMakeSigs()
)

func attMakeSigs() [][]byte {
	sigs := make([][]byte, attFanoutNodes)
	for id := range sigs {
		h := sha256.Sum256(binary.LittleEndian.AppendUint32(nil, uint32(id)))
		sigs[id] = bytes.Repeat(h[:], attSigSize/len(h)+1)[:attSigSize]
	}
	return sigs
}

type attStrategy int

const (
	attEager attStrategy = iota
	attLeastSent
	attPushPull
	attPull
	attPPDelta
	attPullDelta
	attPPSimple
)

func (s attStrategy) String() string {
	return [...]string{"eager", "least-sent", "push-pull", "pull", "pp-delta", "pull-delta", "pp-simple"}[s]
}

// pushes reports whether the strategy pushes each new id to MaxSends peers.
func (s attStrategy) pushes() bool {
	return s == attEager || s == attPushPull || s.delta()
}

// delta reports whether the strategy sends delta lists: only the ids that are new since the last list to the peer.
func (s attStrategy) delta() bool {
	return s == attPPDelta || s == attPullDelta || s == attPPSimple
}

// TestAttestationStrategies compares forward strategies for eth attestations.
// Fanout nodes publish to 2-3 mesh nodes. The test stops when all mesh nodes have all attestations.
func TestAttestationStrategies(t *testing.T) {
	for _, s := range []attStrategy{attEager, attLeastSent, attPushPull, attPull, attPPDelta, attPullDelta, attPPSimple} {
		t.Run(s.String(), func(t *testing.T) { runAttestationSim(t, s) })
	}
}

func runAttestationSim(t *testing.T, strategy attStrategy) {
	var expired atomic.Bool
	wall := time.AfterFunc(attWallTimeLimit, func() { expired.Store(true) })
	defer wall.Stop()

	cfg := attConfFor(strategy)
	t.Logf("config: %s", attConfig(cfg))
	synctest.Test(t, func(t *testing.T) {
		rng := rand.New(rand.NewPCG(attSeed, 2))
		an := newAttNet(t, attAssignRegions(attMeshNodes+attFanoutNodes, rng))
		var rec *attRec
		if dir := os.Getenv("ATT_OUT"); dir != "" {
			rec = newAttRec(dir, an)
		}
		ctx, cancel := context.WithCancel(t.Context())
		var connWg, loopWg, recvWg sync.WaitGroup
		complete := make(chan struct{}, attMeshNodes)
		attesters := make([]*attester, attMeshNodes)
		topics := make([]*Topic, len(an.Nodes))
		t.Cleanup(func() {
			cancel()
			loopWg.Wait()
			for _, tp := range topics {
				if tp != nil {
					tp.Close()
				}
			}
			recvWg.Wait()
			for _, n := range an.Nodes {
				n.Waggle.Close()
			}
			connWg.Wait()
		})

		for i, n := range an.Nodes {
			var onSendComplete func(context.Context, conn.PeerID)
			if i < attMeshNodes {
				attesters[i] = newAttester(t, ctx, strategy, cfg, uint64(i), complete)
				if strategy == attLeastSent {
					onSendComplete = attesters[i].onSendComplete
				}
			}
			tp, err := n.Waggle.NewTopic(attTopic, attSendQueue, onSendComplete)
			require.NoError(t, err)
			topics[i] = tp
			if i < attMeshNodes {
				tp.Subscribe()
				attesters[i].topic = tp
			}
		}

		an.serve(ctx, &connWg)
		fanoutPeers := an.connect(t, ctx, rng, &connWg)
		// Let the topic streams arrive. synctest.Wait does not move the clock.
		time.Sleep(time.Second)
		synctest.Wait()
		for i, a := range attesters {
			a.self = an.Nodes[i].P
			a.setPeers(a.topic.PeersWithCapacity())
			require.Len(t, a.peers, attMeshDegree, "mesh node %d", i)
		}
		for f, peers := range fanoutPeers {
			require.Len(t, topics[attMeshNodes+f].PeersWithCapacity(), len(peers), "fanout node %d", f)
		}

		start := time.Now()
		if rec != nil {
			rec.start(start, attesters, fanoutPeers)
			loopWg.Go(func() { rec.sample(ctx) })
		}
		for _, a := range attesters {
			loopWg.Go(a.run)
			recvWg.Go(a.receive)
		}
		for f := range attFanoutNodes {
			tp := topics[attMeshNodes+f]
			msg := &pb.ObjectPieces{ObjectID: attObjectID[:], Pieces: attEncodePieces([]uint32{uint32(f)})}
			for _, p := range tp.PeersWithCapacity() {
				tp.SendMessage(p, msg)
			}
		}

		check := time.NewTicker(time.Second)
		defer check.Stop()
		completed := 0
		for completed < attMeshNodes && !expired.Load() {
			select {
			case <-complete:
				completed++
			case <-check.C:
			}
		}
		elapsed := time.Since(start)
		cancel()
		loopWg.Wait()

		attLogStats(t, strategy, start, elapsed, attesters)
		if rec != nil {
			if err := rec.write(strategy, cfg, elapsed, attesters); err != nil {
				t.Errorf("write instrumentation: %v", err)
			}
		}
		if completed < attMeshNodes {
			t.Errorf("wall time limit: %d/%d mesh nodes incomplete after %s simulated", attMeshNodes-completed, attMeshNodes, elapsed)
		}
	})
}

type attNet struct {
	*testNet
	regions []attRegion
	index   map[netip.Addr]int
	conns   []*simnet.SimConn
}

func attAddrKey(a net.Addr) netip.Addr {
	return a.(*net.UDPAddr).AddrPort().Addr().Unmap()
}

func attAssignRegions(n int, rng *rand.Rand) []attRegion {
	regions := make([]attRegion, 0, n)
	for r, share := range attRegionShare {
		for range n * share / 100 {
			regions = append(regions, attRegion(r))
		}
	}
	for len(regions) < n {
		regions = append(regions, europe)
	}
	rng.Shuffle(len(regions), func(i, j int) { regions[i], regions[j] = regions[j], regions[i] })
	return regions
}

func newAttNet(t *testing.T, regions []attRegion) *attNet {
	links := simnet.NodeBiDiLinkSettings{
		Uplink:   simnet.LinkSettings{BitsPerSecond: 25 * simnet.Mibps},
		Downlink: simnet.LinkSettings{BitsPerSecond: 50 * simnet.Mibps},
	}
	an := &attNet{regions: regions, index: make(map[netip.Addr]int, len(regions))}
	sn := &simnet.Simnet{
		LatencyFunc: func(p *simnet.Packet) time.Duration {
			from, to := an.regions[an.index[attAddrKey(p.From)]], an.regions[an.index[attAddrKey(p.To)]]
			return attRegionLatency[from][to] * time.Millisecond
		},
	}
	an.testNet = &testNet{sn: sn, nodes: make(map[conn.PeerID]*node)}
	t.Cleanup(an.testNet.Close)

	for i := range regions {
		addr := &net.UDPAddr{IP: simnet.IntToPublicIPv4(i), Port: 80}
		an.index[attAddrKey(addr)] = i
		sc := sn.NewEndpoint(addr, links)
		an.conns = append(an.conns, sc)
		qt := &quic.Transport{Conn: sc}
		ql, err := qt.Listen(&tls.Config{
			Certificates: []tls.Certificate{generateSelfSignedCert(t)},
			NextProtos:   []string{conn.WaggleProtocolID},
		}, nil)
		require.NoError(t, err)
		n := &node{
			P:         conn.PeerID(fmt.Sprintf("peer-%d", i)),
			Transport: qt,
			TestNet:   an.testNet,
			Listener:  ql,
			Waggle:    NewWaggle(),
		}
		an.nodes[n.P] = n
		an.Nodes = append(an.Nodes, n)
	}
	sn.Start()
	return an
}

func (an *attNet) serve(ctx context.Context, wg *sync.WaitGroup) {
	for _, n := range an.Nodes {
		wg.Go(func() {
			for {
				qc, err := n.Listener.Accept(ctx)
				if err != nil {
					return
				}
				p := an.Nodes[an.index[attAddrKey(qc.RemoteAddr())]].P
				wg.Go(func() { attHandleConn(ctx, n, p, conn.QUICConn{Conn: qc}) })
			}
		})
	}
}

func attHandleConn(ctx context.Context, n *node, p conn.PeerID, c conn.Conn) {
	n.Waggle.AddPeer(p, c)
	for {
		s, err := c.AcceptStream(ctx)
		if err != nil {
			return
		}
		n.Waggle.HandleInboundStream(p, s)
	}
}

// connect dials all edges at once. Each node accepts at most attMaxInbound handshakes
// at a time, because the quic accept queue holds 32. It returns the mesh peers of each fanout node.
func (an *attNet) connect(t *testing.T, ctx context.Context, rng *rand.Rand, wg *sync.WaitGroup) [][]int {
	var edges [][2]int
	order := rng.Perm(attMeshNodes)
	for off := 1; off <= attMeshDegree/2; off++ {
		for i, a := range order {
			edges = append(edges, [2]int{a, order[(i+off)%attMeshNodes]})
		}
	}
	fanoutPeers := make([][]int, attFanoutNodes)
	for f := range fanoutPeers {
		fanoutPeers[f] = rng.Perm(attMeshNodes)[:2+rng.IntN(2)]
		for _, m := range fanoutPeers[f] {
			edges = append(edges, [2]int{attMeshNodes + f, m})
		}
	}

	inbound := make([]chan struct{}, len(an.Nodes))
	for i := range inbound {
		inbound[i] = make(chan struct{}, attMaxInbound)
	}
	errs := make([]error, len(edges))
	var dials sync.WaitGroup
	for i, e := range edges {
		dials.Go(func() {
			from, to := an.Nodes[e[0]], an.Nodes[e[1]]
			inbound[e[1]] <- struct{}{}
			c, err := from.DialPeer(ctx, to.P)
			<-inbound[e[1]]
			if err != nil {
				errs[i] = err
				return
			}
			wg.Go(func() { attHandleConn(ctx, from, to.P, c) })
		})
	}
	dials.Wait()
	require.NoError(t, errors.Join(errs...))
	return fanoutPeers
}

type attStats struct {
	BytesSent    int
	MetaBytes    int
	MsgsSent     int
	Duplicates   int
	Requests     int
	IdsRequested int
	IdsServed    int
	IdsDiff      int
	Retries      int
	Held         int
	Syncs        int
	SyncReqBytes int
	IdsSynced    int
	SyncBytes    int // Pieces bytes of sync replies, without piggybacked lists.
	DroppedHave  int
	DroppedSync  int // Sync flags in dropped delta lists.
}

// attRequest is an open request to peer pi. A zero at means the slot is free.
type attRequest struct {
	pi int
	at time.Time
}

// attester is a mesh node. The run loop owns all state.
type attester struct {
	t        *testing.T
	ctx      context.Context
	strategy attStrategy
	cfg      attConf
	topic    *Topic
	rng      *rand.Rand
	complete chan<- struct{}
	incoming chan incomingMessage
	sendDone chan struct{}

	have      []bool
	count     int
	doneAt    time.Time
	got       []uint32 // Ids in the order we got them.
	gotAt     []time.Time
	told      []int    // Per peer, the part of got that the last delta list covered.
	lastMeta  []time.Time
	due       []bool // Per peer, a delta list is due. Only with MetaRotate.
	syncMe    []bool // Per peer, the next delta list has the sync flag.
	metaTicks int

	self       conn.PeerID
	peers      []conn.PeerID
	peerIdx    map[conn.PeerID]int
	announce   []bool // Per peer, the edge carries delta lists.
	known      [][]bool
	knownCount []int
	sendCount  []int

	pending  []uint32
	active   []uint32
	pushQ    [][]uint32
	requestQ [][]uint32
	replyQ   [][]uint32
	diffQ    [][]uint32
	syncQ    [][]uint32

	reqs   [][attMaxReqPeers]attRequest
	asked  [][]bool
	reqIDs []uint32
	heard  [][]int // Per missing id, the peers that announced it, in order. Only with PickOrder.

	stats attStats
	nr    *attNodeRec // Test-only records. Nil if off.
}

func newAttester(t *testing.T, ctx context.Context, strategy attStrategy, cfg attConf, seed uint64, complete chan<- struct{}) *attester {
	return &attester{
		t:          t,
		ctx:        ctx,
		strategy:   strategy,
		cfg:        cfg,
		rng:        rand.New(rand.NewPCG(seed, attSeed+6)),
		complete:   complete,
		incoming:   make(chan incomingMessage, 64),
		sendDone:   make(chan struct{}, 64),
		have:       make([]bool, attFanoutNodes),
		knownCount: make([]int, attFanoutNodes),
		sendCount:  make([]int, attFanoutNodes),
		reqs:       make([][attMaxReqPeers]attRequest, attFanoutNodes),
		asked:      make([][]bool, attFanoutNodes),
		heard:      make([][]int, attFanoutNodes),
	}
}

func (a *attester) setPeers(peers []conn.PeerID) {
	a.peers = peers
	a.peerIdx = make(map[conn.PeerID]int, len(peers))
	for i, p := range peers {
		a.peerIdx[p] = i
	}
	a.known = make([][]bool, len(peers))
	for i := range a.known {
		a.known[i] = make([]bool, attFanoutNodes)
	}
	a.pushQ = make([][]uint32, len(peers))
	a.requestQ = make([][]uint32, len(peers))
	a.replyQ = make([][]uint32, len(peers))
	a.diffQ = make([][]uint32, len(peers))
	a.syncQ = make([][]uint32, len(peers))
	a.told = make([]int, len(peers))
	a.lastMeta = make([]time.Time, len(peers))
	a.due = make([]bool, len(peers))
	a.syncMe = make([]bool, len(peers))
	a.announce = make([]bool, len(peers))
	for i, p := range peers {
		x, y := min(a.self, p), max(a.self, p)
		h := sha256.Sum256([]byte(x + "|" + y))
		a.announce[i] = int(binary.LittleEndian.Uint32(h[:])%100) < a.cfg.AnnounceEdges
	}
}

func (a *attester) onSendComplete(ctx context.Context, _ conn.PeerID) {
	select {
	case a.sendDone <- struct{}{}:
	case <-ctx.Done():
	case <-a.ctx.Done():
	}
}

func (a *attester) receive() {
	for {
		p, msg := a.topic.ReceiveMessage()
		if msg == nil {
			return
		}
		select {
		case a.incoming <- incomingMessage{P: p, Message: msg}:
		case <-a.ctx.Done():
			return
		}
	}
}

func (a *attester) run() {
	tick := time.NewTicker(attSendTick)
	defer tick.Stop()
	metaTick := a.cfg.MetaTick
	meta := time.NewTimer(time.Duration(a.rng.Int64N(int64(metaTick))))
	defer meta.Stop()
	var syncC <-chan time.Time
	if a.cfg.SyncPeriod > 0 {
		syncC = time.After(time.Duration(a.rng.Int64N(int64(a.cfg.SyncPeriod))))
	}
	for {
		select {
		case <-a.ctx.Done():
			return
		case in := <-a.incoming:
			a.handleMessage(in)
		case <-a.sendDone:
			a.pushLeastSent(1, attMinRefill)
		case <-tick.C:
			a.tick()
		case <-meta.C:
			a.sendMetadata()
			meta.Reset(metaTick)
		case <-syncC:
			a.sendSync()
			syncC = time.After(a.cfg.SyncPeriod)
		}
	}
}

func (a *attester) handleMessage(in incomingMessage) {
	pi, mesh := a.peerIdx[in.P]
	if !mesh {
		pi = -1
	}
	a.nr.receive(in)
	if len(in.Message.Pieces) > 0 {
		a.handlePieces(pi, in.Message)
		if a.cfg.PushNow && len(a.pending) > 0 {
			a.assignPending()
			for qi := range a.peers {
				if len(a.pushQ[qi]) > 0 {
					a.flush(qi)
				}
			}
		}
	}
	md := in.Message.PiecesMetadata
	if len(md) == 0 || pi < 0 {
		return
	}
	ids, err := attDecodeIDs(md[1:])
	if err != nil {
		a.t.Errorf("decode metadata: %v", err)
		return
	}
	switch md[0] {
	case attMetaHave:
		a.handleHave(pi, ids)
	case attMetaHaveSync:
		a.handleHave(pi, ids)
		a.handleSync(pi, nil)
	case attMetaRequest:
		a.handleRequest(pi, ids)
	case attMetaSync:
		a.handleSync(pi, ids)
	}
}

func (a *attester) handlePieces(pi int, msg *pb.ObjectPieces) {
	b := msg.Pieces
	if !bytes.Equal(msg.ObjectID, attObjectID[:]) || len(b) < attDataSize ||
		!bytes.Equal(b[:attDataSize], attData) || (len(b)-attDataSize)%attPieceSize != 0 {
		a.t.Errorf("invalid pieces")
		return
	}
	for b = b[attDataSize:]; len(b) > 0; b = b[attPieceSize:] {
		id := binary.LittleEndian.Uint32(b)
		if int(id) >= attFanoutNodes || !bytes.Equal(b[4:attPieceSize], attSigs[id]) {
			a.t.Errorf("invalid attestation %d", id)
			return
		}
		a.nr.piece(a, id)
		if pi >= 0 {
			a.markKnown(pi, id)
		}
		if a.have[id] {
			a.stats.Duplicates++
			continue
		}
		a.store(id)
	}
}

func (a *attester) store(id uint32) {
	a.have[id] = true
	a.count++
	a.got = append(a.got, id)
	a.gotAt = append(a.gotAt, time.Now())
	switch {
	case a.strategy.pushes():
		a.pending = append(a.pending, id)
	case a.strategy == attLeastSent:
		a.active = append(a.active, id)
	}
	if a.count == attFanoutNodes {
		a.doneAt = time.Now()
		a.complete <- struct{}{}
	}
}

func (a *attester) markKnown(pi int, id uint32) {
	if !a.known[pi][id] {
		a.known[pi][id] = true
		a.knownCount[id]++
	}
}

// handleHave requests the missing ids from pi at once.
// For push-pull, it also queues ids that pi does not have.
func (a *attester) handleHave(pi int, ids []uint32) {
	now := time.Now()
	queued := false
	for _, id := range ids {
		if a.cfg.PickOrder && !a.have[id] && !a.known[pi][id] {
			a.heard[id] = append(a.heard[id], pi)
		}
		a.markKnown(pi, id)
		a.nr.learn(a, id)
		if a.have[id] || (a.hedged(id, now) && !a.tail()) || !a.canAsk(id, pi, now) {
			continue
		}
		if r := a.freeSlot(id, now); r != nil {
			a.request(id, pi, r, now)
			queued = true
		}
	}
	if a.strategy == attPushPull && a.queueDiff(pi) {
		queued = true
	}
	if queued {
		a.flush(pi)
	}
}

// queueDiff queues up to DiffPush random ids that pi does not have.
// The have-list of pi must be full.
func (a *attester) queueDiff(pi int) bool {
	var lacks []uint32
	for id, ok := range a.have {
		if ok && !a.known[pi][id] {
			lacks = append(lacks, uint32(id))
		}
	}
	n := min(a.cfg.DiffPush, len(lacks))
	for i := range n {
		j := i + a.rng.IntN(len(lacks)-i)
		lacks[i], lacks[j] = lacks[j], lacks[i]
	}
	a.diffQ[pi] = append(a.diffQ[pi], lacks[:n]...)
	a.nr.enqueue(attKindDiff, pi, lacks[:n])
	return n > 0
}

// retryRequests sends expired requests to a peer that was not asked before, if possible.
// With a hedge, it also asks a second peer when the open requests are older than the hedge.
func (a *attester) retryRequests() {
	now := time.Now()
	tail := a.tail()
	j := 0
	for _, id := range a.reqIDs {
		if a.have[id] {
			continue
		}
		a.reqIDs[j] = id
		j++
		for i := range a.reqs[id] {
			r := &a.reqs[id][i]
			if r.at.IsZero() || now.Sub(r.at) < attRequestTimeout {
				continue
			}
			*r = attRequest{}
			if pi := a.pickHolder(id, now); pi >= 0 {
				a.request(id, pi, r, now)
				a.stats.Retries++
			}
		}
		if a.cfg.Hedge > 0 && (tail || !a.hedged(id, now)) {
			if r := a.freeSlot(id, now); r != nil {
				if pi := a.pickHolder(id, now); pi >= 0 {
					a.request(id, pi, r, now)
					a.stats.Retries++
				}
			}
		}
	}
	a.reqIDs = a.reqIDs[:j]
}

// tail reports whether only a few requested ids are missing. Then we ask a second peer at once.
func (a *attester) tail() bool {
	return a.cfg.TailReq > 0 && len(a.reqIDs) <= a.cfg.TailReq
}

// hedged reports whether id has an open request that is younger than the hedge.
func (a *attester) hedged(id uint32, now time.Time) bool {
	for _, r := range a.reqs[id] {
		if !r.at.IsZero() && now.Sub(r.at) < a.cfg.Hedge {
			return true
		}
	}
	return false
}

func (a *attester) freeSlot(id uint32, now time.Time) *attRequest {
	n := attRequestPeers
	if a.tail() {
		n = a.cfg.TailPeers
	}
	for i := range n {
		if r := &a.reqs[id][i]; r.at.IsZero() || now.Sub(r.at) >= attRequestTimeout {
			return r
		}
	}
	return nil
}

func (a *attester) request(id uint32, pi int, r *attRequest, now time.Time) {
	if a.asked[id] == nil {
		a.asked[id] = make([]bool, len(a.peers))
		a.reqIDs = append(a.reqIDs, id)
	}
	a.asked[id][pi] = true
	*r = attRequest{pi: pi, at: now}
	a.requestQ[pi] = append(a.requestQ[pi], id)
	a.nr.enqueue(attKindRequest, pi, []uint32{id})
}

// outstanding reports whether id has an open request to pi. If pi < 0, to any peer.
func (a *attester) outstanding(id uint32, pi int, now time.Time) bool {
	for _, r := range a.reqs[id] {
		if (pi < 0 || r.pi == pi) && !r.at.IsZero() && now.Sub(r.at) < attRequestTimeout {
			return true
		}
	}
	return false
}

// canAsk prefers peers that were not asked before for id.
func (a *attester) canAsk(id uint32, pi int, now time.Time) bool {
	if a.asked[id] == nil || !a.asked[id][pi] {
		return true
	}
	if a.outstanding(id, pi, now) {
		return false
	}
	for qi := range a.peers {
		if a.known[qi][id] && !a.asked[id][qi] {
			return false
		}
	}
	return true
}

// pickHolder returns a random peer known to have id that canAsk allows, or -1.
// With PickOrder, it returns the first such peer that announced id.
func (a *attester) pickHolder(id uint32, now time.Time) int {
	for _, pi := range a.heard[id] {
		if a.canAsk(id, pi, now) {
			return pi
		}
	}
	pick, n := -1, 0
	for pi := range a.peers {
		if a.known[pi][id] && a.canAsk(id, pi, now) {
			n++
			if a.rng.IntN(n) == 0 {
				pick = pi
			}
		}
	}
	return pick
}

func (a *attester) handleRequest(pi int, ids []uint32) {
	for _, id := range ids {
		if a.have[id] {
			a.replyQ[pi] = append(a.replyQ[pi], id)
			a.nr.enqueue(attKindReply, pi, []uint32{id})
		}
	}
	a.flush(pi)
}

// sendSync sends our full have-list to SyncPeers random peers with capacity.
// With SyncFlag, it sets the sync flag for SyncPeers random announce peers.
func (a *attester) sendSync() {
	if a.cfg.SyncFlag {
		var ann []int
		for pi, ok := range a.announce {
			if ok {
				ann = append(ann, pi)
			}
		}
		for i := range min(a.cfg.SyncPeers, len(ann)) {
			j := i + a.rng.IntN(len(ann)-i)
			ann[i], ann[j] = ann[j], ann[i]
			a.syncMe[ann[i]] = true
		}
		return
	}
	ids := a.got
	if a.cfg.SyncOpen {
		now := time.Now()
		ids = slices.Clone(a.got)
		for _, id := range a.reqIDs {
			if !a.have[id] && a.outstanding(id, -1, now) {
				ids = append(ids, id)
			}
		}
	}
	msg := &pb.ObjectPieces{ObjectID: attObjectID[:], PiecesMetadata: attEncodeIDs(attMetaSync, ids)}
	for i, pi := range a.freePeers() {
		if i == a.cfg.SyncPeers {
			break
		}
		a.send(pi, attKindSyncReq, msg)
		a.stats.Syncs++
	}
}

// handleSync queues the ids that we have and that pi did not list. It skips ids that we sent to pi.
// A new sync from pi replaces the old queue.
func (a *attester) handleSync(pi int, ids []uint32) {
	listed := make([]bool, attFanoutNodes)
	for _, id := range ids {
		listed[id] = true
	}
	var q []uint32
	old := time.Now().Add(-a.cfg.SyncAge)
	for i, id := range a.got {
		if a.gotAt[i].After(old) {
			break
		}
		if !listed[id] && !a.known[pi][id] {
			q = append(q, id)
			if len(q) == a.cfg.SyncCap {
				break
			}
		}
	}
	a.syncQ[pi] = q
	a.nr.enqueue(attKindSync, pi, q)
	a.flush(pi)
}

func (a *attester) tick() {
	if a.strategy.pushes() {
		a.assignPending()
	}
	a.retryRequests()
	free := make([]bool, len(a.peers))
	for _, pi := range a.freePeers() {
		free[pi] = true
	}
	for pi := range a.peers {
		if !a.hasQueued(pi) {
			continue
		}
		if !free[pi] {
			a.stats.Held++
			continue
		}
		a.flush(pi)
	}
	if a.strategy == attLeastSent {
		a.pushLeastSent(attPeersPerSend, 1)
	}
}

// assignPending queues each new id for up to MaxSends random peers.
func (a *attester) assignPending() {
	cand := make([]int, 0, len(a.peers))
	for _, id := range a.pending {
		cand = cand[:0]
		for pi := range a.peers {
			if !a.known[pi][id] {
				cand = append(cand, pi)
			}
		}
		n := min(a.cfg.MaxSends, len(cand))
		if a.cfg.PushRare >= 0 && a.knownCount[id] > a.cfg.PushRare {
			n = 0
		}
		for i := range n {
			j := i + a.rng.IntN(len(cand)-i)
			cand[i], cand[j] = cand[j], cand[i]
			a.pushQ[cand[i]] = append(a.pushQ[cand[i]], id)
			a.nr.enqueue(attKindPush, cand[i], []uint32{id})
		}
		a.sendCount[id] = n
	}
	a.pending = a.pending[:0]
}

func (a *attester) pushLeastSent(maxPeers, minBatch int) {
	sent := 0
	for _, pi := range a.freePeers() {
		if sent == maxPeers {
			return
		}
		batch := a.leastSent(pi)
		if len(batch) < minBatch {
			continue
		}
		a.sendPieces(pi, attKindPush, batch)
		for _, id := range batch {
			a.sendCount[id]++
		}
		sent++
	}
}

// leastSent returns up to attLeastSentBatch ids that peer pi needs, least sent first.
func (a *attester) leastSent(pi int) []uint32 {
	buckets := make([][]uint32, a.cfg.MaxSends)
	j := 0
	for _, id := range a.active {
		if a.sendCount[id] >= a.cfg.MaxSends || a.knownCount[id] == len(a.peers) {
			continue
		}
		a.active[j] = id
		j++
		if b := &buckets[a.sendCount[id]]; !a.known[pi][id] && len(*b) < attLeastSentBatch {
			*b = append(*b, id)
		}
	}
	a.active = a.active[:j]
	batch := make([]uint32, 0, attLeastSentBatch)
	for _, b := range buckets {
		batch = append(batch, b[:min(len(b), attLeastSentBatch-len(batch))]...)
	}
	return batch
}

// sendMetadata sends a have-list to MetaPeers random peers.
// For pull, each list holds only the ids that the peer is not known to have.
// For pp-delta and pull-delta, each list holds only the ids that are new since the last list to the peer.
func (a *attester) sendMetadata() {
	if a.count == 0 && !a.cfg.SyncFlag {
		return
	}
	if a.cfg.MetaRotate {
		// A due peer without capacity stays due for the next tick.
		for pi := range a.due {
			a.due[pi] = a.due[pi] || (pi < len(a.peers)/2) == (a.metaTicks%2 == 0)
		}
		a.metaTicks++
	}
	var full []byte
	now := time.Now()
	sent := 0
	for _, pi := range a.freePeers() {
		if sent == a.cfg.MetaPeers {
			return
		}
		var md []byte
		switch {
		case a.strategy == attPull:
			md = a.haveList(pi)
		case a.strategy.delta():
			if !a.announce[pi] || now.Sub(a.lastMeta[pi]) < a.cfg.MetaGap || (a.cfg.MetaRotate && !a.due[pi]) {
				continue
			}
			md = a.deltaList(pi)
			a.lastMeta[pi], a.due[pi] = now, false
		default:
			if full == nil {
				full = a.haveList(-1)
			}
			md = full
		}
		if md == nil {
			continue
		}
		a.send(pi, attKindHave, &pb.ObjectPieces{ObjectID: attObjectID[:], PiecesMetadata: md})
		sent++
	}
}

// deltaList encodes the ids that we got after the last delta list to pi and that pi is not known to have.
// With the sync flag for pi, it uses attMetaHaveSync and can be empty.
func (a *attester) deltaList(pi int) []byte {
	var ids []uint32
	for _, id := range a.got[a.told[pi]:] {
		if !a.known[pi][id] {
			ids = append(ids, id)
		}
	}
	a.told[pi] = len(a.got)
	flag := a.syncMe[pi]
	a.syncMe[pi] = false
	if len(ids) == 0 && !flag {
		return nil
	}
	if attDropHave > 0 && a.rng.IntN(100) < attDropHave {
		a.stats.DroppedHave++
		if flag {
			a.stats.DroppedSync++
		}
		return nil
	}
	if flag {
		a.stats.Syncs++
		return attEncodeIDs(attMetaHaveSync, ids)
	}
	return attEncodeIDs(attMetaHave, ids)
}

// haveList encodes the ids we have. If pi >= 0, it skips ids that pi is known to have.
func (a *attester) haveList(pi int) []byte {
	ids := make([]uint32, 0, a.count)
	for id, ok := range a.have {
		if ok && (pi < 0 || !a.known[pi][id]) {
			ids = append(ids, uint32(id))
		}
	}
	if len(ids) == 0 {
		return nil
	}
	return attEncodeIDs(attMetaHave, ids)
}

// freePeers returns the peers with capacity in random order.
func (a *attester) freePeers() []int {
	peers := a.topic.PeersWithCapacity()
	idx := make([]int, 0, len(peers))
	for _, p := range peers {
		if pi, ok := a.peerIdx[p]; ok {
			idx = append(idx, pi)
		}
	}
	a.rng.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })
	return idx
}

func (a *attester) hasQueued(pi int) bool {
	return len(a.requestQ[pi])+len(a.replyQ[pi])+len(a.diffQ[pi])+len(a.pushQ[pi])+len(a.syncQ[pi]) > 0
}

// flush sends queued requests, replies, pushes and sync replies to pi while it has capacity.
func (a *attester) flush(pi int) {
	n, err := a.topic.GetPeerCapacity(a.peers[pi])
	if err != nil {
		return
	}
	for ; n > 0 && a.sendNext(pi); n-- {
	}
	if a.hasQueued(pi) {
		a.stats.Held++
	}
}

func (a *attester) sendNext(pi int) bool {
	if ids := attTake(&a.requestQ[pi], attMaxRequest, func(id uint32) bool { return !a.have[id] }); len(ids) > 0 {
		a.nr.dequeue(attKindRequest, pi, ids)
		a.send(pi, attKindRequest, &pb.ObjectPieces{ObjectID: attObjectID[:], PiecesMetadata: attEncodeIDs(attMetaRequest, ids)})
		a.stats.Requests++
		a.stats.IdsRequested += len(ids)
		return true
	}
	// claim marks ids as known so that a batch has no duplicates.
	claim := func(id uint32) bool {
		if a.known[pi][id] {
			return false
		}
		a.markKnown(pi, id)
		return true
	}
	if ids := attTake(&a.replyQ[pi], attEagerBatch, claim); len(ids) > 0 {
		a.nr.dequeue(attKindReply, pi, ids)
		a.sendPieces(pi, attKindReply, ids)
		a.stats.IdsServed += len(ids)
		return true
	}
	if ids := attTake(&a.diffQ[pi], attEagerBatch, claim); len(ids) > 0 {
		a.nr.dequeue(attKindDiff, pi, ids)
		a.sendPieces(pi, attKindDiff, ids)
		a.stats.IdsDiff += len(ids)
		return true
	}
	if ids := attTake(&a.pushQ[pi], attEagerBatch, claim); len(ids) > 0 {
		a.nr.dequeue(attKindPush, pi, ids)
		a.sendPieces(pi, attKindPush, ids)
		return true
	}
	if ids := attTake(&a.syncQ[pi], attEagerBatch, claim); len(ids) > 0 {
		a.nr.dequeue(attKindSync, pi, ids)
		a.sendPieces(pi, attKindSync, ids)
		a.stats.IdsSynced += len(ids)
		return true
	}
	return false
}

func attTake(q *[]uint32, limit int, keep func(uint32) bool) []uint32 {
	var out []uint32
	i := 0
	for ; i < len(*q) && len(out) < limit; i++ {
		if keep((*q)[i]) {
			out = append(out, (*q)[i])
		}
	}
	*q = (*q)[i:]
	if len(*q) == 0 {
		*q = nil
	}
	return out
}

func (a *attester) sendPieces(pi int, kind attKind, ids []uint32) {
	for _, id := range ids {
		a.markKnown(pi, id)
	}
	msg := &pb.ObjectPieces{ObjectID: attObjectID[:], Pieces: attEncodePieces(ids)}
	if a.cfg.Piggyback && a.strategy.delta() && a.announce[pi] {
		msg.PiecesMetadata = a.deltaList(pi)
	}
	a.send(pi, kind, msg)
}

func (a *attester) send(pi int, kind attKind, msg *pb.ObjectPieces) {
	n := proto.Size(msg)
	meta := n
	if len(msg.Pieces) > 0 {
		meta = 0
		if len(msg.PiecesMetadata) > 0 {
			meta = proto.Size(&pb.ObjectPieces{PiecesMetadata: msg.PiecesMetadata})
		}
	}
	a.stats.MsgsSent++
	a.stats.BytesSent += n
	a.stats.MetaBytes += meta
	switch md := msg.PiecesMetadata; {
	case kind == attKindSyncReq, len(md) == 1 && md[0] == attMetaHaveSync:
		a.stats.SyncReqBytes += meta
	case kind == attKindSync:
		a.stats.SyncBytes += n - meta
	}
	a.nr.send(pi, kind, n, msg)
	a.topic.SendMessage(a.peers[pi], msg)
}

// attEncodePieces encodes {att_data, [(validator_id, signature)]}.
func attEncodePieces(ids []uint32) []byte {
	buf := make([]byte, 0, attDataSize+len(ids)*attPieceSize)
	buf = append(buf, attData...)
	for _, id := range ids {
		buf = binary.LittleEndian.AppendUint32(buf, id)
		buf = append(buf, attSigs[id]...)
	}
	return buf
}

func attEncodeIDs(kind byte, ids []uint32) []byte {
	buf := make([]byte, 1, 1+4*len(ids))
	buf[0] = kind
	for _, id := range ids {
		buf = binary.LittleEndian.AppendUint32(buf, id)
	}
	return buf
}

func attDecodeIDs(b []byte) ([]uint32, error) {
	if len(b)%4 != 0 {
		return nil, errors.New("bad id list length")
	}
	ids := make([]uint32, 0, len(b)/4)
	for ; len(b) > 0; b = b[4:] {
		id := binary.LittleEndian.Uint32(b)
		if int(id) >= attFanoutNodes {
			return nil, fmt.Errorf("bad id %d", id)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func attLogStats(t *testing.T, strategy attStrategy, start time.Time, elapsed time.Duration, attesters []*attester) {
	var done []time.Duration
	var total attStats
	maxBytes := 0
	for _, a := range attesters {
		if !a.doneAt.IsZero() {
			done = append(done, a.doneAt.Sub(start))
		}
		s := a.stats
		total.BytesSent += s.BytesSent
		total.MetaBytes += s.MetaBytes
		total.MsgsSent += s.MsgsSent
		total.Duplicates += s.Duplicates
		total.Requests += s.Requests
		total.IdsRequested += s.IdsRequested
		total.IdsServed += s.IdsServed
		total.IdsDiff += s.IdsDiff
		total.Retries += s.Retries
		total.Held += s.Held
		total.Syncs += s.Syncs
		total.SyncReqBytes += s.SyncReqBytes
		total.IdsSynced += s.IdsSynced
		total.SyncBytes += s.SyncBytes
		total.DroppedHave += s.DroppedHave
		total.DroppedSync += s.DroppedSync
		maxBytes = max(maxBytes, s.BytesSent)
	}
	slices.Sort(done)
	pct := func(q float64) time.Duration {
		if len(done) == 0 {
			return 0
		}
		return done[int(q*float64(len(done)-1))]
	}
	n := len(attesters)
	t.Logf("%s: %d/%d complete, %s simulated", strategy, len(done), n, elapsed)
	t.Logf("  completion  p50=%s p90=%s p99=%s max=%s", pct(0.5), pct(0.9), pct(0.99), pct(1))
	t.Logf("  bytes sent  mean=%dKB max=%dKB total=%dMB meta=%dKB/node msgs=%d", total.BytesSent/n>>10, maxBytes>>10, total.BytesSent>>20, total.MetaBytes/n>>10, total.MsgsSent)
	t.Logf("  duplicates  total=%d mean=%.1f", total.Duplicates, float64(total.Duplicates)/float64(n))
	t.Logf("  requests    msgs=%d ids=%d served=%d diff=%d retries=%d held=%d", total.Requests, total.IdsRequested, total.IdsServed, total.IdsDiff, total.Retries, total.Held)
	// first and dups count the sync reply pieces at the receiver. They need ATT_OUT.
	first, dups := -1, -1
	if attesters[0].nr != nil {
		first, dups = 0, 0
		for _, a := range attesters {
			first += a.nr.recvNew[attKindSync]
			dups += a.nr.recvDup[attKindSync]
		}
	}
	t.Logf("  sync        reqs=%d reqKB=%.1f/node ids=%d KB=%.1f/node first=%d dups=%d droppedHave=%d droppedSync=%d",
		total.Syncs, float64(total.SyncReqBytes)/float64(n)/1024, total.IdsSynced, float64(total.SyncBytes)/float64(n)/1024, first, dups, total.DroppedHave, total.DroppedSync)
	missing, pairs := map[uint32]int{}, 0
	for i, a := range attesters {
		if !a.doneAt.IsZero() {
			continue
		}
		var ids []uint32
		for id, ok := range a.have {
			if !ok {
				ids = append(ids, uint32(id))
				missing[uint32(id)]++
			}
		}
		pairs += len(ids)
		t.Logf("  incomplete  node=%d missing=%d ids=%v", i, len(ids), ids[:min(len(ids), 10)])
	}
	if len(missing) > 0 {
		ids := slices.Sorted(maps.Keys(missing))
		t.Logf("  missing     ids=%d pairs=%d first=%v", len(ids), pairs, ids[:min(len(ids), 20)])
	}
}

// Test-only instrumentation. ATT_OUT=<dir> turns it on.
// It reads global state to write the output files. It does not change what the nodes do or send.

type attKind uint8

const (
	attKindFanout attKind = iota
	attKindPush
	attKindReply
	attKindDiff
	attKindHave
	attKindRequest
	attKindSync
	attKindSyncReq
	attNumKinds
)

var attKindNames = [attNumKinds]string{"fanout", "push", "reply", "diff", "have", "request", "sync", "syncreq"}

const attBin = 10 * time.Millisecond

// attMsgRec is one message. Times are µs after t0.
// recv is -1 if the waggle send queue dropped the message, -2 if it was in flight at the end.
type attMsgRec struct {
	key                uint64
	from, to           int16
	kind               attKind
	ids                uint16
	bytes              int32
	queued, sent, recv int32
	newIDs, dups       uint16
}

// attLink holds the messages from one mesh node to another that did not arrive yet, in send order.
type attLink struct {
	mu   sync.Mutex
	msgs []attMsgRec
}

type attRec struct {
	dir         string
	an          *attNet
	t0          time.Time
	nodeIdx     map[conn.PeerID]int
	links       map[[2]int]*attLink
	nodes       []*attNodeRec
	fanoutPeers [][]int
	wireSent    [][]int32 // Per mesh node and bin, bytes into the uplink queue.
	wireRcvd    [][]int32
	fanSent     []int64 // Per bin, sum over all fanout nodes.
	fanRcvd     []int64
}

// attNodeRec holds the records of one mesh node. Only the run loop of the node writes to it.
type attNodeRec struct {
	rec   *attRec
	node  int
	peers []int
	// Per id. Times are µs after t0, -1 if no event. msg is the index in msgs of the first copy.
	arr, msg, known, reqQ, reqSent, lastReq []int32
	nReq, knownAt                           []uint8

	msgs      []attMsgRec
	lost      []attMsgRec
	cur       int
	enq       map[uint64]int32
	queued    int32
	unmatched int

	sentBytes, sentMsgs, recvNew, recvDup [attNumKinds]int
}

func newAttRec(dir string, an *attNet) *attRec {
	r := &attRec{dir: dir, an: an, nodeIdx: make(map[conn.PeerID]int, len(an.Nodes)), links: map[[2]int]*attLink{}}
	for i, n := range an.Nodes {
		r.nodeIdx[n.P] = i
	}
	return r
}

func (r *attRec) us(t time.Time) int32 {
	return int32(t.Sub(r.t0) / time.Microsecond)
}

func (r *attRec) start(t0 time.Time, attesters []*attester, fanoutPeers [][]int) {
	r.t0 = t0
	r.fanoutPeers = fanoutPeers
	neg := func() []int32 {
		s := make([]int32, attFanoutNodes)
		for i := range s {
			s[i] = -1
		}
		return s
	}
	for i, a := range attesters {
		nr := &attNodeRec{
			rec: r, node: i, cur: -1, queued: -1, enq: map[uint64]int32{},
			arr: neg(), msg: neg(), known: neg(), reqQ: neg(), reqSent: neg(), lastReq: neg(),
			nReq: make([]uint8, attFanoutNodes), knownAt: make([]uint8, attFanoutNodes),
		}
		for _, p := range a.peers {
			j := r.nodeIdx[p]
			nr.peers = append(nr.peers, j)
			r.links[[2]int{i, j}] = &attLink{}
		}
		r.nodes = append(r.nodes, nr)
		a.nr = nr
	}
	r.wireSent = make([][]int32, len(attesters))
	r.wireRcvd = make([][]int32, len(attesters))
}

// sample reads the wire byte counters of all nodes once per attBin.
func (r *attRec) sample(ctx context.Context) {
	tick := time.NewTicker(attBin)
	defer tick.Stop()
	prev := make([]simnet.ConnStats, len(r.an.conns))
	for i, c := range r.an.conns {
		prev[i] = c.Stats()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		var fs, fr int64
		for i, c := range r.an.conns {
			s := c.Stats()
			ds, dr := s.BytesSent-prev[i].BytesSent, s.BytesRcvd-prev[i].BytesRcvd
			prev[i] = s
			if i < len(r.wireSent) {
				r.wireSent[i] = append(r.wireSent[i], int32(ds))
				r.wireRcvd[i] = append(r.wireRcvd[i], int32(dr))
			} else {
				fs += int64(ds)
				fr += int64(dr)
			}
		}
		r.fanSent = append(r.fanSent, fs)
		r.fanRcvd = append(r.fanRcvd, fr)
	}
}

func attMsgKey(msg *pb.ObjectPieces) uint64 {
	b := msg.Pieces
	if len(b) == 0 {
		b = msg.PiecesMetadata
	}
	return maphash.Bytes(attHashSeed, b)
}

var attHashSeed = maphash.MakeSeed()

func attMsgIDs(msg *pb.ObjectPieces) uint16 {
	if len(msg.Pieces) > 0 {
		return uint16((len(msg.Pieces) - attDataSize) / attPieceSize)
	}
	return uint16(max(len(msg.PiecesMetadata)-1, 0) / 4)
}

func attEnqKey(kind attKind, pi int, id uint32) uint64 {
	return uint64(kind)<<56 | uint64(pi)<<32 | uint64(id)
}

func (nr *attNodeRec) send(pi int, kind attKind, n int, msg *pb.ObjectPieces) {
	if nr == nil {
		return
	}
	now := nr.rec.us(time.Now())
	q := now
	if nr.queued >= 0 {
		q, nr.queued = nr.queued, -1
	}
	to := nr.peers[pi]
	m := attMsgRec{key: attMsgKey(msg), from: int16(nr.node), to: int16(to), kind: kind, ids: attMsgIDs(msg),
		bytes: int32(n), queued: q, sent: now, recv: -2}
	nr.sentBytes[kind] += n
	nr.sentMsgs[kind]++
	l := nr.rec.links[[2]int{nr.node, to}]
	l.mu.Lock()
	l.msgs = append(l.msgs, m)
	l.mu.Unlock()
}

// receive finds the record of a message. The waggle send queue can drop messages, so it skips older records.
func (nr *attNodeRec) receive(in incomingMessage) {
	if nr == nil {
		return
	}
	from := nr.rec.nodeIdx[in.P]
	m := attMsgRec{from: int16(from), to: int16(nr.node), kind: attKindFanout, ids: attMsgIDs(in.Message), bytes: int32(proto.Size(in.Message))}
	if l := nr.rec.links[[2]int{from, nr.node}]; l != nil {
		key := attMsgKey(in.Message)
		l.mu.Lock()
		if i := slices.IndexFunc(l.msgs, func(m attMsgRec) bool { return m.key == key }); i >= 0 {
			for _, d := range l.msgs[:i] {
				d.recv = -1
				nr.lost = append(nr.lost, d)
			}
			m = l.msgs[i]
			l.msgs = l.msgs[i+1:]
		} else {
			nr.unmatched++
		}
		l.mu.Unlock()
	}
	m.recv = nr.rec.us(time.Now())
	nr.cur = len(nr.msgs)
	nr.msgs = append(nr.msgs, m)
}

// piece must be called before the node marks the sender as a holder of id.
func (nr *attNodeRec) piece(a *attester, id uint32) {
	if nr == nil {
		return
	}
	m := &nr.msgs[nr.cur]
	if a.have[id] {
		m.dups++
		nr.recvDup[m.kind]++
		return
	}
	m.newIDs++
	nr.recvNew[m.kind]++
	nr.arr[id] = m.recv
	nr.msg[id] = int32(nr.cur)
	nr.knownAt[id] = uint8(min(a.knownCount[id], 255))
}

func (nr *attNodeRec) learn(a *attester, id uint32) {
	if nr == nil || a.have[id] || nr.known[id] >= 0 {
		return
	}
	nr.known[id] = nr.rec.us(time.Now())
}

func (nr *attNodeRec) enqueue(kind attKind, pi int, ids []uint32) {
	if nr == nil {
		return
	}
	now := nr.rec.us(time.Now())
	for _, id := range ids {
		if k := attEnqKey(kind, pi, id); nr.enq[k] == 0 {
			nr.enq[k] = now + 1
		}
		if kind == attKindRequest && nr.reqQ[id] < 0 {
			nr.reqQ[id] = now
		}
	}
}

// dequeue keeps the oldest queue time of ids for the next send.
func (nr *attNodeRec) dequeue(kind attKind, pi int, ids []uint32) {
	if nr == nil {
		return
	}
	now := nr.rec.us(time.Now())
	q := now
	for _, id := range ids {
		k := attEnqKey(kind, pi, id)
		if t := nr.enq[k]; t > 0 {
			q = min(q, t-1)
			delete(nr.enq, k)
		}
		if kind == attKindRequest {
			if nr.reqSent[id] < 0 {
				nr.reqSent[id] = now
			}
			nr.lastReq[id] = now
			nr.nReq[id] = uint8(min(int(nr.nReq[id])+1, 255))
		}
	}
	nr.queued = q
}

type attNodeOut struct {
	Region    int
	Peers     []int
	DoneUs    int64
	Stats     attStats
	SentBytes [attNumKinds]int
	SentMsgs  [attNumKinds]int
	RecvNew   [attNumKinds]int
	RecvDup   [attNumKinds]int
	Lost      int
	Unmatched int
}

type attArrayOut struct {
	Name  string
	Type  string
	Count int
}

// write writes <dir>/<strategy>.json and the arrays in <dir>/<strategy>.bin.gz.
func (r *attRec) write(strategy attStrategy, cfg attConf, elapsed time.Duration, attesters []*attester) error {
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return err
	}
	var msgs []attMsgRec
	offsets := make([]int, len(r.nodes))
	for i, nr := range r.nodes {
		offsets[i] = len(msgs)
		msgs = append(msgs, nr.msgs...)
	}
	for _, nr := range r.nodes {
		msgs = append(msgs, nr.lost...)
	}
	for i, nr := range r.nodes {
		for _, j := range nr.peers {
			if l := r.links[[2]int{i, j}]; l != nil {
				msgs = append(msgs, l.msgs...)
			}
		}
	}

	ids := attMeshNodes * attFanoutNodes
	cat32 := func(f func(*attNodeRec) []int32) []int32 {
		out := make([]int32, 0, ids)
		for _, nr := range r.nodes {
			out = append(out, f(nr)...)
		}
		return out
	}
	cat8 := func(f func(*attNodeRec) []uint8) []uint8 {
		out := make([]uint8, 0, ids)
		for _, nr := range r.nodes {
			out = append(out, f(nr)...)
		}
		return out
	}
	msgRef := cat32(func(nr *attNodeRec) []int32 {
		out := slices.Clone(nr.msg)
		for i, m := range out {
			if m >= 0 {
				out[i] = m + int32(offsets[nr.node])
			}
		}
		return out
	})
	col16 := func(f func(attMsgRec) int16) []int16 {
		out := make([]int16, len(msgs))
		for i, m := range msgs {
			out[i] = f(m)
		}
		return out
	}
	colU16 := func(f func(attMsgRec) uint16) []uint16 {
		out := make([]uint16, len(msgs))
		for i, m := range msgs {
			out[i] = f(m)
		}
		return out
	}
	col32 := func(f func(attMsgRec) int32) []int32 {
		out := make([]int32, len(msgs))
		for i, m := range msgs {
			out[i] = f(m)
		}
		return out
	}
	kinds := make([]uint8, len(msgs))
	for i, m := range msgs {
		kinds[i] = uint8(m.kind)
	}
	nbins := len(r.fanSent)
	flat := func(rows [][]int32) []int32 {
		out := make([]int32, 0, len(rows)*nbins)
		for _, row := range rows {
			out = append(out, row[:nbins]...)
		}
		return out
	}

	type array struct {
		name, typ string
		data      any
		count     int
	}
	arrays := []array{
		{"id_arr", "i4", cat32(func(nr *attNodeRec) []int32 { return nr.arr }), ids},
		{"id_msg", "i4", msgRef, ids},
		{"id_known", "i4", cat32(func(nr *attNodeRec) []int32 { return nr.known }), ids},
		{"id_reqq", "i4", cat32(func(nr *attNodeRec) []int32 { return nr.reqQ }), ids},
		{"id_reqsent", "i4", cat32(func(nr *attNodeRec) []int32 { return nr.reqSent }), ids},
		{"id_lastreq", "i4", cat32(func(nr *attNodeRec) []int32 { return nr.lastReq }), ids},
		{"id_nreq", "u1", cat8(func(nr *attNodeRec) []uint8 { return nr.nReq }), ids},
		{"id_knownat", "u1", cat8(func(nr *attNodeRec) []uint8 { return nr.knownAt }), ids},
		{"msg_from", "i2", col16(func(m attMsgRec) int16 { return m.from }), len(msgs)},
		{"msg_to", "i2", col16(func(m attMsgRec) int16 { return m.to }), len(msgs)},
		{"msg_kind", "u1", kinds, len(msgs)},
		{"msg_ids", "u2", colU16(func(m attMsgRec) uint16 { return m.ids }), len(msgs)},
		{"msg_bytes", "i4", col32(func(m attMsgRec) int32 { return m.bytes }), len(msgs)},
		{"msg_queued", "i4", col32(func(m attMsgRec) int32 { return m.queued }), len(msgs)},
		{"msg_sent", "i4", col32(func(m attMsgRec) int32 { return m.sent }), len(msgs)},
		{"msg_recv", "i4", col32(func(m attMsgRec) int32 { return m.recv }), len(msgs)},
		{"msg_new", "u2", colU16(func(m attMsgRec) uint16 { return m.newIDs }), len(msgs)},
		{"msg_dup", "u2", colU16(func(m attMsgRec) uint16 { return m.dups }), len(msgs)},
		{"wire_sent", "i4", flat(r.wireSent), attMeshNodes * nbins},
		{"wire_rcvd", "i4", flat(r.wireRcvd), attMeshNodes * nbins},
		{"fan_sent", "i8", r.fanSent, nbins},
		{"fan_rcvd", "i8", r.fanRcvd, nbins},
	}

	name := strategy.String()
	f, err := os.Create(filepath.Join(r.dir, name+".bin.gz"))
	if err != nil {
		return err
	}
	defer f.Close()
	zw, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
	var layout []attArrayOut
	for _, a := range arrays {
		if err := binary.Write(zw, binary.LittleEndian, a.data); err != nil {
			return err
		}
		layout = append(layout, attArrayOut{a.name, a.typ, a.count})
	}
	if err := zw.Close(); err != nil {
		return err
	}

	nodes := make([]attNodeOut, len(attesters))
	for i, a := range attesters {
		nr := r.nodes[i]
		done := int64(-1)
		if !a.doneAt.IsZero() {
			done = int64(a.doneAt.Sub(r.t0) / time.Microsecond)
		}
		nodes[i] = attNodeOut{
			Region: int(r.an.regions[i]), Peers: nr.peers, DoneUs: done, Stats: a.stats,
			SentBytes: nr.sentBytes, SentMsgs: nr.sentMsgs, RecvNew: nr.recvNew, RecvDup: nr.recvDup,
			Lost: len(nr.lost), Unmatched: nr.unmatched,
		}
	}
	regions := make([]int, len(r.an.regions))
	for i, g := range r.an.regions {
		regions[i] = int(g)
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	out := map[string]any{
		"strategy": name,
		"config": map[string]any{
			"mesh": attMeshNodes, "degree": attMeshDegree, "fanout": attFanoutNodes,
			"send_tick_ms": ms(attSendTick), "meta_tick_ms": ms(cfg.MetaTick), "meta_peers": cfg.MetaPeers,
			"pull_meta_tick_ms": ms(cfg.MetaTick), "diff_push": cfg.DiffPush, "max_sends": cfg.MaxSends,
			"eager_batch": attEagerBatch, "req_timeout_ms": ms(attRequestTimeout), "queue": attSendQueue,
			"hedge_ms": ms(cfg.Hedge), "push_rare": cfg.PushRare, "announce_edges": cfg.AnnounceEdges, "tail_req": cfg.TailReq, "tail_peers": cfg.TailPeers,
			"seed": attSeed, "meta_gap_ms": ms(cfg.MetaGap), "piggyback": cfg.Piggyback, "push_now": cfg.PushNow, "pick_order": cfg.PickOrder, "meta_rotate": cfg.MetaRotate,
			"sync_period_ms": ms(cfg.SyncPeriod), "sync_peers": cfg.SyncPeers, "sync_open": cfg.SyncOpen, "sync_cap": cfg.SyncCap, "sync_age_ms": ms(cfg.SyncAge), "sync_flag": cfg.SyncFlag, "drop_have": attDropHave,
			"uplink_bps": 25 * simnet.Mibps, "downlink_bps": 50 * simnet.Mibps,
		},
		"config_str":   attConfig(cfg),
		"elapsed_us":   elapsed.Microseconds(),
		"bin_us":       attBin.Microseconds(),
		"nbins":        nbins,
		"kinds":        attKindNames,
		"region_names": []string{"us-east", "us-west", "europe", "tokyo", "singapore", "sydney"},
		"latency_ms":   attRegionLatency,
		"regions":      regions,
		"fanout_peers": r.fanoutPeers,
		"nodes":        nodes,
		"bin_file":     name + ".bin.gz",
		"arrays":       layout,
	}
	js, err := json.Marshal(out)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.dir, name+".json"), js, 0o644)
}
