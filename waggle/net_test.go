package waggle

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/marcopolo/simnet"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
	"github.com/sukunrt/go-waggle/conn"
)

type testNet struct {
	Nodes []*node

	sn    *simnet.Simnet
	nodes map[conn.PeerID]*node
}

func generateSelfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(10 * time.Hour),
		DNSNames:     []string{"unknown"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	require.NoError(t, err)

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  privateKey,
	}
}

func newTestNet(t *testing.T, numNodes int) *testNet {
	standardLinkSettings := simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: 100 * simnet.Mibps},
		Uplink:   simnet.LinkSettings{BitsPerSecond: 100 * simnet.Mibps},
	}
	sn := &simnet.Simnet{
		LatencyFunc: simnet.StaticLatency(10 * time.Millisecond),
	}
	nodes := make(map[conn.PeerID]*node)
	nodesList := make([]*node, 0)
	tn := &testNet{sn: sn, nodes: nodes, Nodes: nodesList}

	t.Cleanup(tn.Close)
	for i := range numNodes {
		sconn := sn.NewEndpoint(&net.UDPAddr{IP: simnet.IntToPublicIPv4(i), Port: 80}, standardLinkSettings)
		qt := &quic.Transport{
			Conn: sconn,
		}
		ql, err := qt.Listen(&tls.Config{
			Certificates: []tls.Certificate{generateSelfSignedCert(t)},
			NextProtos:   []string{conn.WaggleProtocolID},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		n := &node{
			P:         conn.PeerID(fmt.Sprintf("peer-%d", i)),
			Transport: qt,
			TestNet:   tn,
			Listener:  ql,
			Waggle:    NewWaggle(),
		}
		tn.nodes[n.P] = n
		tn.Nodes = append(tn.Nodes, n)
	}
	tn.sn.Start()
	return tn
}

func (n *testNet) DialAddr(p conn.PeerID) (net.Addr, error) {
	node, ok := n.nodes[p]
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", p)
	}
	return node.Transport.Conn.LocalAddr(), nil
}

func (n *testNet) Close() {
	for _, n := range n.nodes {
		n.Close()
	}
	n.sn.Close()
}

type node struct {
	P         conn.PeerID
	Transport *quic.Transport
	TestNet   *testNet
	Listener  *quic.Listener
	Waggle    *Waggle
}

func (n *node) DialPeer(ctx context.Context, p conn.PeerID) (conn.Conn, error) {
	addr, err := n.TestNet.DialAddr(p)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		ServerName:         "unknown",
		InsecureSkipVerify: true,
		NextProtos:         []string{conn.WaggleProtocolID},
	}
	qc, err := n.Transport.Dial(ctx, addr, tlsConfig, nil)
	if err != nil {
		return nil, err
	}
	return conn.QUICConn{Conn: qc}, nil
}

func (n *node) Close() {
	n.Transport.Close()
	n.Transport.Conn.Close()
}

func (n *node) Accept(ctx context.Context) (conn.Conn, error) {
	c, err := n.Listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return conn.QUICConn{Conn: c}, nil
}

func TestDialSucceeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 10
		tn := newTestNet(t, 10)
		for i := range n {
			for j := range n {
				if i == j {
					continue
				}
				c1, err := tn.Nodes[i].DialPeer(t.Context(), tn.Nodes[j].P)
				require.NoError(t, err)
				defer c1.CloseWithError(0, "")

				c2, err := tn.Nodes[j].Accept(t.Context())
				require.NoError(t, err)
				defer c2.CloseWithError(0, "")
			}
		}
	})
}
