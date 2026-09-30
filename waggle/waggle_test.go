package waggle

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"github.com/sukunrt/go-waggle/conn"
	"github.com/sukunrt/go-waggle/pb"
)

func TestWaggleMessageSending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 2
		tn := newTestNet(t, 2)

		n1, n2 := tn.Nodes[0], tn.Nodes[1]
		tp1, err := n1.Waggle.NewTopic("topic1")
		require.NoError(t, err)
		tp1.Subscribe()
		defer tp1.Close()

		tp2, err := n2.Waggle.NewTopic("topic1")
		require.NoError(t, err)
		defer tp2.Close()

		c1, err := n1.DialPeer(t.Context(), n2.P)
		require.NoError(t, err)
		defer c1.CloseWithError(0, "")

		c2, err := n2.Accept(t.Context())
		require.NoError(t, err)
		defer c2.CloseWithError(0, "")

		n1.Waggle.AddPeer(n2.P, c1)
		s, err := c2.AcceptStream(t.Context())
		require.NoError(t, err)

		topicID, err := n2.Waggle.HandleInboundStream(n1.P, s)
		require.NoError(t, err)
		require.Equal(t, "topic1", topicID)

		synctest.Wait()

		peers := tp2.Peers()
		require.Contains(t, peers, n1.P)

		tp2.SendMessage(n1.P, &pb.ObjectPieces{
			ObjectID: []byte("1"),
		})

		p, msg := tp1.ReceiveMessage()
		require.Equal(t, p, n2.P)

		require.Equal(t, []byte("1"), msg.ObjectID)
	})
}

func connectNodes(t *testing.T, a, b *node) (conn.Conn, conn.Conn) {
	t.Helper()
	c1, err := a.DialPeer(t.Context(), b.P)
	require.NoError(t, err)
	t.Cleanup(func() { c1.CloseWithError(0, "") })

	c2, err := b.Accept(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { c2.CloseWithError(0, "") })

	a.Waggle.AddPeer(b.P, c1)
	b.Waggle.AddPeer(a.P, c2)

	return c1, c2
}

func TestWaggleMessageSendingMultiple(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const numNodes = 10
		tn := newTestNet(t, numNodes)

		topic := "topic1"
		n := tn.Nodes[0]
		tp, err := n.Waggle.NewTopic(topic)
		require.NoError(t, err)
		defer tp.Close()

		subscribers := make([]*Topic, 0, numNodes-1)
		expectedPeers := make([]conn.PeerID, 0, numNodes-1)
		for _, nn := range tn.Nodes[1:] {
			tt, err := nn.Waggle.NewTopic(topic)
			require.NoError(t, err)
			tt.Subscribe()
			defer tt.Close()
			subscribers = append(subscribers, tt)
			expectedPeers = append(expectedPeers, nn.P)

			c, _ := connectNodes(t, n, nn)
			s, err := c.AcceptStream(t.Context())
			require.NoError(t, err)

			topicID, err := n.Waggle.HandleInboundStream(nn.P, s)
			require.NoError(t, err)
			require.Equal(t, topic, topicID)

			peers := tp.Peers()
			require.ElementsMatch(t, expectedPeers, peers)
			for _, p := range peers {
				tp.SendMessage(p, &pb.ObjectPieces{
					ObjectID: []byte(nn.P),
				})
			}

			for _, subscriber := range subscribers {
				p, msg := subscriber.ReceiveMessage()
				require.Equal(t, n.P, p)
				require.NotNil(t, msg)
				require.Equal(t, []byte(nn.P), msg.ObjectID)
			}
		}

	})
}
