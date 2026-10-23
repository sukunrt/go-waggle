package waggle

import (
	"encoding/binary"
	"io"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"github.com/sukunrt/go-waggle/pb"
	"google.golang.org/protobuf/proto"
)

func TestTopicStreams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 2
		tn := newTestNet(t, 2)

		n1, n2 := tn.Nodes[0], tn.Nodes[1]
		tp, err := n1.Waggle.NewTopic("topic1", 0, nil)
		require.NoError(t, err)
		tp.Subscribe()
		defer tp.Close()

		c1, err := n1.DialPeer(t.Context(), n2.P)
		require.NoError(t, err)
		defer c1.CloseWithError(0, "")

		c2, err := n2.Accept(t.Context())
		require.NoError(t, err)
		defer c2.CloseWithError(0, "")

		n1.Waggle.AddPeer(n2.P, c1)
		s, err := c2.AcceptStream(t.Context())
		require.NoError(t, err)

		topicSubscription := &pb.TopicSubscription{}
		require.NoError(t, readProtoBuf(s, topicSubscription))
		require.Equal(t, "topic1", string(topicSubscription.TopicID))
	})

}

func readProtoBuf(r io.Reader, m proto.Message) error {
	length, err := binary.ReadUvarint(byteReader{r: r})
	if err != nil {
		return err
	}

	msg := make([]byte, length)
	if _, err := io.ReadFull(r, msg); err != nil {
		return err
	}

	return proto.Unmarshal(msg, m)
}
