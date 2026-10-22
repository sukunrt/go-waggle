package main

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Hash is the same as in rust-libp2p and gossipsub-interop.
func TestNodePeerID(t *testing.T) {
	h := sha256.New()
	for id := range 10_000 {
		fmt.Fprintf(h, ">%d:%s\n", id, nodePeerID(id))
	}
	require.Equal(t, "11395ea896d00ca25f7f648ebb336488ee092096a5498d90d76b92eaec27867a", fmt.Sprintf("%x", h.Sum(nil)))
}

func TestReadParams(t *testing.T) {
	params, err := readParams("testdata/params.json")
	require.NoError(t, err)
	require.Len(t, params.Script, 9)
	require.Equal(t, &IfNodeIDEquals{
		Type: "ifNodeIDEquals", NodeID: 0, Instruction: &InitWaggle{Type: "initWaggle"},
	}, params.Script[0])
	require.Equal(t, &IfNodeIDEquals{
		Type: "ifNodeIDEquals", NodeID: 0, Instruction: &Connect{Type: "connect", ConnectTo: []int{1}},
	}, params.Script[2])
	require.Equal(t, &SubscribeToTopic{Type: "subscribeToTopic", TopicID: "waggle-topic"}, params.Script[3])
	require.Equal(t, &WaitUntil{Type: "waitUntil", ElapsedSeconds: 30}, params.Script[4])
	require.Equal(t, &IfNodeIDEquals{
		Type: "ifNodeIDEquals", NodeID: 1,
		Instruction: &AddPiece{Type: "addPiece", Parts: 207, TopicID: "waggle-topic", ObjectID: 14549377870619113110},
	}, params.Script[6])
	require.Equal(t, &Publish{Type: "publish", TopicID: "waggle-topic", ObjectID: 14549377870619113110}, params.Script[7])
}
