package main

import (
	"crypto/ed25519"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var paramsFlag = flag.String("params", "", "path to params.json")

func main() {
	start := time.Now()
	flag.Parse()
	params, err := readParams(*paramsFlag)
	if err != nil {
		log.Fatal(err)
	}
	nodeID, err := readNodeID()
	if err != nil {
		log.Fatal(err)
	}
	h, err := newHost(nodeID)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("PeerID", "id", h.ID().String(), "node_id", nodeID)

	if err := run(h, nodeID, start, params, logger); err != nil {
		log.Fatal(err)
	}
}

func readNodeID() (int, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return 0, err
	}
	var id int
	if _, err := fmt.Sscanf(hostname, "node%d", &id); err != nil {
		return 0, fmt.Errorf("parse node id from hostname %q: %w", hostname, err)
	}
	return id, nil
}

// nodePrivKey returns the ed25519 key with the little-endian node ID as seed.
func nodePrivKey(id int) crypto.PrivKey {
	seed := make([]byte, ed25519.SeedSize)
	binary.LittleEndian.PutUint32(seed, uint32(id))
	k, err := crypto.UnmarshalEd25519PrivateKey(ed25519.NewKeyFromSeed(seed))
	if err != nil {
		panic(err)
	}
	return k
}

func nodePeerID(id int) peer.ID {
	p, err := peer.IDFromPrivateKey(nodePrivKey(id))
	if err != nil {
		panic(err)
	}
	return p
}

func newHost(nodeID int) (host.Host, error) {
	return libp2p.New(
		libp2p.Identity(nodePrivKey(nodeID)),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/9000"),
		libp2p.ConnectionManager(connmgr.NullConnMgr{}),
	)
}
