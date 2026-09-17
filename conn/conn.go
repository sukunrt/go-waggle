package conn

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerID = peer.ID

const (
	WaggleProtocolID = "/wag-1"
)

type Conn interface {
	NewStream() (Stream, error)
	AcceptStream(ctx context.Context) (Stream, error)
}

type Stream interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	ResetWithError(errorCode uint64)
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetDeadline(t time.Time) error
}
