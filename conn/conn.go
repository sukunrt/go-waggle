package conn

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerID = peer.ID

const (
	WaggleProtocolID = "/waggle/1.0"

	StreamErrorCodeUndecided = 42
)

type Conn interface {
	NewStream() (Stream, error)
	AcceptStream(ctx context.Context) (Stream, error)
	CloseWithError(code uint64, description string) error
}

type Stream interface {
	Conn() Conn

	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error

	ResetWithError(errorCode uint64)
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetDeadline(t time.Time) error
}
