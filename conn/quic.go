package conn

import (
	"context"
	"time"

	"github.com/quic-go/quic-go"
)

type QUICConn struct {
	Conn *quic.Conn
}

var _ Conn = QUICConn{nil}

func (q QUICConn) AcceptStream(ctx context.Context) (Stream, error) {
	stream, err := q.Conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return quicStream{qstream: stream}, nil
}

func (q QUICConn) NewStream() (Stream, error) {
	stream, err := q.Conn.OpenStream()
	if err != nil {
		return nil, err
	}
	return quicStream{qstream: stream}, nil
}

// CloseWithError closes the connection
func (q QUICConn) CloseWithError(code uint64, description string) error {
	return q.Conn.CloseWithError(quic.ApplicationErrorCode(code), description)
}

type quicStream struct {
	qstream *quic.Stream
}

// Close implements [Stream].
func (q quicStream) Close() error {
	return q.qstream.Close()
}

// Read implements [Stream].
func (q quicStream) Read(p []byte) (int, error) {
	return q.qstream.Read(p)
}

// ResetWithError implements [Stream].
func (q quicStream) ResetWithError(errorCode uint64) {
	q.qstream.CancelRead(quic.StreamErrorCode(errorCode))
	q.qstream.CancelWrite(quic.StreamErrorCode(errorCode))
}

// SetDeadline implements [Stream].
func (q quicStream) SetDeadline(t time.Time) error {
	return q.qstream.SetDeadline(t)
}

// SetReadDeadline implements [Stream].
func (q quicStream) SetReadDeadline(t time.Time) error {
	return q.qstream.SetReadDeadline(t)
}

// SetWriteDeadline implements [Stream].
func (q quicStream) SetWriteDeadline(t time.Time) error {
	return q.qstream.SetWriteDeadline(t)
}

// Write implements [Stream].
func (q quicStream) Write(p []byte) (int, error) {
	return q.qstream.Write(p)
}

var _ Stream = quicStream{nil}
