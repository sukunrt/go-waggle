package waggle

import (
	"github.com/sukunrt/go-waggle/conn"
)

type Waggle struct{}

func (w *Waggle) HandleInboundStream(p conn.PeerID, s conn.Stream) {}

func (w *Waggle) AddPeer(p conn.PeerID, c conn.Conn) {}

func (w *Waggle) NewTopic() {}

type Topic struct {
	Name string
}

func (t *Topic) HandleSendStream(p conn.PeerID, s conn.Stream) {}

func (t *Topic) HandleReceiveStream(p conn.PeerID, s conn.Stream) {}

func (t *Topic) Peers() []conn.PeerID

func (t *Topic) SendMessage(p conn.PeerID, msg []byte)

func (t *Topic) ReceiveMessage() (p conn.PeerID, msg []byte) { return "", nil }
