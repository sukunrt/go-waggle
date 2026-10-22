package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/bits"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sukunrt/go-waggle/libp2p"
	"github.com/sukunrt/go-waggle/pb"
	"github.com/sukunrt/go-waggle/waggle"
)

const (
	pieceCount = 8
	pieceSize  = 1024
)

func run(host host.Host, nodeID int, start time.Time, params ExperimentParams, logger *slog.Logger) error {
	wh := &waggleHost{
		Host:    host,
		logger:  logger,
		topics:  make(map[string]*waggle.Topic),
		objects: make(map[string]map[uint64]*Object),
	}
	for _, instruction := range params.Script {
		switch in := instruction.(type) {
		case *IfNodeIDEquals:
			if in.NodeID != nodeID {
				continue
			}
			switch inn := in.Instruction.(type) {
			case *Connect:
				for _, id := range inn.ConnectTo {
					if err := wh.Connect(id); err != nil {
						logger.Error("Connect failed", "node_id", id, "error", err)
						return err
					}
				}
			case *InitWaggle:
				if err := wh.InitWaggle(); err != nil {
					logger.Error("InitWaggle failed", "error", err)
					return err
				}
			case *AddPiece:
				if err := wh.AddPiece(inn.ObjectID, inn.TopicID, inn.Parts); err != nil {
					logger.Error("AddPiece failed", "error", err)
					return err
				}
			default:
				return fmt.Errorf("unsupported instruction in ifNodeIDEquals: %T", in.Instruction)
			}
		case *WaitUntil:
			time.Sleep(time.Until(start.Add(time.Duration(in.ElapsedSeconds) * time.Second)))
		case *SubscribeToTopic:
			if err := wh.SubscribeToTopic(in.TopicID); err != nil {
				logger.Error("SubscribeToTopic failed", "error", err)
				return err
			}
		case *Publish:
			if err := wh.Publish(in.ObjectID, in.TopicID); err != nil {
				logger.Error("Publish failed", "error", err)
				return err
			}
		default:
			return fmt.Errorf("unsupported top-level instruction: %T", in)
		}
	}
	return nil
}

type waggleHost struct {
	Waggle          *waggle.Waggle
	Host            host.Host
	logger          *slog.Logger
	stop            func()
	topics          map[string]*waggle.Topic
	incomingPieceCh chan objectPieceMessage
	addPieceCh      chan objectPiece
	publishCh       chan objectPiece
	objects         map[string]map[uint64]*Object // topicID => objectID => object
}

type objectPieceMessage struct {
	*pb.ObjectPieces
	TopicID string
	From    peer.ID
}

type objectPiece struct {
	ObjectID      uint64
	PieceMetadata []byte
	Piece         []byte
	TopicID       string
}

func (wh *waggleHost) Connect(nodeID int) error {
	addrs, err := net.LookupHost(fmt.Sprintf("node%d", nodeID))
	if err != nil {
		return err
	}
	ai, err := peer.AddrInfoFromString(fmt.Sprintf("/ip4/%s/tcp/9000/p2p/%s", addrs[0], nodePeerID(nodeID)))
	if err != nil {
		return err
	}
	return wh.Host.Connect(context.Background(), *ai)
}

func (wh *waggleHost) InitWaggle() error {
	w := waggle.NewWaggle()
	stop, err := libp2p.ConfigureHost(w, wh.Host)
	if err != nil {
		return err
	}
	wh.Waggle = w
	wh.stop = stop
	wh.incomingPieceCh = make(chan objectPieceMessage, 4)
	// Unbuffered so that the loop handles AddPiece and Publish in script order.
	wh.addPieceCh = make(chan objectPiece)
	wh.publishCh = make(chan objectPiece)
	go wh.runWaggleLoop()
	return nil
}

func (wh *waggleHost) SubscribeToTopic(topicID string) error {
	t, err := wh.Waggle.NewTopic(topicID)
	if err != nil {
		return err
	}
	t.Subscribe()
	wh.topics[topicID] = t
	go wh.readTopic(t)
	return nil
}

func (wh *waggleHost) AddPiece(objectID uint64, topicID string, parts uint8) error {
	wh.addPieceCh <- objectPiece{
		ObjectID:      objectID,
		PieceMetadata: []byte{parts},
		Piece:         makePieces(objectID, parts),
		TopicID:       topicID,
	}
	return nil
}

func (wh *waggleHost) Publish(objectID uint64, topicID string) error {
	wh.publishCh <- objectPiece{ObjectID: objectID, TopicID: topicID}
	return nil
}

func (wh *waggleHost) readTopic(t *waggle.Topic) error {
	for {
		p, piece := t.ReceiveMessage()
		if piece == nil {
			return nil
		}
		wh.incomingPieceCh <- objectPieceMessage{
			ObjectPieces: piece,
			TopicID:      t.TopicID,
			From:         p,
		}
	}
}

func (wh *waggleHost) runWaggleLoop() {
	for {
		select {
		case p := <-wh.addPieceCh:
			o := wh.getOrCreateObject(p.TopicID, p.ObjectID)
			if _, err := wh.extend(p.ObjectID, o, p.Piece); err != nil {
				wh.logger.Error("AddPiece failed", "error", err)
			}
		case p := <-wh.publishCh:
			o, ok := wh.objects[p.TopicID][p.ObjectID]
			if !ok {
				wh.logger.Error("Publish failed: unknown object", "topic", p.TopicID, "object_id", strconv.FormatUint(p.ObjectID, 10))
				continue
			}
			wh.flood(p.TopicID, p.ObjectID, o)
		case im := <-wh.incomingPieceCh:
			wh.handleIncoming(im)
		}
	}
}

func (wh *waggleHost) handleIncoming(im objectPieceMessage) {
	if len(im.ObjectID) != 8 {
		wh.logger.Error("invalid object id", "from", im.From, "len", len(im.ObjectID))
		return
	}
	id := binary.BigEndian.Uint64(im.ObjectID)
	o := wh.getOrCreateObject(im.TopicID, id)
	if len(im.PiecesMetadata) == 1 {
		o.peerHas[im.From] |= im.PiecesMetadata[0]
	}
	if has, ok := o.peerHas[im.From]; ok && len(im.Pieces) > 0 {
		o.peerHas[im.From] = has | im.Pieces[0]
	}
	added, err := wh.extend(id, o, im.Pieces)
	if err != nil {
		wh.logger.Error("invalid pieces", "from", im.From, "error", err)
		return
	}
	if added {
		wh.flood(im.TopicID, id, o)
	} else {
		wh.send(im.TopicID, id, o, im.From)
	}
}

func (wh *waggleHost) getOrCreateObject(topicID string, objectID uint64) *Object {
	objs, ok := wh.objects[topicID]
	if !ok {
		objs = make(map[uint64]*Object)
		wh.objects[topicID] = objs
	}
	o, ok := objs[objectID]
	if !ok {
		o = &Object{
			TopicID:        topicID,
			piecesMetadata: []byte{0},
			pieces:         make([][]byte, pieceCount),
			peerHas:        make(map[peer.ID]byte),
			advertised:     make(map[peer.ID]byte),
		}
		objs[objectID] = o
	}
	return o
}

// extend adds the pieces in data to o and logs when o becomes complete.
func (wh *waggleHost) extend(objectID uint64, o *Object, data []byte) (added bool, err error) {
	before := o.piecesMetadata[0]
	if err := o.extend(data); err != nil {
		return false, err
	}
	if before == o.piecesMetadata[0] {
		return false, nil
	}
	if o.piecesMetadata[0] == 0xFF {
		wh.logger.Info("All parts received", "object_id", strconv.FormatUint(objectID, 10))
	}
	return true, nil
}

func (wh *waggleHost) flood(topicID string, objectID uint64, o *Object) {
	t, ok := wh.topics[topicID]
	if !ok {
		return
	}
	for _, p := range t.PeersWithCapacity() {
		wh.send(topicID, objectID, o, p)
	}
}

// send sends p the pieces it is missing when its metadata is known.
// It sends our metadata when p has not seen it.
func (wh *waggleHost) send(topicID string, objectID uint64, o *Object, p peer.ID) {
	t, ok := wh.topics[topicID]
	if !ok {
		return
	}
	ours := o.piecesMetadata[0]
	msg := &pb.ObjectPieces{ObjectID: binary.BigEndian.AppendUint64(nil, objectID)}
	if has, ok := o.peerHas[p]; ok {
		if missing := ours &^ has; missing != 0 {
			msg.Pieces = o.piecesFor(missing)
			o.peerHas[p] = has | missing
		}
	}
	if adv, ok := o.advertised[p]; !ok || adv != ours {
		msg.PiecesMetadata = []byte{ours}
		o.advertised[p] = ours
	}
	if msg.Pieces == nil && msg.PiecesMetadata == nil {
		return
	}
	t.SendMessage(p, msg)
}

// makePieces returns the pieces wire format for parts. Piece i holds 128
// big-endian uint64 values from objectID+i*128.
func makePieces(objectID uint64, parts uint8) []byte {
	data := []byte{parts}
	for i := range pieceCount {
		if parts&(1<<i) == 0 {
			continue
		}
		for j := range pieceSize / 8 {
			data = binary.BigEndian.AppendUint64(data, objectID+uint64(i*pieceSize/8+j))
		}
	}
	return data
}

type Object struct {
	TopicID        string
	piecesMetadata []byte
	pieces         [][]byte
	peerHas        map[peer.ID]byte // pieces each peer has, from its metadata and our sends
	advertised     map[peer.ID]byte // metadata last sent to each peer
}

func (o *Object) extend(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	bitmap, data := data[0], data[1:]
	if len(data) != bits.OnesCount8(bitmap)*pieceSize {
		return errors.New("pieces length does not match bitmap")
	}
	pieces := slices.Collect(slices.Chunk(data, pieceSize))
	for i := range pieceCount {
		if bitmap&(1<<i) == 0 {
			continue
		}
		piece := pieces[0]
		pieces = pieces[1:]
		if o.piecesMetadata[0]&(1<<i) != 0 {
			continue
		}
		o.pieces[i] = piece
		o.piecesMetadata[0] |= 1 << i
	}
	return nil
}

func (o *Object) piecesFor(bitmap byte) []byte {
	data := []byte{bitmap}
	for i := range pieceCount {
		if bitmap&(1<<i) != 0 {
			data = append(data, o.pieces[i]...)
		}
	}
	return data
}
