package tmessage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/bcicen/jstream"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"

	"github.com/iyear/tdl/core/dcpool"
	"github.com/iyear/tdl/core/logctx"
	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/core/util/tutil"
)

const (
	keyID       = "id"
	typeMessage = "message"
)

// getPeerID extracts the peer ID from InputPeerClass
func getPeerID(peer tg.InputPeerClass) int64 {
	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		return p.ChannelID
	case *tg.InputPeerChat:
		return p.ChatID
	case *tg.InputPeerUser:
		return p.UserID
	default:
		return 0
	}
}

type fMessage struct {
	ID        int         `mapstructure:"id"`
	Type      string      `mapstructure:"type"`
	Time      string      `mapstructure:"date_unixtime"`
	File      string      `mapstructure:"file"`
	Photo     string      `mapstructure:"photo"`
	FromID    string      `mapstructure:"from_id"`
	From      string      `mapstructure:"from"`
	Text      interface{} `mapstructure:"text"`
	ChannelID int64       `mapstructure:"ChannelID"` // optional: custom channel ID for mixed sources
}

func FromFile(ctx context.Context, pool dcpool.Pool, kvd storage.Storage, files []string, onlyMedia bool) ParseSource {
	return func() ([]*Dialog, error) {
		dialogs := make([]*Dialog, 0)

		for _, file := range files {
			ds, err := parseFile(ctx, pool.Default(ctx), kvd, file, onlyMedia)
			if err != nil {
				return nil, err
			}

			for _, d := range ds {
				logctx.From(ctx).Debug("Parse file",
					zap.String("file", file),
					zap.Int64("peer_id", getPeerID(d.Peer)),
					zap.Int("num", len(d.Messages)))
			}
			dialogs = append(dialogs, ds...)
		}

		return dialogs, nil
	}
}

func parseFile(ctx context.Context, client *tg.Client, kvd storage.Storage, file string, onlyMedia bool) ([]*Dialog, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	peer, err := getChatInfo(ctx, client, kvd, f)
	if err != nil {
		return nil, err
	}
	logctx.From(ctx).Debug("Got peer info",
		zap.Int64("id", peer.ID()),
		zap.String("name", peer.VisibleName()))

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return collect(ctx, client, kvd, f, peer, onlyMedia)
}

func collect(ctx context.Context, client *tg.Client, kvd storage.Storage, r io.Reader, defaultPeer peers.Peer, onlyMedia bool) ([]*Dialog, error) {
	d := jstream.NewDecoder(r, 2)
	manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(client)

	// Use map to support multiple ChannelIDs, key is peer ID
	msgMap := make(map[int64]*Dialog)
	// Initialize default peer
	msgMap[defaultPeer.ID()] = &Dialog{
		Peer:     defaultPeer.InputPeer(),
		Messages: make([]int, 0),
	}

	for mv := range d.Stream() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			fm := fMessage{}

			if mv.ValueType != jstream.Object {
				continue
			}

			if err := mapstructure.WeakDecode(mv.Value, &fm); err != nil {
				return nil, err
			}

			if fm.ID < 0 || fm.Type != typeMessage {
				continue
			}

			if fm.File == "" && fm.Photo == "" && onlyMedia {
				continue
			}

			// Determine which peer to use
			var targetPeerID int64
			if fm.ChannelID != 0 {
				targetPeerID = fm.ChannelID
			} else {
				targetPeerID = defaultPeer.ID()
			}

			// If new ChannelID, create corresponding Dialog
			if _, ok := msgMap[targetPeerID]; !ok {
				peer, err := tutil.GetInputPeer(ctx, manager, strconv.FormatInt(targetPeerID, 10))
				if err != nil {
					return nil, fmt.Errorf("failed to get peer for ChannelID %d: %w", targetPeerID, err)
				}
				msgMap[targetPeerID] = &Dialog{
					Peer:     peer.InputPeer(),
					Messages: make([]int, 0),
				}
			}

			msgMap[targetPeerID].Messages = append(msgMap[targetPeerID].Messages, fm.ID)
		}
	}

	// Convert to slice
	dialogs := make([]*Dialog, 0, len(msgMap))
	for _, dialog := range msgMap {
		dialogs = append(dialogs, dialog)
	}

	return dialogs, nil
}

func getChatInfo(ctx context.Context, client *tg.Client, kvd storage.Storage, r io.Reader) (peers.Peer, error) {
	d := jstream.NewDecoder(r, 1).EmitKV()

	chatID := int64(0)

	for mv := range d.Stream() {
		_kv, ok := mv.Value.(jstream.KV)
		if !ok {
			continue
		}

		if _kv.Key == keyID {
			chatID = int64(_kv.Value.(float64))
		}

		if chatID != 0 {
			break
		}
	}

	if chatID == 0 {
		return nil, errors.New("can't get chat type or chat id")
	}

	manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(client)
	return tutil.GetInputPeer(ctx, manager, strconv.FormatInt(chatID, 10))
}
