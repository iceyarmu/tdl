package tmessage

import (
	"github.com/gotd/td/tg"
)

type MessageItem struct {
	ID        int
	ChannelID int64 // 0 表示使用 Dialog 的 Peer
}

type Dialog struct {
	Peer     tg.InputPeerClass
	Messages []MessageItem
}

type ParseSource func() ([]*Dialog, error)

func Parse(src ParseSource) ([]*Dialog, error) {
	return src()
}
