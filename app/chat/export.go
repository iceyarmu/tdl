package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
	"github.com/jedib0t/go-pretty/v6/progress"
	"go.uber.org/multierr"

	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/core/tmedia"
	"github.com/iyear/tdl/core/util/tutil"
	"github.com/iyear/tdl/pkg/prog"
	"github.com/iyear/tdl/pkg/texpr"
)

//go:generate go-enum --names --values --flag --nocase

type ExportOptions struct {
	Type        ExportType
	Chat        string
	Thread      int // topic id in forum, message id in group
	Input       []int
	Output      string
	Filter      string
	OnlyMedia   bool
	WithContent bool
	Raw         bool
	All         bool
	URLs        []string // telegram message links to export
}

type Message struct {
	ID   int         `json:"id"`
	Type string      `json:"type"`
	File string      `json:"file"`
	Date int         `json:"date,omitempty"`
	Text string      `json:"text,omitempty"`
	Raw  *tg.Message `json:"raw,omitempty"`
}

// ExportType
// ENUM(time, id, last)
type ExportType int

func Export(ctx context.Context, c *telegram.Client, kvd storage.Storage, opts ExportOptions) (rerr error) {
	// only output available fields
	if opts.Filter == "-" {
		fg := texpr.NewFieldsGetter(nil)

		fields, err := fg.Walk(&texpr.EnvMessage{})
		if err != nil {
			return fmt.Errorf("failed to walk fields: %w", err)
		}

		fmt.Print(fg.Sprint(fields, true))
		return nil
	}

	filter, err := expr.Compile(opts.Filter, expr.AsBool())
	if err != nil {
		return fmt.Errorf("failed to compile filter: %w", err)
	}

	manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(c.API())

	// Determine peer for JSON id field
	var peer peers.Peer
	if opts.Chat != "" {
		peer, err = tutil.GetInputPeer(ctx, manager, opts.Chat)
		if err != nil {
			return fmt.Errorf("failed to get peer: %w", err)
		}
	} else if len(opts.URLs) > 0 {
		// URL-only mode: find first valid URL's peer
		for _, u := range opts.URLs {
			peer, _, err = tutil.ParseMessageLink(ctx, manager, u)
			if err == nil {
				break
			}
			color.Yellow("Skipping invalid URL: %s (%v)", u, err)
		}
		if peer == nil {
			return fmt.Errorf("no valid URLs provided")
		}
	} else {
		// defaults to me(saved messages)
		peer, err = manager.Self(ctx)
		if err != nil {
			return fmt.Errorf("failed to get peer: %w", err)
		}
	}

	color.Yellow("WARN: Export only generates minimal JSON for tdl download, not for backup.")
	color.Cyan("Occasional suspensions are due to Telegram rate limitations, please wait a moment.")
	fmt.Println()

	pw := prog.New(progress.FormatNumber)
	pw.SetUpdateFrequency(200 * time.Millisecond)
	pw.Style().Visibility.TrackerOverall = false
	pw.Style().Visibility.ETA = false
	pw.Style().Visibility.Percentage = false

	go pw.Render()

	f, err := os.Create(opts.Output)
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(f))

	enc := jx.NewStreamingEncoder(f, 512)
	defer multierr.AppendInvoke(&rerr, multierr.Close(enc))

	// process thread is reply type and peer is broadcast channel,
	// so we need to set discussion group id instead of broadcast id
	id := peer.ID()
	if p, ok := peer.(peers.Channel); opts.Thread != 0 && ok && p.IsBroadcast() {
		bc, _ := p.ToBroadcast()
		raw, err := bc.FullRaw(ctx)
		if err != nil {
			return fmt.Errorf("failed to get broadcast full raw: %w", err)
		}

		if id, ok = raw.GetLinkedChatID(); !ok {
			return fmt.Errorf("no linked group")
		}
	}

	enc.ObjStart()
	defer enc.ObjEnd()
	enc.Field("id", func(e *jx.Encoder) { e.Int64(id) })

	enc.FieldStart("messages")
	enc.ArrStart()
	defer enc.ArrEnd()

	count := int64(0)

	// Export from chat if -c is specified or no URLs provided (for saved messages)
	if opts.Chat != "" || len(opts.URLs) == 0 {
		color.Blue("Type: %s | Input: %v", opts.Type, opts.Input)

		tracker := prog.AppendTracker(pw, progress.FormatNumber, fmt.Sprintf("%s-%d", peer.VisibleName(), peer.ID()), 0)

		var q messages.Query
		switch {
		case opts.Thread != 0: // topic messages, reply messages
			q = query.NewQuery(c.API()).Messages().GetReplies(peer.InputPeer()).MsgID(opts.Thread)
		default: // history
			q = query.NewQuery(c.API()).Messages().GetHistory(peer.InputPeer())
		}
		iter := messages.NewIterator(q, 100)

		switch opts.Type {
		case ExportTypeTime:
			iter = iter.OffsetDate(opts.Input[1] + 1)
		case ExportTypeId:
			iter = iter.OffsetID(opts.Input[1] + 1) // #89: retain the last msg id
		case ExportTypeLast:
		}

	loop:
		for iter.Next(ctx) {
			msg := iter.Value()
			switch opts.Type {
			case ExportTypeTime:
				if msg.Msg.GetDate() < opts.Input[0] {
					break loop
				}
			case ExportTypeId:
				if msg.Msg.GetID() < opts.Input[0] {
					break loop
				}
			case ExportTypeLast:
				if count >= int64(opts.Input[0]) {
					break loop
				}
			}

			m, ok := msg.Msg.(*tg.Message)
			if !ok {
				continue
			}
			// only get media messages
			media, ok := tmedia.GetMedia(m)
			if !ok && !opts.All {
				continue
			}

			b, err := texpr.Run(filter, texpr.ConvertEnvMessage(m))
			if err != nil {
				return fmt.Errorf("failed to run filter: %w", err)
			}
			if !b.(bool) { // filtered
				continue
			}

			fileName := ""
			if media != nil { // #207
				fileName = media.Name
			}
			t := &Message{
				ID:   m.ID,
				Type: "message",
				File: fileName,
			}
			if opts.WithContent {
				t.Date = m.Date
				t.Text = m.Message
			}
			if opts.Raw {
				t.Raw = m
			}

			mb, err := json.Marshal(t)
			if err != nil {
				return fmt.Errorf("failed to marshal message: %w", err)
			}
			enc.Raw(mb)

			count++
			tracker.SetValue(count)
		}

		if err = iter.Err(); err != nil {
			return err
		}

		tracker.MarkAsDone()
	}

	// Export from URLs if -u is specified
	if len(opts.URLs) > 0 {
		urlCount, err := exportURLMessages(ctx, c.API(), manager, pw, opts, filter, enc)
		if err != nil {
			return err
		}
		count += urlCount
	}

	prog.Wait(ctx, pw)
	return nil
}

func exportURLMessages(ctx context.Context, api *tg.Client, manager *peers.Manager,
	pw progress.Writer, opts ExportOptions, filter *vm.Program, enc *jx.Encoder) (int64, error) {

	color.Blue("URLs: %d message(s)", len(opts.URLs))

	tracker := prog.AppendTracker(pw, progress.FormatNumber, "URLs", int64(len(opts.URLs)))

	count := int64(0)

	for _, u := range opts.URLs {
		peer, msgID, err := tutil.ParseMessageLink(ctx, manager, u)
		if err != nil {
			color.Yellow("Skipping invalid URL: %s (%v)", u, err)
			tracker.Increment(1)
			continue
		}

		msg, err := tutil.GetSingleMessage(ctx, api, peer.InputPeer(), msgID)
		if err != nil {
			if errors.Is(err, tutil.ErrMessageDeleted) {
				color.Yellow("Skipping deleted message: %s", u)
				tracker.Increment(1)
				continue
			}
			return count, fmt.Errorf("failed to get message from %s: %w", u, err)
		}

		// only get media messages
		media, ok := tmedia.GetMedia(msg)
		if !ok && !opts.All {
			tracker.Increment(1)
			continue
		}

		b, err := texpr.Run(filter, texpr.ConvertEnvMessage(msg))
		if err != nil {
			return count, fmt.Errorf("failed to run filter: %w", err)
		}
		if !b.(bool) { // filtered
			tracker.Increment(1)
			continue
		}

		fileName := ""
		if media != nil {
			fileName = media.Name
		}
		t := &Message{
			ID:   msg.ID,
			Type: "message",
			File: fileName,
		}
		if opts.WithContent {
			t.Date = msg.Date
			t.Text = msg.Message
		}
		if opts.Raw {
			t.Raw = msg
		}

		mb, err := json.Marshal(t)
		if err != nil {
			return count, fmt.Errorf("failed to marshal message: %w", err)
		}
		enc.Raw(mb)

		count++
		tracker.Increment(1)
	}

	tracker.MarkAsDone()
	return count, nil
}
