package main

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/expr-lang/expr/vm"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/peers"
	qmessages "github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
	coretakeout "github.com/iyear/tdl/core/middlewares/takeout"
)

type messageRangeInvoker struct {
	raw          tg.Invoker
	messageRange tg.MessageRange
}

func (i messageRangeInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	query, ok := input.(bin.Object)
	if !ok {
		return fmt.Errorf("takeout query %T is not a Telegram object", input)
	}
	return i.raw.Invoke(ctx, &tg.InvokeWithMessagesRangeRequest{Range: i.messageRange, Query: query}, output)
}

func (s *synchronizer) collect(ctx context.Context, inputPeer tg.InputPeerClass, sel selection, cursor int, program *vm.Program, limit int) ([]qmessages.Elem, int, error) {
	if len(s.ranges) == 0 || max(sel.Topic, sel.Reply) > 0 {
		return s.collectFrom(ctx, s.api, inputPeer, sel, cursor, program, limit)
	}
	result := []qmessages.Elem{}
	seen := cursor
	for _, messageRange := range s.ranges {
		remaining := limit
		if limit > 0 {
			remaining -= len(result)
			if remaining <= 0 {
				break
			}
		}
		api := tg.NewClient(messageRangeInvoker{raw: s.takeout, messageRange: messageRange})
		elems, next, err := s.collectFrom(ctx, api, inputPeer, sel, cursor, program, remaining)
		if err != nil {
			return nil, seen, err
		}
		result = append(result, elems...)
		seen = max(seen, next)
	}
	result = uniqueElements(result)
	if sel.Type == "last" && len(result) > sel.Input[0] {
		result = result[len(result)-sel.Input[0]:]
	}
	return result, seen, nil
}

func uniqueElements(elems []qmessages.Elem) []qmessages.Elem {
	sort.Slice(elems, func(i, j int) bool { return elems[i].Msg.GetID() < elems[j].Msg.GetID() })
	result := elems[:0]
	for _, elem := range elems {
		if len(result) == 0 || result[len(result)-1].Msg.GetID() != elem.Msg.GetID() {
			result = append(result, elem)
		}
	}
	return result
}

func (s *synchronizer) runTakeout(ctx context.Context, sel selection, p peers.Peer) (int, error) {
	request := &tg.AccountInitTakeoutSessionRequest{}
	switch p := p.(type) {
	case peers.User:
		request.SetMessageUsers(true)
	case peers.Chat:
		request.SetMessageChats(true)
		request.SetMessageMegagroups(true)
	case peers.Channel:
		if p.IsSupergroup() {
			request.SetMessageMegagroups(true)
		} else {
			request.SetMessageChannels(true)
		}
	default:
		return 0, fmt.Errorf("unsupported takeout peer %T", p)
	}
	if s.cfg.DownloadMedia {
		request.SetFiles(true)
		request.SetFileMaxSize(4000 * 1024 * 1024)
	}

	session, err := s.ext.Client().API().AccountInitTakeoutSession(ctx, request)
	if err != nil {
		return 0, fmt.Errorf("init takeout session: %w", err)
	}
	s.takeoutID = session.ID
	s.takeout = coretakeout.Middleware(session.ID).Handle(s.ext.Client())
	s.api = tg.NewClient(s.takeout)

	count := 0
	runErr := func() error {
		var err error
		s.ranges, err = s.api.MessagesGetSplitRanges(ctx)
		if err != nil {
			return fmt.Errorf("get message ranges: %w", err)
		}
		if len(s.ranges) == 0 {
			return fmt.Errorf("telegram returned no takeout message ranges")
		}
		sort.Slice(s.ranges, func(i, j int) bool { return s.ranges[i].MinID < s.ranges[j].MinID })
		if channel, ok := p.(peers.Channel); ok && isPublicChannel(channel.Raw()) {
			s.ranges = s.ranges[len(s.ranges)-1:]
		}
		count, err = s.sync(ctx, sel, p)
		return err
	}()

	finish := &tg.AccountFinishTakeoutSessionRequest{}
	finish.SetSuccess(runErr == nil)
	if _, err := s.api.AccountFinishTakeoutSession(context.WithoutCancel(ctx), finish); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("finish takeout session: %w", err))
	}
	if runErr != nil {
		return count, fmt.Errorf("takeout: %w", runErr)
	}
	return count, nil
}

func isPublicChannel(channel *tg.Channel) bool {
	if _, ok := channel.GetUsername(); ok {
		return true
	}
	for _, username := range channel.Usernames {
		if username.Active {
			return true
		}
	}
	return false
}
