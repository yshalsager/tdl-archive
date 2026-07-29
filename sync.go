package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query"
	qmessages "github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/util/tutil"
	"github.com/iyear/tdl/extension"
	"github.com/iyear/tdl/pkg/texpr"
)

type synchronizer struct {
	ext       *extension.Extension
	cfg       config
	store     *store
	api       *tg.Client
	takeout   tg.Invoker
	ranges    []tg.MessageRange
	takeoutID int64
}

const telegramBatchSize = 100

func (s *synchronizer) run(ctx context.Context, sel selection) (int, error) {
	api := s.ext.Client().API()
	manager := peers.Options{}.Build(api)
	dialogPeer, err := s.loadDialogs(ctx, manager, api)
	if err != nil {
		return 0, fmt.Errorf("load dialogs: %w", err)
	}
	var p peers.Peer
	if dialogPeer != nil {
		p, err = manager.FromInputPeer(ctx, dialogPeer)
	} else {
		p, err = tutil.GetInputPeer(ctx, manager, s.cfg.Group)
	}
	if err != nil {
		return 0, fmt.Errorf("resolve group: %w", err)
	}
	if !s.cfg.UseTakeout {
		s.api = api
		return s.sync(ctx, sel, p)
	}
	return s.runTakeout(ctx, sel, p)
}

func (s *synchronizer) sync(ctx context.Context, sel selection, p peers.Peer) (int, error) {
	var err error
	baseScope := strconv.FormatInt(p.ID(), 10)
	scope := baseScope
	if sel.Topic > 0 {
		scope += ":topic:" + strconv.Itoa(sel.Topic)
	} else if sel.Reply > 0 {
		scope += ":reply:" + strconv.Itoa(sel.Reply)
	}
	if sel.Filter != "" {
		scope += ":filter:" + sel.Filter
	}
	cursor := 0
	if !sel.Explicit {
		cursor, err = s.store.cursor(scope, scope == baseScope)
		if err != nil {
			return 0, err
		}
	}

	var program *vm.Program
	if sel.Filter != "" {
		program, err = expr.Compile(sel.Filter, expr.AsBool())
		if err != nil {
			return 0, fmt.Errorf("compile filter: %w", err)
		}
	}

	mapper := mediaMapper{ctx: ctx, ext: s.ext, cfg: s.cfg, overwrite: sel.Explicit, takeoutID: s.takeoutID}
	defer mapper.close()
	total := 0
	for {
		limit := 0
		incremental := !sel.Explicit && sel.Type == ""
		if incremental {
			limit = s.cfg.FetchBatchSize
			if s.cfg.FetchLimit > 0 {
				limit = min(limit, s.cfg.FetchLimit-total)
			}
		}
		elems, seen, err := s.collect(ctx, p.InputPeer(), sel, cursor, program, limit)
		if err != nil {
			return total, err
		}
		messages := make([]archiveMessage, 0, len(elems))
		for _, elem := range elems {
			message, err := mapper.message(elem)
			if err != nil {
				return total, err
			}
			messages = append(messages, message)
		}
		var next *int
		if !sel.Explicit && seen > cursor {
			next = &seen
		}
		if len(messages) == 0 {
			if err := s.store.save(nil, scope, next); err != nil {
				return total, err
			}
		} else {
			for start := 0; start < len(messages); start += s.cfg.FetchBatchSize {
				end := min(start+s.cfg.FetchBatchSize, len(messages))
				var batchCursor *int
				if end == len(messages) {
					batchCursor = next
				}
				if err := s.store.save(messages[start:end], scope, batchCursor); err != nil {
					return total + start, err
				}
			}
		}
		total += len(messages)
		if !incremental || len(elems) < limit || seen <= cursor || s.cfg.FetchLimit > 0 && total >= s.cfg.FetchLimit {
			return total, nil
		}
		cursor = seen
	}
}

func (s *synchronizer) collectFrom(ctx context.Context, api *tg.Client, inputPeer tg.InputPeerClass, sel selection, cursor int, program *vm.Program, limit int) ([]qmessages.Elem, int, error) {
	thread := max(sel.Topic, sel.Reply)
	var q qmessages.Query
	if thread > 0 {
		q = query.NewQuery(api).Messages().GetReplies(inputPeer).MsgID(thread)
	} else {
		q = query.NewQuery(api).Messages().GetHistory(inputPeer)
	}
	if len(sel.IDs) > 0 {
		if thread > 0 {
			result, err := s.collectThreadIDs(ctx, q, inputPeer, sel.IDs, program)
			return result, cursor, err
		}
		result, err := s.collectIDs(ctx, api, inputPeer, sel.IDs, program)
		return result, cursor, err
	}
	lower, upper := cursor+1, 0
	switch {
	case sel.FromID > 0:
		lower = sel.FromID
	case sel.Type == "id":
		lower, upper = sel.Input[0], sel.Input[1]
	}
	if sel.Type != "time" && sel.Type != "last" {
		return s.collectForward(ctx, q, inputPeer, lower, upper, limit, program)
	}

	iter := qmessages.NewIterator(q, telegramBatchSize)
	if sel.Type == "time" {
		iter.OffsetDate(sel.Input[1] + 1)
	}

	result := []qmessages.Elem{}
	seen := cursor
	for iter.Next(ctx) {
		elem := iter.Value()
		id, date := elem.Msg.GetID(), elem.Msg.GetDate()
		if id > seen {
			seen = id
		}
		if sel.Type == "time" && date < sel.Input[0] {
			break
		}
		matched, err := matches(program, elem.Msg)
		if err != nil {
			return nil, seen, fmt.Errorf("filter message %d: %w", id, err)
		}
		if matched {
			result = append(result, elem)
		}
		if sel.Type == "last" && len(result) >= sel.Input[0] {
			break
		}
	}
	if err := iter.Err(); err != nil {
		return nil, seen, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Msg.GetID() < result[j].Msg.GetID() })
	return result, seen, nil
}

func (s *synchronizer) loadDialogs(ctx context.Context, manager *peers.Manager, api *tg.Client) (tg.InputPeerClass, error) {
	iter := query.GetDialogs(api).BatchSize(telegramBatchSize).Iter()
	users := map[int64]*tg.User{}
	chats := map[int64]*tg.Chat{}
	channels := map[int64]*tg.Channel{}
	var matched tg.InputPeerClass
	for iter.Next(ctx) {
		elem := iter.Value()
		for id, value := range elem.Entities.Users() {
			users[id] = value
		}
		for id, value := range elem.Entities.Chats() {
			chats[id] = value
		}
		for id, value := range elem.Entities.Channels() {
			channels[id] = value
		}
		if dialogMatches(s.cfg.Group, elem.Dialog.GetPeer(), elem.Entities) {
			matched = elem.Peer
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	userClasses := make([]tg.UserClass, 0, len(users))
	chatClasses := make([]tg.ChatClass, 0, len(chats)+len(channels))
	for _, value := range users {
		userClasses = append(userClasses, value)
	}
	for _, value := range chats {
		chatClasses = append(chatClasses, value)
	}
	for _, value := range channels {
		chatClasses = append(chatClasses, value)
	}
	return matched, manager.Apply(ctx, userClasses, chatClasses)
}

func dialogMatches(group string, raw tg.PeerClass, entities peer.Entities) bool {
	target := strings.TrimPrefix(group, "@")
	switch value := raw.(type) {
	case *tg.PeerUser:
		user, ok := entities.User(value.UserID)
		return ok && (target == user.Username || group == user.FirstName || target == strconv.FormatInt(value.UserID, 10))
	case *tg.PeerChat:
		chat, ok := entities.Chat(value.ChatID)
		return ok && (group == chat.Title || target == strconv.FormatInt(value.ChatID, 10) || group == "-"+strconv.FormatInt(value.ChatID, 10))
	case *tg.PeerChannel:
		channel, ok := entities.Channel(value.ChannelID)
		return ok && (target == channel.Username || group == channel.Title || target == strconv.FormatInt(value.ChannelID, 10) || group == "-100"+strconv.FormatInt(value.ChannelID, 10))
	}
	return false
}

func (s *synchronizer) collectIDs(ctx context.Context, api *tg.Client, inputPeer tg.InputPeerClass, ids []int, program *vm.Program) ([]qmessages.Elem, error) {
	result := []qmessages.Elem{}
	for start := 0; start < len(ids); start += telegramBatchSize {
		end := min(start+telegramBatchSize, len(ids))
		input := make([]tg.InputMessageClass, end-start)
		for i, id := range ids[start:end] {
			input[i] = &tg.InputMessageID{ID: id}
		}
		var response tg.MessagesMessagesClass
		var err error
		switch p := inputPeer.(type) {
		case *tg.InputPeerChannel:
			response, err = api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
				Channel: &tg.InputChannel{ChannelID: p.ChannelID, AccessHash: p.AccessHash}, ID: input,
			})
		default:
			response, err = api.MessagesGetMessages(ctx, input)
		}
		if err != nil {
			return nil, err
		}
		elems, err := responseElements(response, inputPeer)
		if err != nil {
			return nil, err
		}
		for _, elem := range elems {
			matched, err := matches(program, elem.Msg)
			if err != nil {
				return nil, fmt.Errorf("filter message %d: %w", elem.Msg.GetID(), err)
			}
			if matched {
				result = append(result, elem)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Msg.GetID() < result[j].Msg.GetID() })
	return result, nil
}

func (s *synchronizer) collectThreadIDs(ctx context.Context, q qmessages.Query, inputPeer tg.InputPeerClass, ids []int, program *vm.Program) ([]qmessages.Elem, error) {
	result := []qmessages.Elem{}
	for _, id := range ids {
		elems, _, err := s.collectForward(ctx, q, inputPeer, id, id, 1, program)
		if err != nil {
			return nil, err
		}
		result = append(result, elems...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Msg.GetID() < result[j].Msg.GetID() })
	return result, nil
}

func (s *synchronizer) collectForward(ctx context.Context, q qmessages.Query, inputPeer tg.InputPeerClass, lower, upper, limit int, program *vm.Program) ([]qmessages.Elem, int, error) {
	result := []qmessages.Elem{}
	seen, offsetID := lower-1, lower
	for {
		batchSize := telegramBatchSize
		if limit > 0 {
			batchSize = min(batchSize, limit-len(result))
		}
		response, err := q.Query(ctx, qmessages.Request{OffsetID: offsetID, AddOffset: -batchSize, Limit: batchSize})
		if err != nil {
			return nil, seen, err
		}
		batch, err := responseElements(response, inputPeer)
		if err != nil {
			return nil, seen, err
		}
		sort.Slice(batch, func(i, j int) bool { return batch[i].Msg.GetID() < batch[j].Msg.GetID() })
		if len(batch) == 0 {
			return result, seen, nil
		}
		highest := offsetID - 1
		for _, elem := range batch {
			id := elem.Msg.GetID()
			if id > highest {
				highest = id
			}
			if id < offsetID {
				continue
			}
			if upper > 0 && id > upper {
				return result, seen, nil
			}
			seen = id
			matched, err := matches(program, elem.Msg)
			if err != nil {
				return nil, seen, fmt.Errorf("filter message %d: %w", id, err)
			}
			if matched {
				result = append(result, elem)
			}
			if limit > 0 && len(result) >= limit {
				return result, seen, nil
			}
		}
		if highest < offsetID {
			return result, seen, nil
		}
		offsetID = highest + 1
	}
}

func responseElements(response tg.MessagesMessagesClass, inputPeer tg.InputPeerClass) ([]qmessages.Elem, error) {
	modified, ok := response.AsModified()
	if !ok {
		return nil, fmt.Errorf("unexpected messages response %T", response)
	}
	entityResult, ok := response.(peer.EntitySearchResult)
	if !ok {
		return nil, fmt.Errorf("messages response has no entities: %T", response)
	}
	entities := peer.EntitiesFromResult(entityResult)
	result := make([]qmessages.Elem, 0, len(modified.GetMessages()))
	for _, raw := range modified.GetMessages() {
		message, ok := raw.AsNotEmpty()
		if ok {
			result = append(result, qmessages.Elem{Msg: message, Peer: inputPeer, Entities: entities})
		}
	}
	return result, nil
}

func matches(program *vm.Program, raw tg.NotEmptyMessage) (bool, error) {
	if program == nil {
		return true, nil
	}
	message, ok := raw.(*tg.Message)
	if !ok {
		return false, nil
	}
	result, err := texpr.Run(program, texpr.ConvertEnvMessage(message))
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

func baseMessage(raw tg.NotEmptyMessage, entities peer.Entities, jsonDump bool) (archiveMessage, tg.MessageMediaClass, error) {
	var id, date int
	var from tg.PeerClass
	var media tg.MessageMediaClass
	typ, content := "message", ""
	var editDate *time.Time
	var replyTo *int
	switch m := raw.(type) {
	case *tg.Message:
		id, date, content, from = m.ID, m.Date, m.Message, m.PeerID
		if value, ok := m.GetFromID(); ok {
			from = value
		}
		media, _ = m.GetMedia()
		if edit, ok := m.GetEditDate(); ok {
			v := time.Unix(int64(edit), 0).UTC()
			editDate = &v
		}
		if reply, ok := m.GetReplyTo(); ok {
			if h, ok := reply.(*tg.MessageReplyHeader); ok {
				if value, ok := h.GetReplyToMsgID(); ok {
					replyTo = &value
				}
			}
		}
	case *tg.MessageService:
		id, date, from = m.ID, m.Date, m.PeerID
		if value, ok := m.GetFromID(); ok {
			from = value
		}
		switch m.Action.(type) {
		case *tg.MessageActionChatAddUser:
			typ = "user_joined"
		case *tg.MessageActionChatJoinedByLink:
			typ = "user_joined_by_link"
		case *tg.MessageActionChatDeleteUser:
			typ = "user_left"
		}
	default:
		return archiveMessage{}, nil, fmt.Errorf("unsupported message %T", raw)
	}
	var rawJSON string
	if jsonDump {
		encoded, err := json.Marshal(raw)
		if err != nil {
			return archiveMessage{}, nil, err
		}
		rawJSON = string(encoded)
	}
	return archiveMessage{ID: id, Type: typ, Date: time.Unix(int64(date), 0).UTC(), EditDate: editDate, Content: content, ReplyTo: replyTo, JSON: rawJSON, User: sender(from, entities)}, media, nil
}

func sender(from tg.PeerClass, entities peer.Entities) archiveUser {
	switch p := from.(type) {
	case *tg.PeerUser:
		if u, ok := entities.User(p.UserID); ok {
			tags := []string{}
			if u.Bot {
				tags = append(tags, "bot")
			}
			if u.Scam {
				tags = append(tags, "scam")
			}
			if u.Fake {
				tags = append(tags, "fake")
			}
			username := u.Username
			if username == "" {
				username = strconv.FormatInt(u.ID, 10)
			}
			return archiveUser{ID: u.ID, Username: username, First: u.FirstName, Last: u.LastName, Tags: tags}
		}
		return archiveUser{ID: p.UserID, Username: strconv.FormatInt(p.UserID, 10)}
	case *tg.PeerChat:
		if c, ok := entities.Chat(p.ChatID); ok {
			return archiveUser{ID: c.ID, Username: c.Title, Tags: []string{"group_self"}}
		}
		return archiveUser{ID: p.ChatID, Username: strconv.FormatInt(p.ChatID, 10), Tags: []string{"group_self"}}
	case *tg.PeerChannel:
		if c, ok := entities.Channel(p.ChannelID); ok {
			username := c.Username
			if username == "" {
				username = c.Title
			}
			return archiveUser{ID: c.ID, Username: username, Tags: []string{"group_self"}}
		}
		return archiveUser{ID: p.ChannelID, Username: strconv.FormatInt(p.ChannelID, 10), Tags: []string{"group_self"}}
	}
	return archiveUser{ID: 0, Username: "0"}
}
