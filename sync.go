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
	"github.com/gotd/td/constant"
	"github.com/gotd/td/telegram/deeplink"
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
	ext                *extension.Extension
	cfg                config
	store              *store
	bootstrapPeer      bool
	dryRun             bool
	peer               peerResult
	dialogTopMessageID *int
	api                *tg.Client
	takeout            tg.Invoker
	ranges             []tg.MessageRange
	takeoutID          int64
	historyFetched     int
}

const telegramBatchSize = 100

const (
	channelMegagroup = 1 << iota
	channelBroadcast
	channelPublic
)

func (s *synchronizer) run(ctx context.Context, sel selection) (syncResult, error) {
	api := s.ext.Client().API()
	manager := peers.Options{}.Build(api)
	p, dialogTop, err := s.resolvePeer(ctx, manager, api)
	if err != nil {
		return syncResult{}, fmt.Errorf("resolve group: %w", err)
	}
	s.peer = peerResult{Selector: s.cfg.Group, Title: p.VisibleName(), ID: int64(p.TDLibPeerID()), AccessHash: peerAccessHash(p.InputPeer()), Flags: peerFlags(p)}
	s.dialogTopMessageID = dialogTop
	switch p.(type) {
	case peers.User:
		s.peer.Type = "user"
	case peers.Chat:
		s.peer.Type = "chat"
	case peers.Channel:
		s.peer.Type = "channel"
	default:
		return syncResult{}, fmt.Errorf("unsupported peer %T", p)
	}
	pendingBootstrap, err := s.store.prepare(s.peer, s.bootstrapPeer)
	if err != nil {
		return syncResult{}, fmt.Errorf("prepare database: %w", err)
	}
	warnings := []string{}
	if pendingBootstrap && s.dryRun {
		warnings = append(warnings, "database peer binding is pending")
	}
	if !s.cfg.UseTakeout {
		s.api = api
		result, err := s.sync(ctx, sel, p)
		result.Warnings = append(warnings, result.Warnings...)
		return result, err
	}
	result, err := s.runTakeout(ctx, sel, p)
	result.Warnings = append(warnings, result.Warnings...)
	return result, err
}

func (s *synchronizer) resolvePeer(ctx context.Context, manager *peers.Manager, api *tg.Client) (peers.Peer, *int, error) {
	cached, err := s.store.pinnedPeer()
	if err != nil {
		return nil, nil, err
	}
	if cached != nil && cached.Selector == s.cfg.Group {
		p, err := cachedPeer(manager, *cached)
		return p, nil, err
	}

	target := strings.TrimPrefix(s.cfg.Group, "@")
	var usernameErr error
	if deeplink.ValidateDomain(target) == nil {
		p, err := manager.ResolveDomain(ctx, target)
		if err == nil {
			return p, nil, nil
		}
		if strings.HasPrefix(s.cfg.Group, "@") || !tg.IsUsernameNotOccupied(err) && !tg.IsUsernameInvalid(err) {
			return nil, nil, err
		}
		usernameErr = err
	}

	dialogPeer, dialogTop, err := s.loadDialogs(ctx, manager, api)
	if err != nil {
		return nil, nil, fmt.Errorf("load dialogs: %w", err)
	}
	if dialogPeer != nil {
		p, err := manager.FromInputPeer(ctx, dialogPeer)
		return p, dialogTop, err
	}
	if usernameErr != nil {
		return nil, nil, usernameErr
	}
	p, err := tutil.GetInputPeer(ctx, manager, s.cfg.Group)
	return p, nil, err
}

func cachedPeer(manager *peers.Manager, cached storedPeer) (peers.Peer, error) {
	id := constant.TDLibPeerID(cached.ID).ToPlain()
	if id == 0 {
		return nil, fmt.Errorf("invalid cached peer ID %d", cached.ID)
	}
	switch cached.Type {
	case "user":
		return manager.User(&tg.User{ID: id, AccessHash: cached.AccessHash, FirstName: cached.Title}), nil
	case "chat":
		return manager.Chat(&tg.Chat{ID: id, Title: cached.Title}), nil
	case "channel":
		channel := &tg.Channel{
			ID: id, AccessHash: cached.AccessHash, Title: cached.Title,
			Megagroup: cached.Flags&channelMegagroup != 0,
			Broadcast: cached.Flags&channelBroadcast != 0,
		}
		if cached.Flags&channelPublic != 0 {
			channel.SetUsername(strings.TrimPrefix(cached.Selector, "@"))
		}
		return manager.Channel(channel), nil
	default:
		return nil, fmt.Errorf("unsupported cached peer type %q", cached.Type)
	}
}

func peerFlags(p peers.Peer) int {
	channel, ok := p.(peers.Channel)
	if !ok {
		return 0
	}
	flags := 0
	if channel.IsSupergroup() {
		flags |= channelMegagroup
	}
	if channel.IsBroadcast() {
		flags |= channelBroadcast
	}
	if isPublicChannel(channel.Raw()) {
		flags |= channelPublic
	}
	return flags
}

func peerAccessHash(input tg.InputPeerClass) int64 {
	switch value := input.(type) {
	case *tg.InputPeerUser:
		return value.AccessHash
	case *tg.InputPeerChannel:
		return value.AccessHash
	default:
		return 0
	}
}

func (s *synchronizer) sync(ctx context.Context, sel selection, p peers.Peer) (syncResult, error) {
	result := syncResult{}
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
	cursor, err := s.store.cursor(scope, scope == baseScope)
	if err != nil {
		return result, err
	}
	startingCursor, endingCursor := cursor, cursor
	result.StartingCursor, result.EndingCursor = &startingCursor, &endingCursor

	var program *vm.Program
	if sel.Filter != "" {
		program, err = expr.Compile(sel.Filter, expr.AsBool())
		if err != nil {
			return result, fmt.Errorf("compile filter: %w", err)
		}
	}

	mapper := mediaMapper{ctx: ctx, ext: s.ext, cfg: s.cfg, peerID: s.peer.ID, takeoutID: s.takeoutID}
	defer mapper.close()
	if s.cfg.DownloadMedia {
		stats, warnings, err := s.retryMediaFailures(ctx, p, &mapper)
		result.Media = stats
		result.Warnings = append(result.Warnings, warnings...)
		if err != nil {
			return result, err
		}
	}
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
			return result, err
		}
		result.Selected += len(elems)
		messages := make([]archiveMessage, 0, len(elems))
		for _, elem := range elems {
			message, event, err := mapper.message(elem, sel.Explicit, false)
			if err != nil {
				return result, fmt.Errorf("peer %d message %d: %w", s.peer.ID, elem.Msg.GetID(), err)
			}
			s.recordMediaEvent(&result.Media, &result.Warnings, message, event)
			messages = append(messages, message)
		}
		var next *int
		if !sel.Explicit && seen > cursor {
			next = &seen
		}
		if len(messages) == 0 {
			if err := s.store.save(nil, scope, next); err != nil {
				return result, err
			}
		} else {
			for start := 0; start < len(messages); start += s.cfg.FetchBatchSize {
				end := min(start+s.cfg.FetchBatchSize, len(messages))
				var batchCursor *int
				if end == len(messages) {
					batchCursor = next
				}
				if err := s.store.save(messages[start:end], scope, batchCursor); err != nil {
					return result, err
				}
				if !s.dryRun {
					result.Saved += end - start
				}
			}
		}
		if next != nil {
			endingCursor = *next
		}
		total += len(messages)
		if !incremental || len(elems) < limit || seen <= cursor || s.cfg.FetchLimit > 0 && total >= s.cfg.FetchLimit {
			pending, err := s.store.pendingMediaFailures()
			result.Media.Pending = len(pending)
			if len(pending) > 0 {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%d media downloads remain pending", len(pending)))
			}
			return result, err
		}
		cursor = seen
	}
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *synchronizer) pacedQuery(q qmessages.Query) qmessages.Query {
	if s.cfg.FetchWait == 0 {
		return q
	}
	return qmessages.QueryFunc(func(ctx context.Context, req qmessages.Request) (tg.MessagesMessagesClass, error) {
		if s.historyFetched >= s.cfg.FetchBatchSize {
			if err := waitContext(ctx, time.Duration(s.cfg.FetchWait)*time.Second); err != nil {
				return nil, err
			}
			s.historyFetched = 0
		}
		result, err := q.Query(ctx, req)
		if err == nil {
			modified, ok := result.AsModified()
			if ok {
				s.historyFetched += len(modified.GetMessages())
			}
		}
		return result, err
	})
}

func (s *synchronizer) retryMediaFailures(ctx context.Context, p peers.Peer, mapper *mediaMapper) (mediaStats, []string, error) {
	stats := mediaStats{}
	ids, err := s.store.pendingMediaFailures()
	if err != nil || len(ids) == 0 {
		return stats, nil, err
	}
	elems, err := s.collectIDs(ctx, s.api, p.InputPeer(), ids, nil)
	if err != nil {
		return stats, nil, err
	}
	found := map[int]bool{}
	messages := make([]archiveMessage, 0, len(elems))
	warnings := []string{}
	for _, elem := range elems {
		found[elem.Msg.GetID()] = true
		message, event, err := mapper.message(elem, true, true)
		if err != nil {
			return stats, warnings, fmt.Errorf("retry peer %d message %d: %w", s.peer.ID, elem.Msg.GetID(), err)
		}
		s.recordMediaEvent(&stats, &warnings, message, event)
		messages = append(messages, message)
	}
	if err := s.store.save(messages, "", nil); err != nil {
		return stats, warnings, err
	}
	missing := []int{}
	for _, id := range ids {
		if !found[id] {
			missing = append(missing, id)
			warnings = append(warnings, fmt.Sprintf("media retry message %d is unavailable", id))
		}
	}
	return stats, warnings, s.store.clearMediaFailures(missing)
}

func (s *synchronizer) recordMediaEvent(stats *mediaStats, warnings *[]string, message archiveMessage, event mediaEvent) {
	recordMediaEvent(stats, warnings, message, event)
	if message.MediaFailure != nil {
		s.ext.Log().Error(message.MediaFailure.Error)
	}
}

func recordMediaEvent(stats *mediaStats, warnings *[]string, message archiveMessage, event mediaEvent) {
	switch event {
	case mediaDownloaded:
		stats.Downloaded++
	case mediaReused:
		stats.Reused++
	case mediaSkipped:
		stats.Skipped++
	case mediaFailed:
		stats.Failed++
		stats.FailedIDs = append(stats.FailedIDs, message.ID)
		*warnings = append(*warnings, message.MediaFailure.Error)
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
	q = s.pacedQuery(q)
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

func (s *synchronizer) loadDialogs(ctx context.Context, manager *peers.Manager, api *tg.Client) (tg.InputPeerClass, *int, error) {
	iter := query.GetDialogs(api).BatchSize(telegramBatchSize).Iter()
	for iter.Next(ctx) {
		elem := iter.Value()
		if dialogMatches(s.cfg.Group, elem.Dialog.GetPeer(), elem.Entities) {
			users := make([]tg.UserClass, 0, len(elem.Entities.Users()))
			chats := make([]tg.ChatClass, 0, len(elem.Entities.Chats())+len(elem.Entities.Channels()))
			for _, value := range elem.Entities.Users() {
				users = append(users, value)
			}
			for _, value := range elem.Entities.Chats() {
				chats = append(chats, value)
			}
			for _, value := range elem.Entities.Channels() {
				chats = append(chats, value)
			}
			if err := manager.Apply(ctx, users, chats); err != nil {
				return nil, nil, err
			}
			value := elem.Dialog.GetTopMessage()
			return elem.Peer, &value, nil
		}
	}
	if err := iter.Err(); err != nil {
		return nil, nil, err
	}
	return nil, nil, nil
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
