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
	"github.com/iyear/tdl/extension"
	"github.com/iyear/tdl/pkg/texpr"
)

type synchronizer struct {
	ext                *extension.Extension
	cfg                config
	store              *store
	bootstrapPeer      bool
	dryRun             bool
	reconcile          bool
	peer               peerResult
	dialogTopMessageID *int
	api                *tg.Client
	takeout            tg.Invoker
	ranges             []tg.MessageRange
	takeoutID          int64
	historyFetched     int
	caughtUp           bool
	migrationTo        int64
}

const telegramBatchSize = 100

const (
	channelMegagroup = 1
	channelPublic    = 1 << 2
	peerSelf         = 1 << 3 // baseline cache bits 1, 2, and 4 belong to channels
)

func (s *synchronizer) run(ctx context.Context, sel selection) (syncResult, error) {
	self, err := s.ext.Client().Self(ctx)
	if err != nil {
		return syncResult{}, fmt.Errorf("resolve Telegram account: %w", err)
	}
	api := s.ext.Client().API()
	manager := peers.Options{}.Build(api)
	p, dialogTop, err := s.resolvePeer(ctx, manager, api)
	if err != nil {
		return syncResult{}, fmt.Errorf("resolve group: %w", err)
	}
	s.peer = peerResult{Selector: s.cfg.Group, Title: p.VisibleName(), ID: int64(p.TDLibPeerID()), AccessHash: peerAccessHash(p.InputPeer()), Flags: peerFlags(p)}
	s.dialogTopMessageID = dialogTop
	var migratedTo int64
	switch p := p.(type) {
	case peers.User:
		s.peer.Type = "user"
	case peers.Chat:
		s.peer.Type = "chat"
		if migrated, ok := p.Raw().GetMigratedTo(); ok {
			if target, ok := migrated.(interface{ GetChannelID() int64 }); ok {
				migratedTo = encodedChannelID(target.GetChannelID())
			}
		}
	case peers.Channel:
		s.peer.Type = "channel"
	default:
		return syncResult{}, fmt.Errorf("unsupported peer %T", p)
	}
	pendingBootstrap, err := s.store.prepare(s.peer, self.ID, s.bootstrapPeer, migratedTo)
	if err != nil {
		return syncResult{}, fmt.Errorf("prepare database: %w", err)
	}
	s.migrationTo = migratedTo
	warnings := []string{}
	if pendingBootstrap && s.dryRun {
		warnings = append(warnings, "database peer binding is pending")
	}
	var result syncResult
	if !s.cfg.UseTakeout {
		s.api = api
		result, err = s.sync(ctx, sel, p)
	} else {
		result, err = s.runTakeout(ctx, sel, p)
	}
	if err == nil {
		err = s.checkMigrationBoundary()
	}
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
	if strings.HasPrefix(s.cfg.Group, "@") {
		if err := deeplink.ValidateDomain(target); err != nil {
			return nil, nil, err
		}
		p, err := manager.ResolveDomain(ctx, target)
		return p, nil, err
	}
	var directErr error
	if id, err := strconv.ParseInt(s.cfg.Group, 10, 64); err == nil {
		p, err := manager.ResolveTDLibID(ctx, constant.TDLibPeerID(id))
		if err == nil {
			return p, nil, nil
		}
		directErr = err
	} else if deeplink.ValidateDomain(target) == nil {
		p, err := manager.ResolveDomain(ctx, target)
		if err == nil {
			return p, nil, nil
		}
		if !tg.IsUsernameNotOccupied(err) && !tg.IsUsernameInvalid(err) {
			return nil, nil, err
		}
		directErr = err
	}
	dialogPeer, dialogTop, err := s.loadDialogs(ctx, manager, api)
	if err != nil {
		return nil, nil, fmt.Errorf("load dialogs: %w", err)
	}
	if dialogPeer != nil {
		p, err := manager.FromInputPeer(ctx, dialogPeer)
		if err != nil {
			return nil, nil, err
		}
		return p, dialogTop, nil
	}
	if directErr != nil {
		return nil, nil, directErr
	}
	return nil, nil, fmt.Errorf("chat %q was not found; use @username or a TDLib peer ID", s.cfg.Group)
}

func cachedPeer(manager *peers.Manager, cached storedPeer) (peers.Peer, error) {
	id := constant.TDLibPeerID(cached.ID).ToPlain()
	if id == 0 {
		return nil, fmt.Errorf("invalid cached peer ID %d", cached.ID)
	}
	switch cached.Type {
	case "user":
		return manager.User(&tg.User{ID: id, AccessHash: cached.AccessHash, FirstName: cached.Title, Self: cached.Flags&peerSelf != 0}), nil
	case "chat":
		return manager.Chat(&tg.Chat{ID: id, Title: cached.Title}), nil
	case "channel":
		channel := &tg.Channel{
			ID: id, AccessHash: cached.AccessHash, Title: cached.Title,
			Megagroup: cached.Flags&channelMegagroup != 0,
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
	switch p := p.(type) {
	case peers.User:
		if p.Self() {
			return peerSelf
		}
	case peers.Channel:
		flags := 0
		if p.IsSupergroup() {
			flags |= channelMegagroup
		}
		if isPublicChannel(p.Raw()) {
			flags |= channelPublic
		}
		return flags
	}
	return 0
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
	peerScope := baseScope(s.peer)
	reconcileScope := peerScope + ":reconcile"
	var reconcileUntil int
	var retryUntil int
	var err error
	if !s.dryRun {
		if s.reconcile {
			reconcileUntil, err = s.store.reconcileLimit(reconcileScope)
		}
		if err != nil {
			return result, err
		}
		if s.cfg.DownloadMedia {
			retryUntil, err = s.store.cursor(mediaFailureScope)
			if err != nil {
				return result, err
			}
		}
	}
	scope := peerScope
	if sel.Topic > 0 {
		scope += ":topic:" + strconv.Itoa(sel.Topic)
	} else if sel.Reply > 0 {
		scope += ":reply:" + strconv.Itoa(sel.Reply)
	}
	if sel.Filter != "" {
		scope += ":filter:" + sel.Filter
	}
	cursor, err := s.store.cursor(scope)
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
		messages, err := s.mapBatch(&mapper, elems, &result)
		if err != nil {
			return result, err
		}
		var next *int
		if !sel.Explicit && seen > cursor {
			next = &seen
		}
		if len(messages) == 0 {
			if err := s.store.save(nil, nil, scope, next); err != nil {
				return result, err
			}
		} else {
			for start := 0; start < len(messages); start += s.cfg.FetchBatchSize {
				end := min(start+s.cfg.FetchBatchSize, len(messages))
				var batchCursor *int
				if end == len(messages) {
					batchCursor = next
				}
				if err := s.store.save(messages[start:end], nil, scope, batchCursor); err != nil {
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
		exhausted := incremental && (len(elems) < limit || seen <= cursor)
		if !incremental || exhausted || s.cfg.FetchLimit > 0 && total >= s.cfg.FetchLimit {
			s.caughtUp = exhausted && !sel.Explicit && sel.Type == "" && sel.Topic == 0 && sel.Reply == 0 && sel.Filter == ""
			return s.complete(ctx, p, &mapper, result, reconcileUntil, retryUntil)
		}
		cursor = seen
	}
}

func (s *synchronizer) complete(ctx context.Context, p peers.Peer, mapper *mediaMapper, result syncResult, reconcileUntil, retryUntil int) (syncResult, error) {
	if s.dryRun {
		return result, nil
	}
	api := s.ext.Client().API()
	if err := s.reconcileArchive(ctx, api, p, mapper, &result, reconcileUntil); err != nil {
		return result, err
	}
	if s.cfg.DownloadMedia {
		retryIDs, err := s.store.pendingMediaFailures(retryUntil, s.cfg.FetchBatchSize)
		if err != nil {
			return result, err
		}
		if len(retryIDs) > 0 {
			if err := s.retryMediaFailures(ctx, api, p, mapper, &result, retryIDs); err != nil {
				return result, err
			}
		}
	}
	pending, unavailable, err := s.store.mediaFailureCounts()
	result.Media.Pending, result.Media.Unavailable = pending, unavailable
	if pending > 0 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%d media downloads remain pending", pending))
	}
	if unavailable > 0 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%d media downloads are unavailable from Telegram", unavailable))
	}
	return result, err
}

func (s *synchronizer) checkMigrationBoundary() error {
	if !s.caughtUp || s.peer.Type != "chat" {
		return nil
	}
	target, err := s.migrationTo, error(nil)
	if target == 0 {
		target, err = s.store.migrationTarget()
	}
	if err != nil || target == 0 {
		return err
	}
	return fmt.Errorf("basic group migrated to supergroup %d; continue in a separate archive database", target)
}

func (s *synchronizer) reconcileArchive(ctx context.Context, api *tg.Client, p peers.Peer, mapper *mediaMapper, result *syncResult, until int) error {
	if until == 0 {
		return nil
	}
	scope := baseScope(s.peer) + ":reconcile"
	cursor, err := s.store.cursor(scope)
	if err != nil {
		return err
	}
	for cursor < until {
		ids, err := s.store.messageIDsAfter(cursor, until, s.cfg.FetchBatchSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		cursor = ids[len(ids)-1]
		if err := s.reconcileBatch(ctx, api, p, mapper, result, ids, scope, &cursor); err != nil {
			return err
		}
		if cursor >= until || len(ids) < s.cfg.FetchBatchSize {
			break
		}
		if err := waitContext(ctx, time.Duration(s.cfg.FetchWait)*time.Second); err != nil {
			return err
		}
	}
	return s.store.finishReconcile(scope)
}

func (s *synchronizer) reconcileBatch(ctx context.Context, api *tg.Client, p peers.Peer, mapper *mediaMapper, result *syncResult, ids []int, scope string, cursor *int) error {
	elems, missing, err := s.collectIDs(ctx, api, p.InputPeer(), ids, nil)
	if err != nil {
		return err
	}
	messages, err := s.mapBatch(mapper, elems, result)
	if err != nil {
		return err
	}
	if err := s.store.save(messages, missing, scope, cursor); err != nil {
		return err
	}
	result.Reconciled += len(messages)
	result.Missing += len(missing)
	return nil
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

func (s *synchronizer) retryMediaFailures(ctx context.Context, api *tg.Client, p peers.Peer, mapper *mediaMapper, result *syncResult, ids []int) error {
	elems, missing, err := s.collectIDs(ctx, api, p.InputPeer(), ids, nil)
	if err != nil {
		return err
	}
	messages, err := s.mapBatch(mapper, elems, result)
	if err != nil {
		return err
	}
	if err := s.store.save(messages, missing, "", nil); err != nil {
		return err
	}
	for _, id := range missing {
		result.Warnings = append(result.Warnings, fmt.Sprintf("media retry message %d is unavailable", id))
	}
	return nil
}

func (s *synchronizer) mapBatch(mapper *mediaMapper, elems []qmessages.Elem, result *syncResult) ([]archiveMessage, error) {
	if !s.dryRun {
		ids := make([]int, len(elems))
		for i, elem := range elems {
			ids[i] = elem.Msg.GetID()
		}
		existing, err := s.store.messageMedia(ids)
		if err != nil {
			return nil, err
		}
		mapper.existing = existing
	}
	messages := make([]archiveMessage, 0, len(elems))
	for _, elem := range elems {
		message, event, err := mapper.message(elem)
		if err != nil {
			return nil, fmt.Errorf("peer %d message %d: %w", s.peer.ID, elem.Msg.GetID(), err)
		}
		switch event {
		case mediaDownloaded:
			result.Media.Downloaded++
		case mediaReused:
			result.Media.Reused++
		case mediaSkipped:
			result.Media.Skipped++
		case mediaFailed:
			result.Media.Failed++
			result.Media.FailedIDs = append(result.Media.FailedIDs, message.ID)
			result.Warnings = append(result.Warnings, message.MediaFailure.Error)
			s.ext.Log().Error(message.MediaFailure.Error)
		}
		messages = append(messages, message)
		if message.MigrationTo != 0 {
			s.migrationTo = message.MigrationTo
		}
	}
	return messages, nil
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
			result, missing, err := s.collectThreadIDs(ctx, q, inputPeer, sel.IDs, program)
			if err == nil && len(missing) > 0 {
				err = fmt.Errorf("messages unavailable or outside the selected peer: %v", missing)
			}
			return result, cursor, err
		}
		result, missing, err := s.collectIDs(ctx, api, inputPeer, sel.IDs, program)
		if err == nil && len(missing) > 0 {
			err = fmt.Errorf("messages unavailable or outside the selected peer: %v", missing)
		}
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
		if !s.samePeer(messagePeer(elem.Msg), inputPeer) {
			return nil, seen, fmt.Errorf("message %d belongs to another peer", elem.Msg.GetID())
		}
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
	var matched tg.InputPeerClass
	var top *int
	var matchedID int64
	for iter.Next(ctx) {
		elem := iter.Value()
		if dialogMatches(s.cfg.Group, elem.Dialog.GetPeer(), elem.Entities) {
			id := encodedPeerID(elem.Dialog.GetPeer())
			if matched != nil && id != matchedID {
				return nil, nil, fmt.Errorf("chat selector %q is ambiguous; use @username or a TDLib peer ID", s.cfg.Group)
			}
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
			matched, top, matchedID = elem.Peer, &value, id
		}
	}
	if err := iter.Err(); err != nil {
		return nil, nil, err
	}
	return matched, top, nil
}

func dialogMatches(group string, raw tg.PeerClass, entities peer.Entities) bool {
	if group == strconv.FormatInt(encodedPeerID(raw), 10) {
		return true
	}
	if _, err := strconv.ParseInt(group, 10, 64); err == nil {
		return false
	}
	switch value := raw.(type) {
	case *tg.PeerUser:
		user, ok := entities.User(value.UserID)
		return ok && (group == user.Username || group == user.FirstName || group == strings.TrimSpace(user.FirstName+" "+user.LastName))
	case *tg.PeerChat:
		chat, ok := entities.Chat(value.ChatID)
		return ok && group == chat.Title
	case *tg.PeerChannel:
		channel, ok := entities.Channel(value.ChannelID)
		return ok && (group == channel.Username || group == channel.Title)
	}
	return false
}

func (s *synchronizer) collectIDs(ctx context.Context, api *tg.Client, inputPeer tg.InputPeerClass, ids []int, program *vm.Program) ([]qmessages.Elem, []int, error) {
	result := []qmessages.Elem{}
	found := map[int]bool{}
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
			return nil, nil, err
		}
		elems, err := responseElements(response, inputPeer)
		if err != nil {
			return nil, nil, err
		}
		for _, elem := range elems {
			if !s.samePeer(messagePeer(elem.Msg), inputPeer) {
				continue
			}
			found[elem.Msg.GetID()] = true
			matched, err := matches(program, elem.Msg)
			if err != nil {
				return nil, nil, fmt.Errorf("filter message %d: %w", elem.Msg.GetID(), err)
			}
			if matched {
				result = append(result, elem)
			}
		}
	}
	missing := []int{}
	for _, id := range ids {
		if !found[id] {
			missing = append(missing, id)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Msg.GetID() < result[j].Msg.GetID() })
	return result, missing, nil
}

func (s *synchronizer) collectThreadIDs(ctx context.Context, q qmessages.Query, inputPeer tg.InputPeerClass, ids []int, program *vm.Program) ([]qmessages.Elem, []int, error) {
	result := []qmessages.Elem{}
	missing := []int{}
	for _, id := range ids {
		elems, seen, err := s.collectForward(ctx, q, inputPeer, id, id, 1, program)
		if err != nil {
			return nil, nil, err
		}
		if seen < id {
			missing = append(missing, id)
		}
		result = append(result, elems...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Msg.GetID() < result[j].Msg.GetID() })
	return result, missing, nil
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
			if !s.samePeer(messagePeer(elem.Msg), inputPeer) {
				return nil, seen, fmt.Errorf("message %d belongs to another peer", id)
			}
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

func messagePeer(message tg.NotEmptyMessage) tg.PeerClass {
	switch value := message.(type) {
	case *tg.Message:
		return value.PeerID
	case *tg.MessageService:
		return value.PeerID
	}
	return nil
}

func (s *synchronizer) samePeer(peer tg.PeerClass, input tg.InputPeerClass) bool {
	var target int64
	switch value := input.(type) {
	case *tg.InputPeerSelf:
		target = s.peer.ID
	case *tg.InputPeerUser:
		target = value.UserID
	case *tg.InputPeerChat:
		target = -value.ChatID
	case *tg.InputPeerChannel:
		target = encodedChannelID(value.ChannelID)
	}
	return target != 0 && encodedPeerID(peer) == target
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

func baseMessage(raw tg.NotEmptyMessage, entities peer.Entities) (archiveMessage, tg.MessageMediaClass, error) {
	var id, date int
	var from tg.PeerClass
	var media tg.MessageMediaClass
	var migration int64
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
				value, hasMessage := h.GetReplyToMsgID()
				replyPeer, hasPeer := h.GetReplyToPeerID()
				if hasMessage && (!hasPeer || encodedPeerID(replyPeer) == encodedPeerID(m.PeerID)) {
					replyTo = &value
				}
			}
		}
	case *tg.MessageService:
		id, date, from = m.ID, m.Date, m.PeerID
		if value, ok := m.GetFromID(); ok {
			from = value
		}
		switch action := m.Action.(type) {
		case *tg.MessageActionChatAddUser:
			typ = "user_joined"
		case *tg.MessageActionChatJoinedByLink:
			typ = "user_joined_by_link"
		case *tg.MessageActionChatDeleteUser:
			typ = "user_left"
		case *tg.MessageActionChatMigrateTo:
			typ = "migrated_to"
			migration = encodedChannelID(action.ChannelID)
			content = strconv.FormatInt(migration, 10)
		}
	default:
		return archiveMessage{}, nil, fmt.Errorf("unsupported message %T", raw)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return archiveMessage{}, nil, err
	}
	return archiveMessage{ID: id, Type: typ, Date: time.Unix(int64(date), 0).UTC(), EditDate: editDate, Content: content, ReplyTo: replyTo, JSON: string(encoded), User: sender(from, entities), MigrationTo: migration}, media, nil
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
			return archiveUser{ID: encodedPeerID(p), Username: username, First: u.FirstName, Last: u.LastName, Tags: tags}
		}
		return archiveUser{ID: encodedPeerID(p), Username: strconv.FormatInt(p.UserID, 10)}
	case *tg.PeerChat:
		if c, ok := entities.Chat(p.ChatID); ok {
			return archiveUser{ID: encodedPeerID(p), Username: c.Title, Tags: []string{"group_self"}}
		}
		return archiveUser{ID: encodedPeerID(p), Username: strconv.FormatInt(p.ChatID, 10), Tags: []string{"group_self"}}
	case *tg.PeerChannel:
		if c, ok := entities.Channel(p.ChannelID); ok {
			username := c.Username
			if username == "" {
				username = c.Title
			}
			return archiveUser{ID: encodedPeerID(p), Username: username, Tags: []string{"group_self"}}
		}
		return archiveUser{ID: encodedPeerID(p), Username: strconv.FormatInt(p.ChannelID, 10), Tags: []string{"group_self"}}
	}
	return archiveUser{ID: 0, Username: "0"}
}

func encodedPeerID(peer tg.PeerClass) int64 {
	switch value := peer.(type) {
	case *tg.PeerUser:
		return value.UserID
	case *tg.PeerChat:
		return -value.ChatID
	case *tg.PeerChannel:
		return encodedChannelID(value.ChannelID)
	}
	return 0
}

func encodedChannelID(id int64) int64 { return constant.ZeroTDLibChannelID - id }
