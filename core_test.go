package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/iyear/tdl/core/tmedia"
)

func TestSelectionAndStore(t *testing.T) {
	if err := run(context.Background(), nil, []string{"sync", "--chat", "group", "--fetch-batch-size", "0"}); err == nil || !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("invalid native batch size: %v", err)
	}
	normalized := normalizeListFlags([]string{"--id", "1", "2", "--filter", "true"})
	if len(normalized) != 4 || normalized[1] != "1,2" {
		t.Fatalf("normalized args: %v", normalized)
	}
	invalid := selection{IDs: intList{1}, FromID: 2}
	if invalid.validate() == nil {
		t.Fatal("accepted conflicting selectors")
	}
	combined := selection{IDs: intList{1}, Topic: 2}
	if err := combined.validate(); err != nil || !combined.Explicit {
		t.Fatalf("exact IDs with a thread selector: explicit=%v err=%v", combined.Explicit, err)
	}
	valid := selection{Type: "id", Input: intList{2, 4}, Topic: 1}
	if err := valid.validate(); err != nil || !valid.Explicit || max(valid.Topic, valid.Reply) != 1 {
		t.Fatalf("valid selector: explicit=%v thread=%d err=%v", valid.Explicit, max(valid.Topic, valid.Reply), err)
	}

	dataPath := filepath.Join(t.TempDir(), "data.sqlite")
	db, err := openStore(dataPath, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.db.Close() }()
	identity := peerResult{Selector: "channel", ID: -1000000000009, Type: "channel", Title: "Channel", AccessHash: 99, Flags: channelMegagroup | channelPublic}
	if _, err := db.prepare(identity, false); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	cursor := 7
	message := archiveMessage{ID: 7, Type: "message", Date: now, Content: "old", JSON: `{"id":7}`, User: archiveUser{ID: 3, Username: "user"}}
	if err := db.save([]archiveMessage{message}, "chat", &cursor); err != nil {
		t.Fatal(err)
	}
	message.Content, message.JSON = "new", `{"id":7,"raw":true}`
	if err := db.save([]archiveMessage{message}, "chat", nil); err != nil {
		t.Fatal(err)
	}
	var content, raw string
	if err := db.db.QueryRow("SELECT content, json_dump FROM messages WHERE id=7").Scan(&content, &raw); err != nil {
		t.Fatal(err)
	}
	if content != "new" || raw != message.JSON {
		t.Fatalf("upsert did not overwrite: %q %q", content, raw)
	}
	if got, err := db.cursor("chat", true); err != nil || got != 7 {
		t.Fatalf("cursor=%d err=%v", got, err)
	}
	if got, err := db.cursor("new-topic", false); err != nil || got != 0 {
		t.Fatalf("scoped cursor=%d err=%v", got, err)
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relativeDataPath, err := filepath.Rel(workingDirectory, dataPath)
	if err != nil {
		t.Fatal(err)
	}
	dry, err := openStore(relativeDataPath, true, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dry.db.Close() }()
	if _, err := dry.prepare(identity, false); err != nil {
		t.Fatal(err)
	}
	dryCursor := 9
	message.Content = "dry run"
	if err := dry.save([]archiveMessage{message}, "chat", &dryCursor); err != nil {
		t.Fatal(err)
	}
	if got, err := dry.cursor("chat", true); err != nil || got != 7 {
		t.Fatalf("dry-run cursor=%d err=%v", got, err)
	}
	if err := db.db.QueryRow("SELECT content FROM messages WHERE id=7").Scan(&content); err != nil || content != "new" {
		t.Fatalf("dry run changed content=%q err=%v", content, err)
	}

	legacyPath := filepath.Join(t.TempDir(), "legacy.sqlite")
	legacy, err := sql.Open("sqlite", legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.Exec("CREATE TABLE messages(id INTEGER PRIMARY KEY, type TEXT NOT NULL, date TIMESTAMP NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := openStore(legacyPath, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = migrated.db.Close() }()
	migrated.dryRun = true
	if pending, err := migrated.prepare(identity, false); err != nil || !pending {
		t.Fatalf("legacy dry-run pending=%v err=%v", pending, err)
	}
	migrated.dryRun = false
	if _, err := migrated.prepare(identity, false); err == nil || !strings.Contains(err.Error(), "bootstrap-peer") {
		t.Fatalf("legacy database was bound implicitly: %v", err)
	}
	if _, err := migrated.prepare(identity, true); err != nil {
		t.Fatal(err)
	}
	if _, err := migrated.db.Exec("INSERT INTO messages(id,type,date,json_dump) VALUES(1,'message','2026-07-29','{}')"); err != nil {
		t.Fatalf("json_dump migration failed: %v", err)
	}

	optional, err := openStore(filepath.Join(t.TempDir(), "optional.sqlite"), false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = optional.db.Close() }()
	if _, err := optional.prepare(identity, false); err != nil {
		t.Fatal(err)
	}
	if err := optional.save([]archiveMessage{message}, "chat", nil); err != nil {
		t.Fatal(err)
	}
	var emptyRaw sql.NullString
	if err := optional.db.QueryRow("SELECT json_dump FROM messages").Scan(&emptyRaw); err != nil || emptyRaw.Valid {
		t.Fatalf("optional json_dump=%q valid=%v err=%v", emptyRaw.String, emptyRaw.Valid, err)
	}

	entities := peer.NewEntities(nil, nil, map[int64]*tg.Channel{9: {ID: 9, Title: "Private group"}})
	if !dialogMatches("Private group", &tg.PeerChannel{ChannelID: 9}, entities) || !dialogMatches("-1009", &tg.PeerChannel{ChannelID: 9}, entities) {
		t.Fatal("private channel did not match by title and Telegram ID")
	}
	archived, _, err := baseMessage(&tg.Message{ID: 1, Date: 1, PeerID: &tg.PeerChannel{ChannelID: 9}}, entities, false)
	if err != nil || archived.User.ID != 9 || archived.User.Username != "Private group" {
		t.Fatalf("channel sender fallback: user=%+v err=%v", archived.User, err)
	}
	blockedMediaDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blockedMediaDir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	photo := &tg.MessageMediaPhoto{Photo: &tg.Photo{ID: 1, DCID: 1, Sizes: []tg.PhotoSizeClass{&tg.PhotoSize{Type: "x", Size: 1}}}}
	mediaMessage := &tg.Message{ID: 1, Date: 1, PeerID: &tg.PeerChannel{ChannelID: 9}}
	mediaMessage.SetMedia(photo)
	_, _, err = (&mediaMapper{ctx: context.Background(), cfg: config{DownloadMedia: true, MediaDir: blockedMediaDir}}).message(messages.Elem{
		Msg: mediaMessage, Entities: entities,
	}, true, false)
	if err == nil {
		t.Fatal("explicit media replacement ignored a download error")
	}
	calls := 0
	query := messages.QueryFunc(func(_ context.Context, request messages.Request) (tg.MessagesMessagesClass, error) {
		calls++
		if request.Limit != 2 || request.AddOffset != -2 {
			t.Fatalf("unsafe request: %+v", request)
		}
		return &tg.MessagesMessages{Messages: []tg.MessageClass{
			&tg.Message{ID: 3}, &tg.Message{ID: 2}, &tg.Message{ID: 1},
		}}, nil
	})
	collected, seen, err := (&synchronizer{}).collectForward(context.Background(), query, &tg.InputPeerChat{ChatID: 1}, 1, 0, 2, nil)
	if err != nil || calls != 1 || seen != 2 || len(collected) != 2 || collected[0].Msg.GetID() != 1 || collected[1].Msg.GetID() != 2 {
		t.Fatalf("bounded forward fetch: ids=%v seen=%d calls=%d err=%v", collected, seen, calls, err)
	}
	threadCalls := 0
	threadQuery := messages.QueryFunc(func(_ context.Context, request messages.Request) (tg.MessagesMessagesClass, error) {
		threadCalls++
		if request.Limit != 1 || request.AddOffset != -1 {
			t.Fatalf("unsafe thread ID request: %+v", request)
		}
		return &tg.MessagesMessages{Messages: []tg.MessageClass{&tg.Message{ID: request.OffsetID}}}, nil
	})
	collected, err = (&synchronizer{}).collectThreadIDs(context.Background(), threadQuery, &tg.InputPeerChannel{ChannelID: 1}, []int{7, 3}, nil)
	if err != nil || threadCalls != 2 || len(collected) != 2 || collected[0].Msg.GetID() != 3 || collected[1].Msg.GetID() != 7 {
		t.Fatalf("thread exact IDs: ids=%v calls=%d err=%v", collected, threadCalls, err)
	}
	rangeWrapped := false
	rawInvoker := telegram.InvokeFunc(func(_ context.Context, input bin.Encoder, _ bin.Decoder) error {
		request, ok := input.(*tg.InvokeWithMessagesRangeRequest)
		rangeWrapped = ok && request.Range.MinID == 10 && request.Range.MaxID == 20
		return nil
	})
	if err := (messageRangeInvoker{raw: rawInvoker, messageRange: tg.MessageRange{MinID: 10, MaxID: 20}}).Invoke(
		context.Background(), &tg.MessagesGetHistoryRequest{}, &tg.MessagesMessages{},
	); err != nil || !rangeWrapped {
		t.Fatalf("takeout message range: wrapped=%v err=%v", rangeWrapped, err)
	}
	unique := uniqueElements([]messages.Elem{{Msg: &tg.Message{ID: 2}}, {Msg: &tg.Message{ID: 1}}, {Msg: &tg.Message{ID: 2}}})
	if len(unique) != 2 || unique[0].Msg.GetID() != 1 || unique[1].Msg.GetID() != 2 {
		t.Fatalf("takeout range deduplication: %v", unique)
	}
	public := &tg.Channel{}
	public.SetUsernames([]tg.Username{{Username: "public", Active: true}})
	if !isPublicChannel(public) || isPublicChannel(&tg.Channel{}) {
		t.Fatal("takeout public channel detection failed")
	}
}

func TestConfigOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("group: original\ndownload_media: true\nfetch_batch_size: 50\nfetch_limit: 20\njson_dump: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options, err := parseOptions([]string{"sync", "--config", path, "--chat", "override", "--download-media=false", "--fetch-limit", "1", "--json-dump=false"})
	if err != nil {
		t.Fatal(err)
	}
	if options.Config.Group != "override" || options.Config.DownloadMedia || options.Config.FetchBatchSize != 50 || options.Config.FetchLimit != 1 || options.Config.JSONDump {
		t.Fatalf("unexpected merged config: %+v", options.Config)
	}
	if _, err := parseOptions([]string{"sync", "--config", path, "--id", "1"}); err == nil || !strings.Contains(err.Error(), "fetch-limit") {
		t.Fatalf("explicit selector accepted configured fetch limit: %v", err)
	}
	if !wantsJSON([]string{"sync", "--bad", "--json"}) || wantsJSON([]string{"sync", "--json=false"}) {
		t.Fatal("JSON mode pre-detection failed")
	}
	var output strings.Builder
	if err := writeResult(&output, runResult{Version: 1}); err != nil || strings.Contains(output.String(), "starting_cursor") {
		t.Fatalf("unavailable cursor was emitted: %s err=%v", output.String(), err)
	}
}

func TestIdentityAndMediaFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.sqlite")
	store, err := openStore(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.db.Close() }()
	identity := peerResult{Selector: "channel", ID: -1000000000009, Type: "channel", Title: "Channel", AccessHash: 99, Flags: channelMegagroup | channelPublic}
	if _, err := store.db.Exec(`CREATE TABLE archive_metadata(id INTEGER PRIMARY KEY, peer_id INTEGER, peer_type TEXT);
INSERT INTO archive_metadata VALUES(1, -1000000000009, 'channel');
CREATE TABLE archive_peer_cache(id INTEGER PRIMARY KEY, peer_selector TEXT, peer_title TEXT, peer_access_hash INTEGER);
INSERT INTO archive_peer_cache VALUES(1, 'channel', 'Channel', 99);`); err != nil {
		t.Fatal(err)
	}
	if cached, err := store.pinnedPeer(); err != nil || cached != nil {
		t.Fatalf("old cache was not treated as a miss: %+v err=%v", cached, err)
	}
	if err := store.ensurePeerFlags(); err != nil {
		t.Fatal(err)
	}
	if cached, err := store.pinnedPeer(); err != nil || cached != nil {
		t.Fatalf("interrupted cache migration was treated as complete: %+v err=%v", cached, err)
	}
	if _, err := store.prepare(identity, false); err != nil {
		t.Fatal(err)
	}
	cached, err := store.pinnedPeer()
	if err != nil || cached == nil || cached.ID != identity.ID || cached.AccessHash != identity.AccessHash {
		t.Fatalf("cached peer=%+v err=%v", cached, err)
	}
	p, err := cachedPeer(peers.Options{}.Build(nil), *cached)
	channel, channelOK := p.(peers.Channel)
	if err != nil || !channelOK || channel.InputPeer().(*tg.InputPeerChannel).AccessHash != identity.AccessHash || !channel.IsSupergroup() || !isPublicChannel(channel.Raw()) {
		t.Fatalf("reconstructed peer=%+v err=%v", p, err)
	}
	if _, err := store.prepare(peerResult{ID: -1000000000010, Type: "channel"}, false); err == nil || !strings.Contains(err.Error(), "pinned") {
		t.Fatalf("peer mismatch accepted: %v", err)
	}
	now := time.Now().UTC()
	cursor := 1
	message := archiveMessage{ID: 1, Type: "message", Date: now, User: archiveUser{ID: 1}, Media: &archiveMedia{ID: 1, URL: "1.jpg"}, MediaAction: mediaReplace}
	if err := store.save([]archiveMessage{message}, "9", &cursor); err != nil {
		t.Fatal(err)
	}
	cursor = 2
	message.Media, message.MediaAction = nil, mediaKeep
	message.MediaFailure = &mediaFailure{Attempts: 3, Error: "download failed"}
	if err := store.save([]archiveMessage{message}, "9", &cursor); err != nil {
		t.Fatal(err)
	}
	var mediaID, attempts int
	if err := store.db.QueryRow("SELECT media_id FROM messages WHERE id = 1").Scan(&mediaID); err != nil || mediaID != 1 {
		t.Fatalf("media link was not preserved: id=%d err=%v", mediaID, err)
	}
	if err := store.db.QueryRow("SELECT attempts FROM media_failures WHERE message_id = 1").Scan(&attempts); err != nil || attempts != 3 {
		t.Fatalf("failure was not persisted: attempts=%d err=%v", attempts, err)
	}
	if got, err := store.cursor("9", false); err != nil || got != 2 {
		t.Fatalf("cursor=%d err=%v", got, err)
	}
	message.Media = &archiveMedia{ID: 1, URL: "1.jpg"}
	message.MediaAction, message.MediaFailure, message.ClearMediaFailure = mediaReplace, nil, true
	if err := store.save([]archiveMessage{message}, "", nil); err != nil {
		t.Fatal(err)
	}
	if ids, err := store.pendingMediaFailures(); err != nil || len(ids) != 0 {
		t.Fatalf("failure was not cleared: ids=%v err=%v", ids, err)
	}
}

func TestPeerResolutionAndFloodWait(t *testing.T) {
	archiveStore, err := openStore(filepath.Join(t.TempDir(), "data.sqlite"), false, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = archiveStore.db.Close() }()
	calls := 0
	api := tg.NewClient(telegram.InvokeFunc(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		request, ok := input.(*tg.ContactsResolveUsernameRequest)
		if !ok || request.Username != "Ktbalbadr" {
			return fmt.Errorf("unexpected request %T", input)
		}
		calls++
		*output.(*tg.ContactsResolvedPeer) = tg.ContactsResolvedPeer{
			Peer:  &tg.PeerChannel{ChannelID: 9},
			Chats: []tg.ChatClass{&tg.Channel{ID: 9, AccessHash: 99, Title: "Channel"}},
		}
		return nil
	}))
	syncer := synchronizer{cfg: config{Group: "Ktbalbadr"}, store: archiveStore}
	p, _, err := syncer.resolvePeer(context.Background(), peers.Options{}.Build(api), api)
	if err != nil || calls != 1 || p.ID() != 9 {
		t.Fatalf("direct username resolution: peer=%+v calls=%d err=%v", p, calls, err)
	}

	flood := tgerr.New(420, tgerr.ErrFloodWait)
	flood.Argument = 60
	var limited tg.Invoker = telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { return flood })
	middlewares := mediaMiddlewares(context.Background(), 0)
	for i := len(middlewares) - 1; i >= 0; i-- {
		limited = middlewares[i].Handle(limited)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = limited.Invoke(ctx, &tg.ContactsResolveUsernameRequest{}, &tg.ContactsResolvedPeer{})
	rpcError, businessError := tgerr.As(err)
	if _, hidden := tgerr.AsFloodWait(err); hidden || !businessError || rpcError.Type != floodWaitLimitExceeded || rpcError.Argument != 60 {
		t.Fatalf("long flood wait was not surfaced: %v", err)
	}
}

func TestMediaRetries(t *testing.T) {
	entities := peer.NewEntities(nil, nil, map[int64]*tg.Channel{9: {ID: 9, Title: "group"}})
	photo := &tg.MessageMediaPhoto{Photo: &tg.Photo{ID: 1, DCID: 1, Sizes: []tg.PhotoSizeClass{&tg.PhotoSize{Type: "x", Size: 1}}}}
	message := &tg.Message{ID: 1, Date: 1, PeerID: &tg.PeerChannel{ChannelID: 9}}
	message.SetMedia(photo)
	oldDelay := mediaRetryDelay
	mediaRetryDelay = 0
	defer func() { mediaRetryDelay = oldDelay }()
	calls := 0
	mapper := mediaMapper{ctx: context.Background(), cfg: config{DownloadMedia: true, MediaDir: t.TempDir()}, downloadFn: func(*tmedia.Media, string) error {
		calls++
		return errors.New("network")
	}}
	archived, event, err := mapper.message(messages.Elem{Msg: message, Entities: entities}, false, false)
	if err != nil || event != mediaFailed || calls != 3 || archived.MediaFailure == nil || !strings.Contains(archived.MediaFailure.Error, mapper.cfg.MediaDir) {
		t.Fatalf("retry result: event=%v calls=%d failure=%v err=%v", event, calls, archived.MediaFailure, err)
	}
	if !fatalMediaError(context.Canceled) || !fatalMediaError(context.DeadlineExceeded) {
		t.Fatal("context termination was treated as recoverable")
	}
	if !fatalMediaError(&os.PathError{Op: "open", Path: "media.tmp", Err: errors.New("local")}) {
		t.Fatal("local path failure was treated as recoverable")
	}
}
