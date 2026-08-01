package main

import (
	"context"
	"database/sql"
	"encoding/json"
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
	db, err := openStore(dataPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.db.Close() }()
	if got := db.db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("max open connections=%d", got)
	}
	identity := peerResult{Selector: "channel", ID: -1000000000009, Type: "channel", Title: "Channel", AccessHash: 99, Flags: channelMegagroup}
	if _, err := db.prepare(identity, 42, false, 0); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	cursor := 7
	message := archiveMessage{ID: 7, Type: "message", Date: now, Content: "old", JSON: `{"id":7}`, User: archiveUser{ID: 3, Username: "user"}}
	if err := db.save([]archiveMessage{message}, nil, "chat", &cursor); err != nil {
		t.Fatal(err)
	}
	message.Content, message.JSON = "new", `{"id":7,"raw":true}`
	if err := db.save([]archiveMessage{message}, nil, "chat", nil); err != nil {
		t.Fatal(err)
	}
	var content, raw string
	if err := db.db.QueryRow("SELECT content, json_dump FROM messages WHERE id=7").Scan(&content, &raw); err != nil {
		t.Fatal(err)
	}
	if content != "new" || raw != message.JSON {
		t.Fatalf("upsert did not overwrite: %q %q", content, raw)
	}
	if err := db.db.QueryRow("SELECT json_dump FROM message_revisions WHERE message_id=7").Scan(&raw); err != nil || raw != `{"id":7}` {
		t.Fatalf("message revision=%q err=%v", raw, err)
	}
	message.JSON = `{"id":7,"raw":true,"views":2}`
	if err := db.save([]archiveMessage{message}, nil, "chat", nil); err != nil {
		t.Fatal(err)
	}
	var revisions int
	if err := db.db.QueryRow("SELECT COUNT(*) FROM message_revisions WHERE message_id=7").Scan(&revisions); err != nil || revisions != 1 {
		t.Fatalf("raw-only refresh created %d revisions: %v", revisions, err)
	}
	message.Media, message.MediaAction = &archiveMedia{ID: 7, Type: "photo", URL: "old.jpg", Title: "photo", Thumb: "old.jpg"}, mediaReplace
	message.JSON = `{"id":7,"photo":"old"}`
	if err := db.save([]archiveMessage{message}, nil, "chat", nil); err != nil {
		t.Fatal(err)
	}
	message.Media.URL, message.Media.Thumb, message.JSON = "new.jpg", "new.jpg", `{"id":7,"photo":"new"}`
	if err := db.save([]archiveMessage{message}, nil, "chat", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRow("SELECT json_dump FROM message_revisions WHERE message_id=7 ORDER BY id DESC LIMIT 1").Scan(&raw); err != nil || raw != `{"id":7,"photo":"old"}` {
		t.Fatalf("media revision=%q err=%v", raw, err)
	}
	if err := db.save(nil, []int{7}, "", nil); err != nil {
		t.Fatal(err)
	}
	var tombstoned int
	if err := db.db.QueryRow("SELECT COUNT(*) FROM message_tombstones WHERE message_id=7").Scan(&tombstoned); err != nil || tombstoned != 1 {
		t.Fatalf("tombstone count=%d err=%v", tombstoned, err)
	}
	if err := db.save([]archiveMessage{message}, nil, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRow("SELECT COUNT(*) FROM message_tombstones WHERE message_id=7").Scan(&tombstoned); err != nil || tombstoned != 0 {
		t.Fatalf("live message retained tombstone: count=%d err=%v", tombstoned, err)
	}
	if got, err := db.cursor("chat"); err != nil || got != 7 {
		t.Fatalf("cursor=%d err=%v", got, err)
	}
	if got, err := db.cursor("new-topic"); err != nil || got != 0 {
		t.Fatalf("scoped cursor=%d err=%v", got, err)
	}
	if got, err := db.cursor(baseScope(identity)); err != nil || got != 0 {
		t.Fatalf("base cursor was inferred from archived rows: %d err=%v", got, err)
	}
	low := 3
	if err := db.save(nil, nil, "chat", &low); err != nil {
		t.Fatal(err)
	}
	if got, err := db.cursor("chat"); err != nil || got != 7 {
		t.Fatalf("cursor regressed to %d: %v", got, err)
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relativeDataPath, err := filepath.Rel(workingDirectory, dataPath)
	if err != nil {
		t.Fatal(err)
	}
	dry, err := openStore(relativeDataPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dry.db.Close() }()
	if _, err := dry.prepare(identity, 42, false, 0); err != nil {
		t.Fatal(err)
	}
	dryCursor := 9
	message.Content = "dry run"
	if err := dry.save([]archiveMessage{message}, nil, "chat", &dryCursor); err != nil {
		t.Fatal(err)
	}
	if got, err := dry.cursor("chat"); err != nil || got != 7 {
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
	if _, err = legacy.Exec("CREATE TABLE messages(id INTEGER PRIMARY KEY, type TEXT NOT NULL, date TIMESTAMP NOT NULL, edit_date TIMESTAMP, content TEXT, reply_to INTEGER, user_id INTEGER, media_id INTEGER); INSERT INTO messages(id,type,date) VALUES(50,'message','2026-07-29')"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := openStore(legacyPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = migrated.db.Close() }()
	migrated.dryRun = true
	if pending, err := migrated.prepare(identity, 42, false, 0); err != nil || !pending {
		t.Fatalf("legacy dry-run pending=%v err=%v", pending, err)
	}
	migrated.dryRun = false
	if _, err := migrated.prepare(identity, 42, false, 0); err == nil || !strings.Contains(err.Error(), "bootstrap-peer") {
		t.Fatalf("legacy database was bound implicitly: %v", err)
	}
	if _, err := migrated.prepare(identity, 42, true, 0); err != nil {
		t.Fatal(err)
	}
	if cursor, err := migrated.cursor(baseScope(identity)); err != nil || cursor != 50 {
		t.Fatalf("legacy cursor=%d err=%v", cursor, err)
	}
	if _, err := migrated.db.Exec("INSERT INTO messages(id,type,date,json_dump) VALUES(1,'message','2026-07-29','{}')"); err != nil {
		t.Fatalf("json_dump migration failed: %v", err)
	}

	oldPluginPath := filepath.Join(t.TempDir(), "old-plugin.sqlite")
	oldPlugin, err := sql.Open("sqlite", oldPluginPath)
	if err != nil {
		t.Fatal(err)
	}
	oldSchema := strings.Replace(schema, ", account_id INTEGER NOT NULL,\n  migrated_to INTEGER", "", 1)
	if _, err = oldPlugin.Exec(oldSchema + `
INSERT INTO messages(id,type,date,json_dump) VALUES(50,'message','2026-07-29','{}');
INSERT INTO sync_state(scope,last_seen_id) VALUES('9',50);
INSERT INTO archive_metadata(id,peer_id,peer_type) VALUES(1,-1000000000009,'channel');
INSERT INTO archive_peer_cache(id,peer_selector,peer_title,peer_access_hash,peer_flags) VALUES(1,'channel','Channel',99,1);`); err != nil {
		t.Fatal(err)
	}
	if err := oldPlugin.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := openStore(oldPluginPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upgraded.db.Close() }()
	if _, err := upgraded.prepare(identity, 42, false, 0); err == nil || !strings.Contains(err.Error(), "bootstrap-peer") {
		t.Fatalf("old plugin database was rebound implicitly: %v", err)
	}
	if _, err := upgraded.prepare(identity, 42, true, 0); err != nil {
		t.Fatal(err)
	}
	if cursor, err := upgraded.cursor(baseScope(identity)); err != nil || cursor != 50 {
		t.Fatalf("old plugin cursor=%d err=%v", cursor, err)
	}

	entities := peer.NewEntities(nil, nil, map[int64]*tg.Channel{9: {ID: 9, Title: "Private group"}})
	if !dialogMatches("Private group", &tg.PeerChannel{ChannelID: 9}, entities) || !dialogMatches("-1000000000009", &tg.PeerChannel{ChannelID: 9}, entities) {
		t.Fatal("private channel did not match by title and TDLib ID")
	}
	numericTitle := peer.NewEntities(nil, nil, map[int64]*tg.Channel{10: {ID: 10, Title: "-1000000000009"}})
	if dialogMatches("-1000000000009", &tg.PeerChannel{ChannelID: 10}, numericTitle) {
		t.Fatal("numeric selector matched a title instead of its canonical peer ID")
	}
	archived, _, err := baseMessage(&tg.Message{ID: 1, Date: 1, PeerID: &tg.PeerChannel{ChannelID: 9}}, entities)
	if err != nil || archived.User.ID != -1000000000009 || archived.User.Username != "Private group" || archived.JSON == "" {
		t.Fatalf("channel sender fallback: user=%+v err=%v", archived.User, err)
	}
	reply := &tg.MessageReplyHeader{}
	reply.SetReplyToMsgID(7)
	reply.SetReplyToPeerID(&tg.PeerChannel{ChannelID: 10})
	replied := &tg.Message{ID: 2, Date: 1, PeerID: &tg.PeerChannel{ChannelID: 9}}
	replied.SetReplyTo(reply)
	archived, _, err = baseMessage(replied, entities)
	if err != nil || archived.ReplyTo != nil {
		t.Fatalf("cross-peer reply became a local link: reply=%v err=%v", archived.ReplyTo, err)
	}
	reply.SetReplyToPeerID(&tg.PeerChannel{ChannelID: 9})
	archived, _, err = baseMessage(replied, entities)
	if err != nil || archived.ReplyTo == nil || *archived.ReplyTo != 7 {
		t.Fatalf("local reply was lost: reply=%v err=%v", archived.ReplyTo, err)
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
	})
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
			&tg.Message{ID: 3, PeerID: &tg.PeerChat{ChatID: 1}}, &tg.Message{ID: 2, PeerID: &tg.PeerChat{ChatID: 1}}, &tg.Message{ID: 1, PeerID: &tg.PeerChat{ChatID: 1}},
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
		return &tg.MessagesMessages{Messages: []tg.MessageClass{&tg.Message{ID: request.OffsetID, PeerID: &tg.PeerChannel{ChannelID: 1}}}}, nil
	})
	collected, missing, err := (&synchronizer{}).collectThreadIDs(context.Background(), threadQuery, &tg.InputPeerChannel{ChannelID: 1}, []int{7, 3}, nil)
	if err != nil || len(missing) != 0 || threadCalls != 2 || len(collected) != 2 || collected[0].Msg.GetID() != 3 || collected[1].Msg.GetID() != 7 {
		t.Fatalf("thread exact IDs: ids=%v missing=%v calls=%d err=%v", collected, missing, threadCalls, err)
	}
	foreignThread := messages.QueryFunc(func(_ context.Context, request messages.Request) (tg.MessagesMessagesClass, error) {
		return &tg.MessagesMessages{Messages: []tg.MessageClass{&tg.Message{ID: request.OffsetID, PeerID: &tg.PeerChannel{ChannelID: 2}}}}, nil
	})
	if _, _, err := (&synchronizer{}).collectThreadIDs(context.Background(), foreignThread, &tg.InputPeerChannel{ChannelID: 1}, []int{7}, nil); err == nil || !strings.Contains(err.Error(), "another peer") {
		t.Fatalf("cross-peer thread was accepted: %v", err)
	}
	emptyThread := messages.QueryFunc(func(context.Context, messages.Request) (tg.MessagesMessagesClass, error) {
		return &tg.MessagesMessages{}, nil
	})
	if _, missing, err := (&synchronizer{}).collectThreadIDs(context.Background(), emptyThread, &tg.InputPeerChannel{ChannelID: 1}, []int{7}, nil); err != nil || len(missing) != 1 || missing[0] != 7 {
		t.Fatalf("missing thread message: missing=%v err=%v", missing, err)
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
	migration, _, err := baseMessage(&tg.MessageService{ID: 2, Date: 1, PeerID: &tg.PeerChat{ChatID: 1}, Action: &tg.MessageActionChatMigrateTo{ChannelID: 10}}, peer.Entities{})
	if err != nil || migration.Type != "migrated_to" || migration.MigrationTo != -1000000000010 || migration.Content != "-1000000000010" {
		t.Fatalf("migration message=%+v err=%v", migration, err)
	}
}

func TestConfigOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("group: original\ndownload_media: true\nfetch_batch_size: 50\nfetch_wait: 5\nfetch_limit: 20\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options, err := parseOptions([]string{"sync", "--config", path, "--chat", "override", "--download-media=false", "--fetch-wait", "2", "--fetch-limit", "1", "--reconcile"})
	if err != nil {
		t.Fatal(err)
	}
	if options.Config.Group != "override" || options.Config.DownloadMedia || options.Config.FetchBatchSize != 50 || options.Config.FetchWait != 2 || options.Config.FetchLimit != 1 || !options.Reconcile {
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

func TestHistoryPacingStopsBeforeTheNextRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	syncer := synchronizer{cfg: config{FetchBatchSize: 2, FetchWait: 3600}}
	query := syncer.pacedQuery(messages.QueryFunc(func(context.Context, messages.Request) (tg.MessagesMessagesClass, error) {
		calls++
		return &tg.MessagesMessages{Messages: []tg.MessageClass{&tg.Message{ID: calls}}}, nil
	}))
	for range 2 {
		if _, err := query.Query(ctx, messages.Request{}); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	if _, err := query.Query(ctx, messages.Request{}); !errors.Is(err, context.Canceled) || calls != 2 {
		t.Fatalf("pacing did not stop the next request: calls=%d err=%v", calls, err)
	}
}

func TestIdentityAndMediaFailures(t *testing.T) {
	freshPath := filepath.Join(t.TempDir(), "fresh.sqlite")
	fresh, err := openStore(freshPath, false)
	if err != nil {
		t.Fatal(err)
	}
	if cached, err := fresh.pinnedPeer(); err != nil || cached != nil {
		t.Fatalf("fresh peer cache=%+v err=%v", cached, err)
	}
	if err := fresh.db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(freshPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("peer cache probe created database: %v", err)
	}

	path := filepath.Join(t.TempDir(), "data.sqlite")
	store, err := openStore(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.db.Close() }()
	identity := peerResult{Selector: "@channel", ID: -1000000000009, Type: "channel", Title: "Channel", AccessHash: 99, Flags: channelMegagroup}
	if _, err := store.db.Exec(`CREATE TABLE archive_metadata(id INTEGER PRIMARY KEY, peer_id INTEGER, peer_type TEXT);
INSERT INTO archive_metadata VALUES(1, -1000000000009, 'channel');
CREATE TABLE archive_peer_cache(id INTEGER PRIMARY KEY, peer_selector TEXT, peer_title TEXT, peer_access_hash INTEGER);
INSERT INTO archive_peer_cache VALUES(1, '@channel', 'Channel', 99);
CREATE TABLE media_failures(message_id INTEGER PRIMARY KEY, attempts INTEGER, last_error TEXT);`); err != nil {
		t.Fatal(err)
	}
	store.existing = true
	if cached, err := store.pinnedPeer(); err != nil || cached != nil {
		t.Fatalf("legacy cache was not treated as incomplete: %+v err=%v", cached, err)
	}
	if _, err := store.prepare(identity, 42, false, 0); err != nil {
		t.Fatal(err)
	}
	cached, err := store.pinnedPeer()
	if err != nil || cached == nil || cached.ID != identity.ID || cached.AccessHash != identity.AccessHash {
		t.Fatalf("cached peer=%+v err=%v", cached, err)
	}
	p, err := cachedPeer(peers.Options{}.Build(nil), *cached)
	channel, channelOK := p.(peers.Channel)
	if err != nil || !channelOK || channel.InputPeer().(*tg.InputPeerChannel).AccessHash != identity.AccessHash || !channel.IsSupergroup() {
		t.Fatalf("reconstructed peer=%+v err=%v", p, err)
	}
	self, err := cachedPeer(peers.Options{}.Build(nil), storedPeer{ID: 42, Type: "user", Title: "Saved Messages", Flags: peerSelf})
	if _, ok := self.InputPeer().(*tg.InputPeerSelf); err != nil || !ok || peerFlags(self) != peerSelf {
		t.Fatalf("reconstructed self=%+v err=%v", self, err)
	}
	if _, err := store.prepare(identity, 43, false, 0); err == nil || !strings.Contains(err.Error(), "account 42") {
		t.Fatalf("account mismatch accepted: %v", err)
	}
	if _, err := store.prepare(peerResult{ID: -1000000000010, Type: "channel"}, 42, false, 0); err == nil || !strings.Contains(err.Error(), "pinned") {
		t.Fatalf("peer mismatch accepted: %v", err)
	}
	if encodedPeerID(&tg.PeerUser{UserID: 9}) == encodedPeerID(&tg.PeerChannel{ChannelID: 9}) {
		t.Fatal("user and channel sender IDs collide")
	}

	now := time.Now().UTC()
	cursor := 1
	message := archiveMessage{ID: 1, Type: "message", Date: now, JSON: `{"id":1}`, User: archiveUser{ID: 1}, Media: &archiveMedia{ID: 1, URL: "1.jpg"}, MediaAction: mediaReplace}
	if err := store.save([]archiveMessage{message}, nil, "9", &cursor); err != nil {
		t.Fatal(err)
	}
	cursor = 2
	message.Media, message.MediaAction = nil, mediaKeep
	if err := store.save([]archiveMessage{message}, nil, "9", &cursor); err != nil {
		t.Fatal(err)
	}
	var mediaID, attempts int
	if err := store.db.QueryRow("SELECT media_id FROM messages WHERE id = 1").Scan(&mediaID); err != nil || mediaID != 1 {
		t.Fatalf("media link was not preserved: id=%d err=%v", mediaID, err)
	}
	message.MediaAction = mediaClear
	message.MediaFailure = &mediaFailure{Attempts: 3, Error: "download failed"}
	if err := store.save([]archiveMessage{message}, nil, "9", &cursor); err != nil {
		t.Fatal(err)
	}
	var cleared sql.NullInt64
	if err := store.db.QueryRow("SELECT media_id FROM messages WHERE id = 1").Scan(&cleared); err != nil || cleared.Valid {
		t.Fatalf("stale media link remained: id=%v err=%v", cleared, err)
	}
	if err := store.db.QueryRow("SELECT attempts FROM media_failures WHERE message_id = 1").Scan(&attempts); err != nil || attempts != 3 {
		t.Fatalf("failure was not persisted: attempts=%d err=%v", attempts, err)
	}
	cutoff, err := store.cursor(mediaFailureScope)
	if err != nil || cutoff != 1 {
		t.Fatalf("media failure cutoff=%d err=%v", cutoff, err)
	}
	retryIDs, err := store.pendingMediaFailures(cutoff, 100)
	if err != nil || fmt.Sprint(retryIDs) != "[1]" {
		t.Fatalf("retry snapshot: ids=%v err=%v", retryIDs, err)
	}
	second := message
	second.ID, second.JSON = 3, `{"id":3}`
	if err := store.save([]archiveMessage{second}, nil, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.save([]archiveMessage{message}, nil, "", nil); err != nil {
		t.Fatal(err)
	}
	if ids, err := store.pendingMediaFailures(cutoff, 100); err != nil || len(ids) != 0 {
		t.Fatalf("attempted failures remained in the starting queue: ids=%v err=%v", ids, err)
	}
	currentCutoff, err := store.cursor(mediaFailureScope)
	if err != nil || currentCutoff != 3 {
		t.Fatalf("current media failure cutoff=%d err=%v", currentCutoff, err)
	}
	if ids, err := store.pendingMediaFailures(currentCutoff, 100); err != nil || fmt.Sprint(ids) != "[3 1]" {
		t.Fatalf("failure queue did not rotate: ids=%v err=%v", ids, err)
	}
	if _, err := store.db.Exec("DELETE FROM media_failures WHERE message_id=3"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.cursor("9"); err != nil || got != 2 {
		t.Fatalf("cursor=%d err=%v", got, err)
	}
	if err := store.save(nil, []int{1}, "", nil); err != nil {
		t.Fatal(err)
	}
	if ids, err := store.pendingMediaFailures(currentCutoff, 100); err != nil || len(ids) != 0 {
		t.Fatalf("unavailable failure remained pending: ids=%v err=%v", ids, err)
	}
	if pending, unavailable, err := store.mediaFailureCounts(); err != nil || pending != 0 || unavailable != 1 {
		t.Fatalf("media failure counts: pending=%d unavailable=%d err=%v", pending, unavailable, err)
	}
	message.Media = &archiveMedia{ID: 1, URL: "1.jpg"}
	message.MediaAction, message.MediaFailure, message.ClearMediaFailure = mediaReplace, nil, true
	if err := store.save([]archiveMessage{message}, nil, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("SELECT COUNT(*) FROM media_failures").Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("failure was not cleared: count=%d err=%v", attempts, err)
	}
	migration := archiveMessage{ID: 2, Type: "migrated_to", Date: now, Content: "-1000000000010", JSON: `{"id":2}`, User: archiveUser{ID: 1}, MigrationTo: -1000000000010}
	if err := store.save([]archiveMessage{migration}, nil, "", nil); err != nil {
		t.Fatal(err)
	}
	if target, err := store.migrationTarget(); err != nil || target != -1000000000010 {
		t.Fatalf("migration target=%d err=%v", target, err)
	}
	if _, err := store.db.Exec("DELETE FROM messages WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	if target, err := store.migrationTarget(); err != nil || target != -1000000000010 {
		t.Fatalf("durable migration target=%d err=%v", target, err)
	}
	chatStore, err := openStore(filepath.Join(t.TempDir(), "migrated.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = chatStore.db.Close() }()
	chat := peerResult{Selector: "group", ID: -9, Type: "chat", Title: "Group"}
	if _, err := chatStore.prepare(chat, 42, false, -1000000000010); err != nil {
		t.Fatal(err)
	}
	if target, err := chatStore.migrationTarget(); err != nil || target != -1000000000010 {
		t.Fatalf("entity migration target=%d err=%v", target, err)
	}
	boundary := synchronizer{store: chatStore, peer: chat}
	if err := boundary.checkMigrationBoundary(); err != nil {
		t.Fatalf("incomplete migrated chat was blocked: %v", err)
	}
	boundary.caughtUp = true
	if err := boundary.checkMigrationBoundary(); err == nil || !strings.Contains(err.Error(), "-1000000000010") {
		t.Fatalf("completed migrated chat was not stopped: %v", err)
	}
}

func TestExactIDsArePeerBound(t *testing.T) {
	api := tg.NewClient(telegram.InvokeFunc(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		if _, ok := input.(*tg.MessagesGetMessagesRequest); !ok {
			return fmt.Errorf("unexpected request %T", input)
		}
		output.(*tg.MessagesMessagesBox).Messages = &tg.MessagesMessages{Messages: []tg.MessageClass{
			&tg.Message{ID: 77, PeerID: &tg.PeerUser{UserID: 222}},
		}}
		return nil
	}))
	syncer := synchronizer{}
	elems, missing, err := syncer.collectIDs(context.Background(), api, &tg.InputPeerUser{UserID: 111}, []int{77}, nil)
	if err != nil || len(elems) != 0 || len(missing) != 1 || missing[0] != 77 {
		t.Fatalf("wrong-peer exact result: elems=%v missing=%v err=%v", elems, missing, err)
	}
	elems, missing, err = syncer.collectIDs(context.Background(), api, &tg.InputPeerUser{UserID: 222}, []int{77}, nil)
	if err != nil || len(elems) != 1 || len(missing) != 0 {
		t.Fatalf("matching exact result: elems=%v missing=%v err=%v", elems, missing, err)
	}
	syncer.peer.ID = 222
	elems, missing, err = syncer.collectIDs(context.Background(), api, &tg.InputPeerSelf{}, []int{77}, nil)
	if err != nil || len(elems) != 1 || len(missing) != 0 {
		t.Fatalf("saved-messages exact result: elems=%v missing=%v err=%v", elems, missing, err)
	}
}

func TestMediaIntegrityAndPolls(t *testing.T) {
	entities := peer.NewEntities(nil, nil, map[int64]*tg.Channel{9: {ID: 9, Title: "group"}})
	photo := &tg.MessageMediaPhoto{Photo: &tg.Photo{ID: 55, DCID: 1, Sizes: []tg.PhotoSizeClass{&tg.PhotoSize{Type: "x", Size: 3}}}}
	message := &tg.Message{ID: 7, Date: 1, PeerID: &tg.PeerChannel{ChannelID: 9}}
	message.SetMedia(photo)
	dir := t.TempDir()
	path := filepath.Join(dir, "-1000000000009", "7-p55-3.jpg")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	mapper := mediaMapper{ctx: context.Background(), peerID: -1000000000009, cfg: config{DownloadMedia: true, MediaDir: dir}, downloadFn: func(_ *tmedia.Media, path string) error {
		calls++
		return os.WriteFile(path, []byte("new"), 0o600)
	}}
	archived, event, err := mapper.message(messages.Elem{Msg: message, Entities: entities})
	if err != nil || event != mediaDownloaded || calls != 1 || archived.Media.URL != "-1000000000009/7-p55-3.jpg" {
		t.Fatalf("media repair: media=%+v event=%v calls=%d err=%v", archived.Media, event, calls, err)
	}
	if info, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o644 {
		t.Fatalf("published media mode=%v", info.Mode().Perm())
	}
	_, event, err = mapper.message(messages.Elem{Msg: message, Entities: entities})
	if err != nil || event != mediaReused || calls != 1 {
		t.Fatalf("verified reuse: event=%v calls=%d err=%v", event, calls, err)
	}
	excluded := mapper
	excluded.cfg.MediaMIMETypes = []string{"video/mp4"}
	archived, event, err = excluded.message(messages.Elem{Msg: message, Entities: entities})
	if err != nil || event != mediaSkipped || archived.MediaAction != mediaClear || !archived.ClearMediaFailure {
		t.Fatalf("excluded media policy: action=%v event=%v clear=%v err=%v", archived.MediaAction, event, archived.ClearMediaFailure, err)
	}

	legacyMessage := &tg.Message{ID: 8, Date: 1, PeerID: &tg.PeerChannel{ChannelID: 9}}
	legacyPhoto := &tg.MessageMediaPhoto{Photo: &tg.Photo{ID: 56, DCID: 1, Sizes: []tg.PhotoSizeClass{&tg.PhotoSize{Type: "x", Size: 3}}}}
	legacyMessage.SetMedia(legacyPhoto)
	legacyJSON, err := json.Marshal(legacyMessage)
	if err != nil || storedSourceKey(string(legacyJSON)) != "p56" {
		t.Fatalf("stored media identity=%q err=%v", storedSourceKey(string(legacyJSON)), err)
	}
	legacyPath := filepath.Join(dir, "8.jpg")
	if err := os.WriteFile(legacyPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	adopt := mapper
	adopt.existing = map[int]storedMedia{8: {URL: "8.jpg"}}
	archived, event, err = adopt.message(messages.Elem{Msg: legacyMessage, Entities: entities})
	adoptedPath := filepath.Join(dir, "-1000000000009", "8-p56-3.jpg")
	if err != nil || event != mediaReused || calls != 1 || archived.Media.URL != "-1000000000009/8-p56-3.jpg" {
		t.Fatalf("legacy media adoption: media=%+v event=%v calls=%d err=%v", archived.Media, event, calls, err)
	}
	if info, err := os.Stat(adoptedPath); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o644 || info.Size() != 3 {
		t.Fatalf("adopted media=%v", info)
	}
	publishedPath := filepath.Join(dir, "published.bin")
	if err := publishFile(publishedPath, 3, func(file *os.File) error {
		_, err := file.Write([]byte("new"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(publishedPath); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o644 || info.Size() != 3 {
		t.Fatalf("published media=%v", info)
	}

	disabledMessage := &tg.Message{ID: 9, Date: 1, PeerID: &tg.PeerChannel{ChannelID: 9}}
	disabledMessage.SetMedia(photo)
	disabled := mapper
	disabled.cfg.DownloadMedia = false
	disabled.existing = map[int]storedMedia{9: {URL: "-1000000000009/9-p1-3.jpg", Source: "p55"}}
	archived, event, err = disabled.message(messages.Elem{Msg: disabledMessage, Entities: entities})
	if err != nil || event != mediaSkipped || archived.MediaAction != mediaClear || archived.ClearMediaFailure {
		t.Fatalf("disabled replacement policy: action=%v event=%v clear=%v err=%v", archived.MediaAction, event, archived.ClearMediaFailure, err)
	}
	disabled.existing[9] = storedMedia{URL: "-1000000000009/9-p55-3.jpg", Source: "p55"}
	archived, event, err = disabled.message(messages.Elem{Msg: disabledMessage, Entities: entities})
	if err != nil || event != mediaSkipped || archived.MediaAction != mediaKeep || archived.ClearMediaFailure {
		t.Fatalf("disabled matching media policy: action=%v event=%v clear=%v err=%v", archived.MediaAction, event, archived.ClearMediaFailure, err)
	}

	disabledLegacy := mapper
	disabledLegacy.cfg.DownloadMedia = false
	disabledLegacy.existing = map[int]storedMedia{8: {URL: "8.jpg"}}
	if err := os.Remove(adoptedPath); err != nil {
		t.Fatal(err)
	}
	archived, event, err = disabledLegacy.message(messages.Elem{Msg: legacyMessage, Entities: entities})
	if err != nil || event != mediaSkipped || archived.MediaAction != mediaKeep {
		t.Fatalf("disabled legacy policy: action=%v event=%v err=%v", archived.MediaAction, event, err)
	}
	if _, err := os.Stat(adoptedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled media adopted legacy file: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(adoptedPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(adoptedPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	disabledLegacy.existing[8] = storedMedia{URL: "-1000000000009/8-p56-3.jpg", Source: "p56"}
	archived, event, err = disabledLegacy.message(messages.Elem{Msg: legacyMessage, Entities: entities})
	if err != nil || event != mediaReused || archived.MediaAction != mediaReplace {
		t.Fatalf("disabled existing media: action=%v event=%v err=%v", archived.MediaAction, event, err)
	}
	if info, err := os.Stat(adoptedPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("disabled media mode changed: info=%v err=%v", info, err)
	}

	poll := &tg.MessageMediaPoll{Poll: tg.Poll{Question: tg.TextWithEntities{Text: "Question"}, Answers: []tg.PollAnswerClass{
		&tg.PollAnswer{Text: tg.TextWithEntities{Text: "Yes"}, Option: []byte{1}},
		&tg.PollAnswer{Text: tg.TextWithEntities{Text: "No"}, Option: []byte{2}},
	}}}
	media, action, _, _, _, err := mapper.media(8, &tg.Message{ID: 8}, poll)
	if err != nil || action != mediaReplace || media == nil || media.Title != "Question" || !strings.Contains(media.Description, `"Yes"`) {
		t.Fatalf("zero-vote poll: media=%+v action=%v err=%v", media, action, err)
	}
}

func TestPeerResolutionAndFloodWait(t *testing.T) {
	archiveStore, err := openStore(filepath.Join(t.TempDir(), "data.sqlite"), true)
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
	syncer := synchronizer{cfg: config{Group: "@Ktbalbadr"}, store: archiveStore}
	p, _, err := syncer.resolvePeer(context.Background(), peers.Options{}.Build(api), api)
	if err != nil || calls != 1 || p.ID() != 9 {
		t.Fatalf("direct username resolution: peer=%+v calls=%d err=%v", p, calls, err)
	}
	syncer.cfg.Group = "Ktbalbadr"
	p, _, err = syncer.resolvePeer(context.Background(), peers.Options{}.Build(api), api)
	if err != nil || calls != 2 || p.ID() != 9 {
		t.Fatalf("bare username resolution: peer=%+v calls=%d err=%v", p, calls, err)
	}

	bound, err := openStore(filepath.Join(t.TempDir(), "bound.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bound.db.Close() }()
	identity := peerResult{Selector: "Ktbalbadr", ID: encodedChannelID(9), Type: "channel", Title: "Cached", AccessHash: 99, Flags: channelPublic}
	if _, err := bound.prepare(identity, 42, false, 0); err != nil {
		t.Fatal(err)
	}
	liveCalls := 0
	live := tg.NewClient(telegram.InvokeFunc(func(_ context.Context, _ bin.Encoder, output bin.Decoder) error {
		liveCalls++
		*output.(*tg.ContactsResolvedPeer) = tg.ContactsResolvedPeer{
			Peer:  &tg.PeerChannel{ChannelID: 9},
			Chats: []tg.ChatClass{&tg.Channel{ID: 9, AccessHash: 100, Title: "Live"}},
		}
		return nil
	}))
	boundSyncer := synchronizer{cfg: config{Group: identity.Selector}, store: bound}
	p, _, err = boundSyncer.resolvePeer(context.Background(), peers.Options{}.Build(live), live)
	channel, public := p.(peers.Channel)
	if err != nil || liveCalls != 0 || peerAccessHash(p.InputPeer()) != 99 || !public || !isPublicChannel(channel.Raw()) {
		t.Fatalf("cached peer was not used first: peer=%+v calls=%d err=%v", p, liveCalls, err)
	}
	offline := tg.NewClient(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { return errors.New("offline") }))
	p, _, err = boundSyncer.resolvePeer(context.Background(), peers.Options{}.Build(offline), offline)
	if err != nil || peerAccessHash(p.InputPeer()) != 99 {
		t.Fatalf("cached peer fallback: peer=%+v err=%v", p, err)
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
	archived, event, err := mapper.message(messages.Elem{Msg: message, Entities: entities})
	if err != nil || event != mediaFailed || calls != 3 || archived.MediaAction != mediaClear || archived.MediaFailure == nil || !strings.Contains(archived.MediaFailure.Error, mapper.cfg.MediaDir) {
		t.Fatalf("retry result: event=%v action=%v calls=%d failure=%v err=%v", event, archived.MediaAction, calls, archived.MediaFailure, err)
	}
	if !fatalMediaError(context.Canceled) || !fatalMediaError(context.DeadlineExceeded) {
		t.Fatal("context termination was treated as recoverable")
	}
	if !fatalMediaError(&os.PathError{Op: "open", Path: "media.tmp", Err: errors.New("local")}) ||
		!fatalMediaError(&os.LinkError{Op: "rename", Old: "media.tmp", New: "media", Err: errors.New("local")}) {
		t.Fatal("local filesystem failure was treated as recoverable")
	}
	if !fatalMediaError(tgerr.New(420, "FLOOD_WAIT_5")) {
		t.Fatal("flood wait was treated as recoverable")
	}
	calls = 0
	mapper.downloadFn = func(*tmedia.Media, string) error {
		calls++
		return tgerr.New(420, "FLOOD_WAIT_LIMIT_EXCEEDED_60")
	}
	if _, _, err := mapper.message(messages.Elem{Msg: message, Entities: entities}); err == nil || calls != 1 {
		t.Fatalf("flood wait retry result: calls=%d err=%v", calls, err)
	}
}

func TestReconcileArchive(t *testing.T) {
	identity := peerResult{Selector: "@archive", ID: -1000000000009, Type: "channel", Title: "Archive", AccessHash: 99}
	archiveStore, err := openStore(filepath.Join(t.TempDir(), "data.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = archiveStore.db.Close() }()
	if _, err := archiveStore.prepare(identity, 42, false, 0); err != nil {
		t.Fatal(err)
	}
	old := []archiveMessage{
		{ID: 1, Type: "message", Date: time.Unix(1, 0), Content: "old", JSON: `{"id":1,"message":"old"}`, User: archiveUser{ID: identity.ID}},
		{ID: 2, Type: "message", Date: time.Unix(2, 0), Content: "missing", JSON: `{"id":2}`, User: archiveUser{ID: identity.ID}},
	}
	if err := archiveStore.save(old, nil, "", nil); err != nil {
		t.Fatal(err)
	}
	until, err := archiveStore.reconcileLimit("9:reconcile")
	if err != nil || until != 2 {
		t.Fatalf("reconcile limit=%d err=%v", until, err)
	}
	third := archiveMessage{ID: 3, Type: "message", Date: time.Unix(3, 0), Content: "not in this pass", JSON: `{"id":3}`, User: archiveUser{ID: identity.ID}}
	if err := archiveStore.save([]archiveMessage{third}, nil, "", nil); err != nil {
		t.Fatal(err)
	}
	channel := &tg.Channel{ID: 9, AccessHash: 99, Title: "Archive"}
	available := map[int]string{1: "new"}
	requested := []int{}
	api := tg.NewClient(telegram.InvokeFunc(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		request, ok := input.(*tg.ChannelsGetMessagesRequest)
		if !ok {
			return fmt.Errorf("unexpected request %T", input)
		}
		result := &tg.MessagesMessages{Chats: []tg.ChatClass{channel}}
		for _, rawID := range request.ID {
			id := rawID.(*tg.InputMessageID).ID
			requested = append(requested, id)
			if content, ok := available[id]; ok {
				result.Messages = append(result.Messages, &tg.Message{ID: id, Date: id, PeerID: &tg.PeerChannel{ChannelID: 9}, Message: content})
			}
		}
		output.(*tg.MessagesMessagesBox).Messages = result
		return nil
	}))
	p := peers.Options{}.Build(api).Channel(channel)
	syncer := synchronizer{store: archiveStore, cfg: config{FetchBatchSize: 2}, peer: identity, reconcile: true}
	result := syncResult{}
	mapper := mediaMapper{ctx: context.Background()}
	if err := syncer.reconcileArchive(context.Background(), api, p, &mapper, &result, until); err != nil {
		t.Fatal(err)
	}
	if result.Reconciled != 1 || result.Missing != 1 || fmt.Sprint(requested) != "[1 2]" {
		t.Fatalf("reconcile result: %+v requested=%v", result, requested)
	}
	var content, revision string
	if err := archiveStore.db.QueryRow("SELECT content FROM messages WHERE id=1").Scan(&content); err != nil || content != "new" {
		t.Fatalf("refreshed content=%q err=%v", content, err)
	}
	if err := archiveStore.db.QueryRow("SELECT json_dump FROM message_revisions WHERE message_id=1").Scan(&revision); err != nil || revision != old[0].JSON {
		t.Fatalf("revision=%q err=%v", revision, err)
	}
	var tombstones int
	if err := archiveStore.db.QueryRow("SELECT COUNT(*) FROM message_tombstones WHERE message_id=2").Scan(&tombstones); err != nil || tombstones != 1 {
		t.Fatalf("tombstones=%d err=%v", tombstones, err)
	}
	if err := archiveStore.db.QueryRow("SELECT content FROM messages WHERE id=3").Scan(&content); err != nil || content != third.Content {
		t.Fatalf("snapshot included a new message: content=%q err=%v", content, err)
	}
	for _, scope := range []string{"9:reconcile", "9:reconcile:until"} {
		if cursor, err := archiveStore.cursor(scope); err != nil || cursor != 0 {
			t.Fatalf("completed reconcile scope %q=%d err=%v", scope, cursor, err)
		}
	}

	available[2], available[3], requested = "restored", "third", nil
	until, err = archiveStore.reconcileLimit("9:reconcile")
	if err != nil || until != 3 {
		t.Fatalf("next reconcile limit=%d err=%v", until, err)
	}
	result = syncResult{}
	if err := syncer.reconcileArchive(context.Background(), api, p, &mapper, &result, until); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(requested) != "[1 2 3]" || result.Missing != 0 {
		t.Fatalf("next reconcile result: %+v requested=%v", result, requested)
	}
	if err := archiveStore.db.QueryRow("SELECT COUNT(*) FROM message_tombstones WHERE message_id=2").Scan(&tombstones); err != nil || tombstones != 0 {
		t.Fatalf("restored message retained tombstone: count=%d err=%v", tombstones, err)
	}
	if err := archiveStore.db.QueryRow("SELECT content FROM messages WHERE id=2").Scan(&content); err != nil || content != "restored" {
		t.Fatalf("restored content=%q err=%v", content, err)
	}

}
