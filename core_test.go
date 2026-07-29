package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
)

func TestSelectionAndStore(t *testing.T) {
	if err := run(context.Background(), nil, []string{"sync", "--media-dir", "media"}); err == nil || !strings.Contains(err.Error(), "require --chat") {
		t.Fatalf("native option without chat: %v", err)
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
	dry, err := openStore(dataPath, true, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dry.db.Close() }()
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
	if _, err := migrated.db.Exec("INSERT INTO messages(id,type,date,json_dump) VALUES(1,'message','2026-07-29','{}')"); err != nil {
		t.Fatalf("json_dump migration failed: %v", err)
	}

	optional, err := openStore(filepath.Join(t.TempDir(), "optional.sqlite"), false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = optional.db.Close() }()
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
	_, err = (&mediaMapper{cfg: config{DownloadMedia: true, MediaDir: blockedMediaDir}, overwrite: true}).message(messages.Elem{
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
}
