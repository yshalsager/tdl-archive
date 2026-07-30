package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS users (
  id INTEGER NOT NULL PRIMARY KEY,
  username TEXT, first_name TEXT, last_name TEXT, tags TEXT, avatar TEXT
);
CREATE TABLE IF NOT EXISTS media (
  id INTEGER NOT NULL PRIMARY KEY,
  type TEXT, url TEXT, title TEXT, description TEXT, thumb TEXT
);
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER NOT NULL PRIMARY KEY,
  type TEXT NOT NULL, date TIMESTAMP NOT NULL, edit_date TIMESTAMP,
  content TEXT, reply_to INTEGER, user_id INTEGER, media_id INTEGER, json_dump JSON,
  FOREIGN KEY(user_id) REFERENCES users(id), FOREIGN KEY(media_id) REFERENCES media(id)
);
CREATE TABLE IF NOT EXISTS sync_state (
  scope TEXT NOT NULL PRIMARY KEY, last_seen_id INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS archive_metadata (
  id INTEGER NOT NULL PRIMARY KEY CHECK(id = 1),
  peer_id INTEGER NOT NULL, peer_type TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS media_failures (
  message_id INTEGER NOT NULL PRIMARY KEY,
  attempts INTEGER NOT NULL, last_error TEXT NOT NULL
);`

type mediaAction int

const (
	mediaKeep mediaAction = iota
	mediaReplace
	mediaClear
)

type mediaFailure struct {
	Attempts int
	Error    string
}

type archiveUser struct {
	ID                    int64
	Username, First, Last string
	Tags                  []string
	Avatar                string
}

type archiveMedia struct {
	ID                                   int
	Type, URL, Title, Description, Thumb string
}

type archiveMessage struct {
	ID                int
	Type              string
	Date              time.Time
	EditDate          *time.Time
	Content           string
	ReplyTo           *int
	User              archiveUser
	Media             *archiveMedia
	JSON              string
	MediaAction       mediaAction
	MediaFailure      *mediaFailure
	ClearMediaFailure bool
}

type store struct {
	db       *sql.DB
	jsonDump bool
	dryRun   bool
	existing bool
}

func openStore(path string, jsonDump, dryRun bool) (*store, error) {
	_, statErr := os.Stat(path)
	existing := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	dsn := path
	if dryRun {
		if existing {
			absolutePath, err := filepath.Abs(path)
			if err != nil {
				return nil, err
			}
			dsn = (&url.URL{Scheme: "file", Path: filepath.ToSlash(absolutePath), RawQuery: "mode=ro"}).String()
		} else {
			dsn = ":memory:"
		}
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	return &store{db: db, jsonDump: jsonDump, dryRun: dryRun, existing: existing}, nil
}

func (s *store) prepare(peer peerResult, bootstrap bool) (bool, error) {
	var peerID int64
	var peerType string
	err := s.db.QueryRow("SELECT peer_id, peer_type FROM archive_metadata WHERE id = 1").Scan(&peerID, &peerType)
	if err == nil {
		if peerID != peer.ID || peerType != peer.Type {
			return false, fmt.Errorf("database is pinned to %s %d, resolved %s %d", peerType, peerID, peer.Type, peer.ID)
		}
		if !s.dryRun {
			if _, err = s.db.Exec(schema); err == nil {
				err = s.ensureJSONDump()
			}
		}
		return false, err
	}
	if err != sql.ErrNoRows && !strings.Contains(err.Error(), "no such table: archive_metadata") {
		return false, err
	}
	pending := true
	if s.dryRun {
		if !s.existing {
			_, err = s.db.Exec(schema)
		}
		return pending, err
	}
	if s.existing && !bootstrap {
		return pending, fmt.Errorf("existing unpinned database requires --bootstrap-peer")
	}
	if _, err = s.db.Exec(schema); err != nil {
		return pending, err
	}
	if err = s.ensureJSONDump(); err != nil {
		return pending, err
	}
	_, err = s.db.Exec("INSERT INTO archive_metadata(id, peer_id, peer_type) VALUES(1, ?, ?)", peer.ID, peer.Type)
	return pending, err
}

func (s *store) ensureJSONDump() error {
	rows, err := s.db.Query("PRAGMA table_info(messages)")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == "json_dump" {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.Exec("ALTER TABLE messages ADD COLUMN json_dump JSON")
	return err
}

func (s *store) cursor(scope string, useExistingMessages bool) (int, error) {
	var id int
	err := s.db.QueryRow("SELECT last_seen_id FROM sync_state WHERE scope = ?", scope).Scan(&id)
	if err != nil && strings.Contains(err.Error(), "no such table: sync_state") {
		err = sql.ErrNoRows
	}
	if err == sql.ErrNoRows && useExistingMessages {
		err = s.db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM messages").Scan(&id)
	} else if err == sql.ErrNoRows {
		err = nil
	}
	return id, err
}

func (s *store) save(messages []archiveMessage, scope string, cursor *int) error {
	if s.dryRun {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, m := range messages {
		if _, err = tx.Exec(`INSERT INTO users(id, username, first_name, last_name, tags, avatar) VALUES(?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name,
last_name=excluded.last_name, tags=excluded.tags, avatar=COALESCE(excluded.avatar,users.avatar)`, m.User.ID, m.User.Username, m.User.First, m.User.Last, strings.Join(m.User.Tags, " "), nullString(m.User.Avatar)); err != nil {
			return fmt.Errorf("save user %d: %w", m.User.ID, err)
		}
		var mediaID any
		if m.MediaAction == mediaReplace && m.Media != nil {
			mediaID = m.Media.ID
			if _, err = tx.Exec(`INSERT INTO media(id,type,url,title,description,thumb) VALUES(?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET type=excluded.type,url=excluded.url,title=excluded.title,description=excluded.description,thumb=excluded.thumb`,
				m.Media.ID, m.Media.Type, nullString(m.Media.URL), nullString(m.Media.Title), nullString(m.Media.Description), nullString(m.Media.Thumb)); err != nil {
				return fmt.Errorf("save media %d: %w", m.Media.ID, err)
			}
		}
		query := `INSERT INTO messages(id,type,date,edit_date,content,reply_to,user_id,media_id) VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET type=excluded.type,date=excluded.date,edit_date=excluded.edit_date,
content=excluded.content,reply_to=excluded.reply_to,user_id=excluded.user_id,
media_id=CASE WHEN ? THEN excluded.media_id ELSE messages.media_id END`
		updateMedia := m.MediaAction == mediaReplace || m.MediaAction == mediaClear
		args := []any{m.ID, m.Type, formatTime(m.Date), formatOptionalTime(m.EditDate), m.Content, m.ReplyTo, m.User.ID, mediaID, updateMedia}
		if s.jsonDump {
			query = `INSERT INTO messages(id,type,date,edit_date,content,reply_to,user_id,media_id,json_dump) VALUES(?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET type=excluded.type,date=excluded.date,edit_date=excluded.edit_date,
content=excluded.content,reply_to=excluded.reply_to,user_id=excluded.user_id,
media_id=CASE WHEN ? THEN excluded.media_id ELSE messages.media_id END,json_dump=excluded.json_dump`
			args = []any{m.ID, m.Type, formatTime(m.Date), formatOptionalTime(m.EditDate), m.Content, m.ReplyTo, m.User.ID, mediaID, m.JSON, updateMedia}
		}
		if _, err = tx.Exec(query, args...); err != nil {
			return fmt.Errorf("save message %d: %w", m.ID, err)
		}
		if m.MediaFailure != nil {
			_, err = tx.Exec(`INSERT INTO media_failures(message_id,attempts,last_error) VALUES(?,?,?)
ON CONFLICT(message_id) DO UPDATE SET attempts=media_failures.attempts+excluded.attempts,last_error=excluded.last_error`,
				m.ID, m.MediaFailure.Attempts, m.MediaFailure.Error)
		} else if m.ClearMediaFailure {
			_, err = tx.Exec("DELETE FROM media_failures WHERE message_id = ?", m.ID)
		}
		if err != nil {
			return fmt.Errorf("update media failure %d: %w", m.ID, err)
		}
	}
	if cursor != nil {
		if _, err = tx.Exec(`INSERT INTO sync_state(scope,last_seen_id) VALUES(?,?)
ON CONFLICT(scope) DO UPDATE SET last_seen_id=excluded.last_seen_id`, scope, *cursor); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *store) pendingMediaFailures() ([]int, error) {
	rows, err := s.db.Query("SELECT message_id FROM media_failures ORDER BY message_id")
	if err != nil {
		if strings.Contains(err.Error(), "no such table: media_failures") {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *store) clearMediaFailures(ids []int) error {
	if s.dryRun || len(ids) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, id := range ids {
		if _, err = tx.Exec("DELETE FROM media_failures WHERE message_id = ?", id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func formatTime(v time.Time) string { return v.UTC().Format("2006-01-02 15:04:05") }
func formatOptionalTime(v *time.Time) any {
	if v == nil {
		return nil
	}
	return formatTime(*v)
}
