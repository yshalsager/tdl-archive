package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/constant"
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
  peer_id INTEGER NOT NULL, peer_type TEXT NOT NULL, account_id INTEGER NOT NULL,
  migrated_to INTEGER
);
CREATE TABLE IF NOT EXISTS archive_peer_cache (
  id INTEGER NOT NULL PRIMARY KEY CHECK(id = 1),
  peer_selector TEXT NOT NULL, peer_title TEXT NOT NULL,
  peer_access_hash INTEGER NOT NULL, peer_flags INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS media_failures (
  message_id INTEGER NOT NULL PRIMARY KEY,
  attempts INTEGER NOT NULL, last_error TEXT NOT NULL, queue_order INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS message_tombstones (
  message_id INTEGER NOT NULL PRIMARY KEY, detected_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS message_revisions (
  id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL,
  captured_at TIMESTAMP NOT NULL, json_dump JSON NOT NULL
);
CREATE INDEX IF NOT EXISTS message_revisions_message ON message_revisions(message_id);`

const mediaFailureScope = ":media-failures"

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
	MigrationTo       int64
}

type store struct {
	db       *sql.DB
	dryRun   bool
	existing bool
}

type storedPeer struct {
	ID, AccessHash int64
	Flags          int
	Type, Selector string
	Title          string
}

type storedMedia struct {
	URL, Source string
}

func openStore(path string, dryRun bool) (*store, error) {
	_, statErr := os.Stat(path)
	existing := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	dsn := path
	if dryRun {
		if existing {
			absolute, err := filepath.Abs(path)
			if err != nil {
				return nil, err
			}
			dsn = (&url.URL{Scheme: "file", Path: filepath.ToSlash(absolute), RawQuery: "mode=ro"}).String()
		} else {
			dsn = ":memory:"
		}
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &store{db: db, dryRun: dryRun, existing: existing}, nil
}

func (s *store) prepare(peer peerResult, accountID int64, bootstrap bool, migratedTo int64) (bool, error) {
	if s.dryRun {
		if !s.existing {
			return true, nil
		}
		return s.checkBinding(peer, accountID)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.Exec(schema); err != nil {
		return false, err
	}
	for _, column := range [][3]string{
		{"messages", "json_dump", "JSON"},
		{"archive_metadata", "account_id", "INTEGER"},
		{"archive_metadata", "migrated_to", "INTEGER"},
		{"archive_peer_cache", "peer_flags", "INTEGER NOT NULL DEFAULT -1"},
		{"media_failures", "queue_order", "INTEGER NOT NULL DEFAULT 0"},
	} {
		if err = ensureColumn(tx, column[0], column[1], column[2]); err != nil {
			return false, err
		}
	}
	var pinnedID, pinnedAccount sql.NullInt64
	var pinnedType string
	err = tx.QueryRow("SELECT peer_id, peer_type, account_id FROM archive_metadata WHERE id = 1").Scan(&pinnedID, &pinnedType, &pinnedAccount)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	if pinnedID.Valid {
		if pinnedID.Int64 != peer.ID || pinnedType != peer.Type {
			return false, fmt.Errorf("database is pinned to %s %d, resolved %s %d", pinnedType, pinnedID.Int64, peer.Type, peer.ID)
		}
		if pinnedAccount.Valid && pinnedAccount.Int64 != accountID {
			return false, fmt.Errorf("database is pinned to Telegram account %d, logged in as %d", pinnedAccount.Int64, accountID)
		}
		if !pinnedAccount.Valid {
			count, _, err := archiveStats(tx)
			if err != nil {
				return false, err
			}
			if count > 0 && !bootstrap {
				return false, fmt.Errorf("existing database requires --bootstrap-peer to bind Telegram account %d", accountID)
			}
			if _, err = tx.Exec("UPDATE archive_metadata SET account_id = ? WHERE id = 1", accountID); err != nil {
				return false, err
			}
		}
	} else {
		count, last, err := archiveStats(tx)
		if err != nil {
			return false, err
		}
		if count > 0 && !bootstrap {
			return false, fmt.Errorf("existing unpinned database requires --bootstrap-peer")
		}
		if _, err = tx.Exec("INSERT INTO archive_metadata(id, peer_id, peer_type, account_id) VALUES(1, ?, ?, ?)", peer.ID, peer.Type, accountID); err != nil {
			return false, err
		}
		if count == 0 {
			last = 0
		}
		if _, err = tx.Exec("INSERT OR IGNORE INTO sync_state(scope,last_seen_id) VALUES(?,?)", baseScope(peer), last); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec("INSERT OR IGNORE INTO sync_state(scope,last_seen_id) VALUES(?,0)", baseScope(peer)); err != nil {
		return false, err
	}
	if _, err = tx.Exec(`INSERT INTO sync_state(scope,last_seen_id) VALUES(?,(SELECT COALESCE(MAX(queue_order),0) FROM media_failures))
ON CONFLICT(scope) DO UPDATE SET last_seen_id=MAX(sync_state.last_seen_id,excluded.last_seen_id)`, mediaFailureScope); err != nil {
		return false, err
	}
	if _, err = tx.Exec(`UPDATE archive_metadata SET migrated_to=COALESCE(
		migrated_to,NULLIF(?,0),
		(SELECT NULLIF(CAST(content AS INTEGER),0) FROM messages WHERE type='migrated_to' ORDER BY id DESC LIMIT 1)
	) WHERE id=1`, migratedTo); err != nil {
		return false, err
	}
	if err = savePeerCache(tx, peer); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	s.existing = true
	return false, nil
}

func (s *store) checkBinding(peer peerResult, accountID int64) (bool, error) {
	var peerID int64
	var peerType string
	err := s.db.QueryRow("SELECT peer_id, peer_type FROM archive_metadata WHERE id = 1").Scan(&peerID, &peerType)
	if err != nil {
		if err == sql.ErrNoRows || strings.Contains(err.Error(), "no such table") {
			return true, nil
		}
		return false, err
	}
	if peerID != peer.ID || peerType != peer.Type {
		return false, fmt.Errorf("database is pinned to %s %d, resolved %s %d", peerType, peerID, peer.Type, peer.ID)
	}
	var storedAccount sql.NullInt64
	err = s.db.QueryRow("SELECT account_id FROM archive_metadata WHERE id = 1").Scan(&storedAccount)
	if err != nil && strings.Contains(err.Error(), "no such column") {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if storedAccount.Valid && storedAccount.Int64 != accountID {
		return false, fmt.Errorf("database is pinned to Telegram account %d, logged in as %d", storedAccount.Int64, accountID)
	}
	return !storedAccount.Valid, nil
}

func ensureColumn(tx *sql.Tx, table, name, definition string) error {
	rows, err := tx.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var column, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &column, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		if column == name {
			return rows.Close()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	_, err = tx.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, name, definition))
	return err
}

func archiveStats(tx *sql.Tx) (count, last int, err error) {
	err = tx.QueryRow("SELECT COUNT(*), COALESCE(MAX(id), 0) FROM messages").Scan(&count, &last)
	return
}

func savePeerCache(tx *sql.Tx, peer peerResult) error {
	_, err := tx.Exec(`INSERT INTO archive_peer_cache(id, peer_selector, peer_title, peer_access_hash, peer_flags) VALUES(1, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET peer_selector=excluded.peer_selector, peer_title=excluded.peer_title, peer_access_hash=excluded.peer_access_hash, peer_flags=excluded.peer_flags`, peer.Selector, peer.Title, peer.AccessHash, peer.Flags)
	return err
}

func (s *store) pinnedPeer() (*storedPeer, error) {
	if !s.existing {
		return nil, nil
	}
	peer := &storedPeer{}
	err := s.db.QueryRow(`SELECT m.peer_id, m.peer_type, c.peer_selector, c.peer_title, c.peer_access_hash, c.peer_flags
FROM archive_metadata m JOIN archive_peer_cache c ON c.id = m.id WHERE m.id = 1`).Scan(&peer.ID, &peer.Type, &peer.Selector, &peer.Title, &peer.AccessHash, &peer.Flags)
	if err == sql.ErrNoRows || err != nil && (strings.Contains(err.Error(), "no such table") || strings.Contains(err.Error(), "no such column")) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if peer.Flags < 0 || peer.AccessHash == 0 && peer.Type != "chat" && (peer.Type != "user" || peer.Flags&peerSelf == 0) {
		return nil, nil
	}
	return peer, nil
}

func baseScope(peer peerResult) string {
	return strconv.FormatInt(constant.TDLibPeerID(peer.ID).ToPlain(), 10)
}

func (s *store) cursor(scope string) (int, error) {
	var id int
	err := s.db.QueryRow("SELECT last_seen_id FROM sync_state WHERE scope = ?", scope).Scan(&id)
	if err == sql.ErrNoRows || err != nil && strings.Contains(err.Error(), "no such table") {
		return 0, nil
	}
	return id, err
}

func (s *store) save(messages []archiveMessage, missing []int, scope string, cursor *int) error {
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
		var mediaID, mediaType, mediaURL, mediaTitle, mediaDescription, mediaThumb any
		if m.MediaAction == mediaReplace && m.Media != nil {
			mediaID, mediaType = m.Media.ID, m.Media.Type
			mediaURL, mediaTitle = nullString(m.Media.URL), nullString(m.Media.Title)
			mediaDescription, mediaThumb = nullString(m.Media.Description), nullString(m.Media.Thumb)
		}
		updateMedia := m.MediaAction == mediaReplace || m.MediaAction == mediaClear
		if _, err = tx.Exec(`INSERT INTO message_revisions(message_id,captured_at,json_dump)
SELECT messages.id,CURRENT_TIMESTAMP,messages.json_dump FROM messages
LEFT JOIN media ON media.id=messages.media_id
WHERE messages.id=? AND messages.json_dump IS NOT NULL AND messages.json_dump IS NOT ? AND (
	messages.type IS NOT ? OR messages.edit_date IS NOT ? OR messages.content IS NOT ? OR messages.reply_to IS NOT ? OR
	? AND (messages.media_id IS NOT ? OR media.type IS NOT ? OR media.url IS NOT ? OR
		media.title IS NOT ? OR media.description IS NOT ? OR media.thumb IS NOT ?)
)`, m.ID, m.JSON, m.Type, formatOptionalTime(m.EditDate), m.Content, m.ReplyTo,
			updateMedia, mediaID, mediaType, mediaURL, mediaTitle, mediaDescription, mediaThumb); err != nil {
			return fmt.Errorf("save revision %d: %w", m.ID, err)
		}
		if mediaID != nil {
			if _, err = tx.Exec(`INSERT INTO media(id,type,url,title,description,thumb) VALUES(?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET type=excluded.type,url=excluded.url,title=excluded.title,description=excluded.description,thumb=excluded.thumb`,
				mediaID, mediaType, mediaURL, mediaTitle, mediaDescription, mediaThumb); err != nil {
				return fmt.Errorf("save media %d: %w", m.Media.ID, err)
			}
		}
		if _, err = tx.Exec(`INSERT INTO messages(id,type,date,edit_date,content,reply_to,user_id,media_id,json_dump) VALUES(?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET type=excluded.type,date=excluded.date,edit_date=excluded.edit_date,
content=excluded.content,reply_to=excluded.reply_to,user_id=excluded.user_id,
media_id=CASE WHEN ? THEN excluded.media_id ELSE messages.media_id END,json_dump=excluded.json_dump`,
			m.ID, m.Type, formatTime(m.Date), formatOptionalTime(m.EditDate), m.Content, m.ReplyTo, m.User.ID, mediaID, m.JSON, updateMedia); err != nil {
			return fmt.Errorf("save message %d: %w", m.ID, err)
		}
		if _, err = tx.Exec("DELETE FROM message_tombstones WHERE message_id = ?", m.ID); err != nil {
			return err
		}
		if m.MigrationTo != 0 {
			if _, err = tx.Exec("UPDATE archive_metadata SET migrated_to=COALESCE(migrated_to,?) WHERE id=1", m.MigrationTo); err != nil {
				return err
			}
		}
		if m.MediaFailure != nil {
			var order int
			if err = tx.QueryRow(`INSERT INTO sync_state(scope,last_seen_id) VALUES(?,1)
ON CONFLICT(scope) DO UPDATE SET last_seen_id=sync_state.last_seen_id+1
RETURNING last_seen_id`, mediaFailureScope).Scan(&order); err != nil {
				return err
			}
			_, err = tx.Exec(`INSERT INTO media_failures(message_id,attempts,last_error,queue_order)
	VALUES(?,?,?,?)
ON CONFLICT(message_id) DO UPDATE SET attempts=media_failures.attempts+excluded.attempts,
	last_error=excluded.last_error,queue_order=excluded.queue_order`, m.ID, m.MediaFailure.Attempts, m.MediaFailure.Error, order)
		} else if m.ClearMediaFailure {
			_, err = tx.Exec("DELETE FROM media_failures WHERE message_id = ?", m.ID)
		}
		if err != nil {
			return fmt.Errorf("update media failure %d: %w", m.ID, err)
		}
	}
	for _, id := range missing {
		if _, err = tx.Exec(`INSERT INTO message_tombstones(message_id,detected_at) VALUES(?,CURRENT_TIMESTAMP)
ON CONFLICT(message_id) DO NOTHING`, id); err != nil {
			return fmt.Errorf("tombstone message %d: %w", id, err)
		}
	}
	if cursor != nil {
		if _, err = tx.Exec(`INSERT INTO sync_state(scope,last_seen_id) VALUES(?,?)
ON CONFLICT(scope) DO UPDATE SET last_seen_id=MAX(sync_state.last_seen_id,excluded.last_seen_id)`, scope, *cursor); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *store) pendingMediaFailures(until, limit int) ([]int, error) {
	rows, err := s.db.Query(`SELECT f.message_id FROM media_failures f
WHERE NOT EXISTS (
	SELECT 1 FROM message_tombstones t WHERE t.message_id=f.message_id
	) AND f.queue_order<=? ORDER BY f.queue_order,f.message_id LIMIT ?`, until, limit)
	if err != nil {
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

func (s *store) messageMedia(ids []int) (map[int]storedMedia, error) {
	result := map[int]storedMedia{}
	for start := 0; start < len(ids); start += 500 {
		end := min(start+500, len(ids))
		args := make([]any, end-start)
		for i, id := range ids[start:end] {
			args[i] = id
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")
		rows, err := s.db.Query(`SELECT messages.id,COALESCE(media.url,''),COALESCE(messages.json_dump,'')
FROM messages JOIN media ON media.id=messages.media_id WHERE messages.id IN (`+placeholders+")", args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int
			var url, raw string
			if err := rows.Scan(&id, &url, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			result[id] = storedMedia{URL: url, Source: storedSourceKey(raw)}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *store) mediaFailureCounts() (pending, unavailable int, err error) {
	err = s.db.QueryRow(`SELECT
	COALESCE(SUM(CASE WHEN t.message_id IS NULL THEN 1 ELSE 0 END),0),
	COALESCE(SUM(CASE WHEN t.message_id IS NOT NULL THEN 1 ELSE 0 END),0)
FROM media_failures f LEFT JOIN message_tombstones t ON t.message_id=f.message_id`).Scan(&pending, &unavailable)
	return
}

func (s *store) reconcileLimit(scope string) (int, error) {
	until, err := s.cursor(scope + ":until")
	if err != nil || until != 0 {
		return until, err
	}
	if err = s.db.QueryRow("SELECT COALESCE(MAX(id),0) FROM messages").Scan(&until); err != nil || until == 0 || s.dryRun {
		return until, err
	}
	_, err = s.db.Exec("INSERT OR IGNORE INTO sync_state(scope,last_seen_id) VALUES(?,?)", scope+":until", until)
	return until, err
}

func (s *store) messageIDsAfter(id, until, limit int) ([]int, error) {
	return s.messageIDs("SELECT id FROM messages WHERE id>? AND id<=? ORDER BY id LIMIT ?", id, until, limit)
}

func (s *store) messageIDs(query string, args ...any) ([]int, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
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

func (s *store) finishReconcile(scope string) error {
	if s.dryRun {
		return nil
	}
	_, err := s.db.Exec("DELETE FROM sync_state WHERE scope IN (?,?)", scope, scope+":until")
	return err
}

func (s *store) migrationTarget() (int64, error) {
	var target sql.NullInt64
	err := s.db.QueryRow("SELECT migrated_to FROM archive_metadata WHERE id=1").Scan(&target)
	if err == nil && target.Valid {
		return target.Int64, nil
	}
	if err != nil && err != sql.ErrNoRows && !strings.Contains(err.Error(), "no such table") && !strings.Contains(err.Error(), "no such column") {
		return 0, err
	}
	err = s.db.QueryRow("SELECT NULLIF(CAST(content AS INTEGER),0) FROM messages WHERE type='migrated_to' ORDER BY id DESC LIMIT 1").Scan(&target)
	if err == sql.ErrNoRows || err != nil && strings.Contains(err.Error(), "no such table") {
		return 0, nil
	}
	return target.Int64, err
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
