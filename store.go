package main

import (
	"database/sql"
	"fmt"
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
);`

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
	ID       int
	Type     string
	Date     time.Time
	EditDate *time.Time
	Content  string
	ReplyTo  *int
	User     archiveUser
	Media    *archiveMedia
	JSON     string
}

type store struct {
	db       *sql.DB
	jsonDump bool
}

func openStore(path string, jsonDump bool) (*store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	s := &store{db: db, jsonDump: jsonDump}
	if err := s.ensureJSONDump(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *store) ensureJSONDump() error {
	rows, err := s.db.Query("PRAGMA table_info(messages)")
	if err != nil {
		return err
	}
	defer rows.Close()
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
	_, err = s.db.Exec("ALTER TABLE messages ADD COLUMN json_dump JSON")
	return err
}

func (s *store) cursor(scope string, useExistingMessages bool) (int, error) {
	var id int
	err := s.db.QueryRow("SELECT last_seen_id FROM sync_state WHERE scope = ?", scope).Scan(&id)
	if err == sql.ErrNoRows && useExistingMessages {
		err = s.db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM messages").Scan(&id)
	} else if err == sql.ErrNoRows {
		err = nil
	}
	return id, err
}

func (s *store) save(messages []archiveMessage, scope string, cursor *int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, m := range messages {
		if _, err = tx.Exec(`INSERT INTO users(id, username, first_name, last_name, tags, avatar) VALUES(?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name,
last_name=excluded.last_name, tags=excluded.tags, avatar=COALESCE(excluded.avatar,users.avatar)`, m.User.ID, m.User.Username, m.User.First, m.User.Last, strings.Join(m.User.Tags, " "), nullString(m.User.Avatar)); err != nil {
			return fmt.Errorf("save user %d: %w", m.User.ID, err)
		}
		var mediaID any
		if m.Media != nil {
			mediaID = m.Media.ID
			if _, err = tx.Exec(`INSERT INTO media(id,type,url,title,description,thumb) VALUES(?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET type=excluded.type,url=excluded.url,title=excluded.title,description=excluded.description,thumb=excluded.thumb`,
				m.Media.ID, m.Media.Type, nullString(m.Media.URL), nullString(m.Media.Title), nullString(m.Media.Description), nullString(m.Media.Thumb)); err != nil {
				return fmt.Errorf("save media %d: %w", m.Media.ID, err)
			}
		}
		query := `INSERT INTO messages(id,type,date,edit_date,content,reply_to,user_id,media_id) VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET type=excluded.type,date=excluded.date,edit_date=excluded.edit_date,
content=excluded.content,reply_to=excluded.reply_to,user_id=excluded.user_id,media_id=excluded.media_id`
		args := []any{m.ID, m.Type, formatTime(m.Date), formatOptionalTime(m.EditDate), m.Content, m.ReplyTo, m.User.ID, mediaID}
		if s.jsonDump {
			query = `INSERT INTO messages(id,type,date,edit_date,content,reply_to,user_id,media_id,json_dump) VALUES(?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET type=excluded.type,date=excluded.date,edit_date=excluded.edit_date,
content=excluded.content,reply_to=excluded.reply_to,user_id=excluded.user_id,media_id=excluded.media_id,json_dump=excluded.json_dump`
			args = append(args, m.JSON)
		}
		if _, err = tx.Exec(query, args...); err != nil {
			return fmt.Errorf("save message %d: %w", m.ID, err)
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
