package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/telegram"
	gotddownloader "github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/iyear/tdl/core/dcpool"
	coredownloader "github.com/iyear/tdl/core/downloader"
	coretakeout "github.com/iyear/tdl/core/middlewares/takeout"
	"github.com/iyear/tdl/core/tclient"
	"github.com/iyear/tdl/core/tmedia"
	"github.com/iyear/tdl/extension"
)

type mediaMapper struct {
	ctx        context.Context
	ext        *extension.Extension
	cfg        config
	peerID     int64
	pool       dcpool.Pool
	takeoutID  int64
	existing   map[int]storedMedia
	downloadFn func(*tmedia.Media, string) error
}

type mediaEvent int

const (
	mediaNone mediaEvent = iota
	mediaDownloaded
	mediaReused
	mediaSkipped
	mediaFailed
)

var mediaRetryDelay = time.Second

func (m *mediaMapper) close() {
	if m.pool != nil {
		_ = m.pool.Close()
	}
}

func (m *mediaMapper) message(elem messages.Elem) (archiveMessage, mediaEvent, error) {
	result, rawMedia, err := baseMessage(elem.Msg, elem.Entities)
	if err != nil {
		return result, mediaNone, err
	}
	if sticker := stickerText(rawMedia); sticker != "" {
		result.Content = sticker
	}
	var event mediaEvent
	result.Media, result.MediaAction, result.MediaFailure, result.ClearMediaFailure, event, err = m.media(result.ID, elem.Msg, rawMedia)
	return result, event, err
}

func (m *mediaMapper) media(id int, msg tg.NotEmptyMessage, raw tg.MessageMediaClass) (*archiveMedia, mediaAction, *mediaFailure, bool, mediaEvent, error) {
	switch value := raw.(type) {
	case *tg.MessageMediaPoll:
		counts := map[string]tg.PollAnswerVoters{}
		for _, result := range value.Results.Results {
			counts[string(result.Option)] = result
		}
		options := make([]map[string]any, 0, len(value.Poll.Answers))
		for _, rawAnswer := range value.Poll.Answers {
			answer, ok := rawAnswer.(*tg.PollAnswer)
			if !ok {
				continue
			}
			result := counts[string(answer.Option)]
			percent := float64(0)
			if value.Results.TotalVoters > 0 {
				percent = float64(result.Voters) / float64(value.Results.TotalVoters) * 100
			}
			options = append(options, map[string]any{"label": answer.Text.Text, "count": result.Voters, "correct": result.Correct, "percent": percent})
		}
		description, _ := json.Marshal(options)
		return &archiveMedia{ID: id, Type: "poll", Title: value.Poll.Question.Text, Description: string(description)}, mediaReplace, nil, true, mediaNone, nil
	case *tg.MessageMediaWebPage:
		if page, ok := value.Webpage.(*tg.WebPage); ok {
			return &archiveMedia{ID: id, Type: "webpage", URL: page.URL, Title: page.Title, Description: page.Description}, mediaReplace, nil, true, mediaNone, nil
		}
	}
	normal, ok := msg.(*tg.Message)
	if !ok {
		return nil, mediaClear, nil, true, mediaNone, nil
	}
	file, ok := tmedia.GetMedia(normal)
	if !ok {
		return nil, mediaClear, nil, true, mediaNone, nil
	}

	key := sourceMediaKey(file.InputFileLoc)
	ext := filepath.Ext(filepath.Base(file.Name))
	if ext == "" || len(ext) > 6 {
		ext = ".file"
	}
	name := fmt.Sprintf("%d-%s-%d%s", id, key, file.Size, strings.ToLower(ext))
	url := filepath.ToSlash(filepath.Join(strconv.FormatInt(m.peerID, 10), name))
	path := filepath.Join(m.cfg.MediaDir, filepath.FromSlash(url))
	stored := m.existing[id]

	if m.cfg.DownloadMedia && !m.allowed(raw) {
		return nil, mediaClear, nil, true, mediaSkipped, nil
	}
	if validFile(path, file.Size) {
		if m.cfg.DownloadMedia {
			if err := chmodValidFile(path, file.Size); err != nil {
				return nil, mediaKeep, nil, false, mediaNone, err
			}
		}
		return mediaRecord(id, raw, file.Name, url), mediaReplace, nil, true, mediaReused, nil
	}
	if !m.cfg.DownloadMedia {
		if stored.URL == url || legacyMedia(id, key, stored) {
			return nil, mediaKeep, nil, false, mediaSkipped, nil
		}
		return nil, mediaClear, nil, false, mediaSkipped, nil
	}
	if adopted, err := m.adoptLegacy(id, key, stored, path, file.Size); err != nil {
		return nil, mediaKeep, nil, false, mediaNone, err
	} else if adopted {
		return mediaRecord(id, raw, file.Name, url), mediaReplace, nil, true, mediaReused, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, mediaKeep, nil, false, mediaNone, err
	}
	download := m.download
	if m.downloadFn != nil {
		download = m.downloadFn
	}
	for attempt := 1; attempt <= 3; attempt++ {
		err := download(file, path)
		if err == nil && validFile(path, file.Size) {
			if err := chmodValidFile(path, file.Size); err != nil {
				return nil, mediaKeep, nil, false, mediaNone, err
			}
			return mediaRecord(id, raw, file.Name, url), mediaReplace, nil, true, mediaDownloaded, nil
		}
		if err == nil {
			err = fmt.Errorf("downloaded size does not match Telegram metadata")
		}
		err = fmt.Errorf("peer %d media %d to %s: %w", m.peerID, id, path, err)
		if fatalMediaError(err) {
			return nil, mediaKeep, nil, false, mediaNone, err
		}
		if attempt == 3 {
			return nil, mediaClear, &mediaFailure{Attempts: attempt, Error: err.Error()}, false, mediaFailed, nil
		}
		if err := waitContext(m.ctx, time.Duration(attempt)*mediaRetryDelay); err != nil {
			return nil, mediaKeep, nil, false, mediaNone, err
		}
	}
	panic("unreachable")
}

func mediaRecord(id int, raw tg.MessageMediaClass, original, url string) *archiveMedia {
	media := &archiveMedia{ID: id, Type: "photo", URL: url, Title: filepath.Base(original)}
	if _, ok := raw.(*tg.MessageMediaPhoto); ok {
		media.Thumb = url
	}
	return media
}

func sourceMediaKey(location tg.InputFileLocationClass) string {
	switch value := location.(type) {
	case *tg.InputPhotoFileLocation:
		return "p" + strconv.FormatInt(value.ID, 10)
	case *tg.InputDocumentFileLocation:
		return "d" + strconv.FormatInt(value.ID, 10)
	}
	return "unknown"
}

func validFile(path string, size int64) bool {
	file, err := openValidFile(path, size)
	if err != nil {
		return false
	}
	return file.Close() == nil
}

func openValidFile(path string, size int64) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() != size {
		return nil, &os.PathError{Op: "validate", Path: path, Err: errors.New("not a regular file of the expected size")}
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !after.Mode().IsRegular() || after.Size() != size {
		_ = file.Close()
		if err != nil {
			return nil, err
		}
		return nil, &os.PathError{Op: "validate", Path: path, Err: errors.New("file changed during validation")}
	}
	return file, nil
}

func chmodValidFile(path string, size int64) error {
	file, err := openValidFile(path, size)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Chmod(0o644)
}

func storedSourceKey(raw string) string {
	var message struct {
		Media struct {
			Photo struct {
				ID int64 `json:"id"`
			} `json:"photo"`
			Document struct {
				ID int64 `json:"id"`
			} `json:"document"`
		} `json:"media"`
	}
	if json.Unmarshal([]byte(raw), &message) != nil {
		return ""
	}
	if message.Media.Photo.ID != 0 {
		return "p" + strconv.FormatInt(message.Media.Photo.ID, 10)
	}
	if message.Media.Document.ID != 0 {
		return "d" + strconv.FormatInt(message.Media.Document.ID, 10)
	}
	return ""
}

func (m *mediaMapper) allowed(raw tg.MessageMediaClass) bool {
	if len(m.cfg.MediaMIMETypes) == 0 {
		return true
	}
	mime := "image/jpeg"
	if value, ok := raw.(*tg.MessageMediaDocument); ok {
		if doc, ok := value.Document.(*tg.Document); ok {
			mime = doc.MimeType
		}
	}
	for _, allowed := range m.cfg.MediaMIMETypes {
		if mime == allowed {
			return true
		}
	}
	return false
}

func legacyMedia(id int, key string, stored storedMedia) bool {
	return stored.URL != "" && !strings.ContainsAny(stored.URL, `/\\`) && strings.HasPrefix(stored.URL, strconv.Itoa(id)+".") && (stored.Source == "" || stored.Source == key)
}

func (m *mediaMapper) adoptLegacy(id int, key string, stored storedMedia, path string, size int64) (bool, error) {
	if !legacyMedia(id, key, stored) {
		return false, nil
	}
	legacy := filepath.Join(m.cfg.MediaDir, stored.URL)
	if !validFile(legacy, size) {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.Link(legacy, path); err == nil {
		linked, err := openValidFile(path, size)
		if err == nil {
			err = linked.Chmod(0o644)
		}
		if err == nil {
			err = linked.Sync()
		}
		if linked != nil {
			if closeErr := linked.Close(); err == nil {
				err = closeErr
			}
		}
		if err != nil {
			_ = os.Remove(path)
			return false, err
		}
		if err == nil {
			err = syncDirectory(filepath.Dir(path))
		}
		if err != nil {
			_ = os.Remove(path)
			return false, err
		}
		return true, nil
	}
	source, err := os.Open(legacy)
	if err != nil {
		return false, err
	}
	defer func() { _ = source.Close() }()
	return true, publishFile(path, size, func(target *os.File) error {
		_, err := io.Copy(target, source)
		return err
	})
}

func (m *mediaMapper) download(file *tmedia.Media, path string) error {
	if m.pool == nil {
		m.pool = dcpool.NewPool(m.ext.Client(), m.ext.Config().Pool, mediaMiddlewares(m.ctx, m.takeoutID)...)
	}
	client := m.pool.Client(m.ctx, file.DC)
	return publishFile(path, file.Size, func(target *os.File) error {
		_, err := gotddownloader.NewDownloader().WithPartSize(coredownloader.MaxPartSize).Download(client, file.InputFileLoc).WithThreads(4).WithVerify(true).Parallel(m.ctx, target)
		return err
	})
}

func publishFile(path string, size int64, write func(*os.File) error) error {
	if validFile(path, size) {
		return chmodValidFile(path, size)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := write(tmp); err != nil {
		return err
	}
	if info, err := tmp.Stat(); err != nil {
		return err
	} else if info.Size() != size {
		return fmt.Errorf("downloaded size does not match Telegram metadata")
	}
	if err := tmp.Chmod(0o644); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func mediaMiddlewares(ctx context.Context, takeoutID int64) []telegram.Middleware {
	middlewares := append(tclient.NewDefaultMiddlewares(ctx, 0), floodWaitMiddlewares()...)
	if takeoutID != 0 {
		middlewares = append(middlewares, coretakeout.Middleware(takeoutID))
	}
	return middlewares
}

func fatalMediaError(err error) bool {
	var pathError *os.PathError
	var linkError *os.LinkError
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &pathError) || errors.As(err, &linkError) || tgerr.IsCode(err, 420)
}

func stickerText(raw tg.MessageMediaClass) string {
	media, ok := raw.(*tg.MessageMediaDocument)
	if !ok {
		return ""
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok || doc.MimeType != "application/x-tgsticker" {
		return ""
	}
	for _, attribute := range doc.Attributes {
		if sticker, ok := attribute.(*tg.DocumentAttributeSticker); ok {
			return sticker.Alt
		}
	}
	return ""
}
