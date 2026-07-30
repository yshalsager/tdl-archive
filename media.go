package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func (m *mediaMapper) message(elem messages.Elem, overwrite, repair bool) (archiveMessage, mediaEvent, error) {
	result, rawMedia, err := baseMessage(elem.Msg, elem.Entities, m.cfg.JSONDump)
	if err != nil {
		return result, mediaNone, err
	}
	if sticker := stickerText(rawMedia); sticker != "" {
		result.Content = sticker
	}
	var event mediaEvent
	result.Media, result.MediaAction, result.MediaFailure, result.ClearMediaFailure, event, err = m.media(result.ID, elem.Msg, rawMedia, overwrite, repair)
	return result, event, err
}

func (m *mediaMapper) media(id int, msg tg.NotEmptyMessage, raw tg.MessageMediaClass, overwrite, repair bool) (*archiveMedia, mediaAction, *mediaFailure, bool, mediaEvent, error) {
	switch value := raw.(type) {
	case *tg.MessageMediaPoll:
		if len(value.Results.Results) == 0 {
			return nil, mediaClear, nil, true, mediaNone, nil
		}
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
			result, found := counts[string(answer.Option)]
			count, correct := 0, false
			if found {
				count, correct = result.Voters, result.Correct
			}
			percent := float64(0)
			if value.Results.TotalVoters > 0 {
				percent = float64(count) / float64(value.Results.TotalVoters) * 100
			}
			options = append(options, map[string]any{"label": answer.Text.Text, "count": count, "correct": correct, "percent": percent})
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
	if !m.cfg.DownloadMedia || !m.allowed(raw) {
		return nil, mediaKeep, nil, repair, mediaSkipped, nil
	}
	if err := os.MkdirAll(m.cfg.MediaDir, 0o755); err != nil {
		return nil, mediaKeep, nil, false, mediaNone, err
	}
	ext := filepath.Ext(filepath.Base(file.Name))
	if ext == "" || len(ext) > 6 {
		ext = ".file"
	}
	name := fmt.Sprintf("%d%s", id, strings.ToLower(ext))
	path := filepath.Join(m.cfg.MediaDir, name)
	if !overwrite && fileExists(path) {
		return mediaRecord(id, raw, file.Name, name), mediaReplace, nil, true, mediaReused, nil
	}
	download := m.download
	if m.downloadFn != nil {
		download = m.downloadFn
	}
	for attempt := 1; attempt <= 3; attempt++ {
		err := download(file, path)
		if err == nil {
			return mediaRecord(id, raw, file.Name, name), mediaReplace, nil, true, mediaDownloaded, nil
		}
		err = fmt.Errorf("peer %d media %d to %s: %w", m.peerID, id, path, err)
		if fatalMediaError(err) {
			return nil, mediaKeep, nil, false, mediaNone, err
		}
		if attempt == 3 {
			return nil, mediaKeep, &mediaFailure{Attempts: attempt, Error: err.Error()}, false, mediaFailed, nil
		}
		timer := time.NewTimer(time.Duration(attempt) * mediaRetryDelay)
		select {
		case <-m.ctx.Done():
			timer.Stop()
			return nil, mediaKeep, nil, false, mediaNone, m.ctx.Err()
		case <-timer.C:
		}
	}
	panic("unreachable")
}

func mediaRecord(id int, raw tg.MessageMediaClass, original, name string) *archiveMedia {
	media := &archiveMedia{ID: id, Type: "photo", URL: name, Title: filepath.Base(original)}
	if _, ok := raw.(*tg.MessageMediaPhoto); ok {
		media.Thumb = name
	}
	return media
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

func (m *mediaMapper) download(file *tmedia.Media, path string) error {
	if m.pool == nil {
		m.pool = dcpool.NewPool(m.ext.Client(), m.ext.Config().Pool, mediaMiddlewares(m.ctx, m.takeoutID)...)
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	client := m.pool.Client(m.ctx, file.DC)
	_, err := gotddownloader.NewDownloader().WithPartSize(coredownloader.MaxPartSize).Download(client, file.InputFileLoc).WithThreads(4).ToPath(m.ctx, tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
	}
	return err
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
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &pathError) || tgerr.IsCode(err, 420)
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
