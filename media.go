package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	gotddownloader "github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/dcpool"
	coredownloader "github.com/iyear/tdl/core/downloader"
	"github.com/iyear/tdl/core/tclient"
	"github.com/iyear/tdl/core/tmedia"
	"github.com/iyear/tdl/extension"
)

type mediaMapper struct {
	ctx       context.Context
	ext       *extension.Extension
	cfg       config
	overwrite bool
	pool      dcpool.Pool
}

func (m *mediaMapper) close() {
	if m.pool != nil {
		_ = m.pool.Close()
	}
}

func (m *mediaMapper) message(elem messages.Elem) (archiveMessage, error) {
	result, rawMedia, err := baseMessage(elem.Msg, elem.Entities, m.cfg.JSONDump)
	if err != nil {
		return result, err
	}
	if sticker := stickerText(rawMedia); sticker != "" {
		result.Content = sticker
	}
	result.Media, err = m.media(result.ID, elem.Msg, rawMedia)
	if err != nil {
		if m.overwrite {
			return result, err
		}
		m.ext.Log().Warn(fmt.Sprintf("media %d: %v", result.ID, err))
		result.Media = nil
	}
	return result, nil
}

func (m *mediaMapper) media(id int, msg tg.NotEmptyMessage, raw tg.MessageMediaClass) (*archiveMedia, error) {
	switch value := raw.(type) {
	case *tg.MessageMediaPoll:
		if len(value.Results.Results) == 0 {
			return nil, nil
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
		return &archiveMedia{ID: id, Type: "poll", Title: value.Poll.Question.Text, Description: string(description)}, nil
	case *tg.MessageMediaWebPage:
		if page, ok := value.Webpage.(*tg.WebPage); ok {
			return &archiveMedia{ID: id, Type: "webpage", URL: page.URL, Title: page.Title, Description: page.Description}, nil
		}
	}
	if !m.cfg.DownloadMedia {
		return nil, nil
	}
	normal, ok := msg.(*tg.Message)
	if !ok {
		return nil, nil
	}
	file, ok := tmedia.GetMedia(normal)
	if !ok || !m.allowed(raw) {
		return nil, nil
	}
	if err := os.MkdirAll(m.cfg.MediaDir, 0o755); err != nil {
		return nil, err
	}
	ext := filepath.Ext(filepath.Base(file.Name))
	if ext == "" || len(ext) > 6 {
		ext = ".file"
	}
	name := fmt.Sprintf("%d%s", id, strings.ToLower(ext))
	path := filepath.Join(m.cfg.MediaDir, name)
	if m.overwrite || !fileExists(path) {
		if err := m.download(file, path); err != nil {
			return nil, fmt.Errorf("download media %d: %w", id, err)
		}
	}
	media := &archiveMedia{ID: id, Type: "photo", URL: name, Title: filepath.Base(file.Name)}
	if _, ok := raw.(*tg.MessageMediaPhoto); ok {
		media.Thumb = name
	}
	return media, nil
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
		m.pool = dcpool.NewPool(m.ext.Client(), m.ext.Config().Pool, tclient.NewDefaultMiddlewares(m.ctx, 0)...)
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	client := m.pool.Client(m.ctx, file.DC)
	if m.cfg.UseTakeout {
		client = m.pool.Takeout(m.ctx, file.DC)
	}
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
