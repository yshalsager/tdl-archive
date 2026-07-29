package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/iyear/tdl/extension"
)

func main() {
	extension.New(extension.Options{})(func(ctx context.Context, ext *extension.Extension) error {
		return run(ctx, ext, os.Args[1:])
	})
}

func run(ctx context.Context, ext *extension.Extension, args []string) error {
	if len(args) == 0 || args[0] != "sync" {
		return fmt.Errorf("usage: tdl archive sync [options]")
	}
	flags := flag.NewFlagSet("sync", flag.ContinueOnError)
	configPath := flags.String("config", "config.yaml", "tgarchive config path")
	dataPath := flags.String("data", "data.sqlite", "tgarchive SQLite database path")
	chat := flags.String("chat", "", "chat ID, username, or title (uses native CLI configuration)")
	downloadMedia := flags.Bool("download-media", false, "download attached media")
	mediaDir := flags.String("media-dir", "", "media directory (default: media)")
	mediaTypes := flags.String("media-type", "", "comma-separated MIME types to download")
	fetchBatchSize := flags.Int("fetch-batch-size", 100, "messages to process per checkpoint")
	fetchLimit := flags.Int("fetch-limit", 0, "maximum messages to sync; zero means unlimited")
	useTakeout := flags.Bool("takeout", false, "use Telegram's takeout API for bulk exports")
	dryRun := flags.Bool("dry-run", false, "show how many messages would be synced without writing")
	jsonDump := flags.Bool("json-dump", false, "store raw Telegram JSON")
	sel := selection{}
	flags.Var(&sel.IDs, "id", "message ID (repeat or comma-separate)")
	flags.IntVar(&sel.FromID, "from-id", 0, "first message ID, inclusive")
	flags.StringVar(&sel.Type, "type", "", "tdl selector: id, time, or last")
	flags.Var(&sel.Input, "input", "selector input (comma-separated)")
	flags.IntVar(&sel.Topic, "topic", 0, "forum topic root message ID")
	flags.IntVar(&sel.Reply, "reply", 0, "reply thread root message ID")
	flags.StringVar(&sel.Filter, "filter", "", "tdl expression filter")
	if err := flags.Parse(normalizeListFlags(args[1:])); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if err := sel.validate(); err != nil {
		return err
	}
	if *fetchBatchSize < 1 {
		return fmt.Errorf("--fetch-batch-size must be positive")
	}
	if *fetchLimit < 0 {
		return fmt.Errorf("--fetch-limit must be non-negative")
	}
	var cfg config
	var err error
	if *chat == "" {
		if *downloadMedia || *mediaDir != "" || *mediaTypes != "" || *fetchBatchSize != 100 || *fetchLimit != 0 || *useTakeout {
			return fmt.Errorf("native sync options require --chat")
		}
		cfg, err = loadConfig(*configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
	} else {
		cfg = config{Group: *chat, MediaDir: "media", DownloadMedia: *downloadMedia, FetchBatchSize: *fetchBatchSize, FetchLimit: *fetchLimit, UseTakeout: *useTakeout}
		if *mediaDir != "" {
			cfg.MediaDir = *mediaDir
		}
		for _, mime := range strings.Split(*mediaTypes, ",") {
			if mime = strings.TrimSpace(mime); mime != "" {
				cfg.MediaMIMETypes = append(cfg.MediaMIMETypes, mime)
			}
		}
	}
	if *jsonDump {
		cfg.JSONDump = true
	}
	if *dryRun {
		cfg.DownloadMedia = false
	}
	store, err := openStore(*dataPath, cfg.JSONDump, *dryRun)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = store.db.Close() }()
	count, err := (&synchronizer{ext: ext, cfg: cfg, store: store}).run(ctx, sel)
	if err == nil {
		if *dryRun {
			fmt.Printf("would sync %d messages\n", count)
		} else {
			fmt.Printf("synced %d messages\n", count)
		}
	}
	return err
}

func normalizeListFlags(args []string) []string {
	result := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		result = append(result, args[i])
		if args[i] != "--id" && args[i] != "--input" {
			continue
		}
		values := []string{}
		for i+1 < len(args) && len(args[i+1]) > 0 && args[i+1][0] != '-' {
			i++
			values = append(values, args[i])
		}
		if len(values) > 0 {
			result = append(result, strings.Join(values, ","))
		}
	}
	return result
}
