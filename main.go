package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/iyear/tdl/extension"
)

type commandOptions struct {
	ConfigPath    string
	DataPath      string
	Config        config
	Selection     selection
	BootstrapPeer bool
	DryRun        bool
	Reconcile     bool
	JSON          bool
}

func main() {
	jsonMode := wantsJSON(os.Args[1:])
	exitCode := 0
	extension.New(extension.Options{Middlewares: floodWaitMiddlewares()})(func(ctx context.Context, ext *extension.Extension) error {
		err := run(ctx, ext, os.Args[1:])
		if jsonMode && err != nil {
			exitCode = 1
			return nil
		}
		return err
	})
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func run(ctx context.Context, ext *extension.Extension, args []string) error {
	started := time.Now()
	result := runResult{Version: 1, Status: "failed", ConfigPath: "config.yaml", DataPath: "data.sqlite"}
	options, err := parseOptions(args)
	if options != nil {
		result.ConfigPath, result.DataPath = options.ConfigPath, options.DataPath
		result.DryRun, result.JSONDump = options.DryRun, true
	}
	if err == nil {
		var store *store
		store, err = openStore(options.DataPath, options.DryRun)
		if err == nil {
			defer func() { _ = store.db.Close() }()
			syncer := &synchronizer{ext: ext, cfg: options.Config, store: store, bootstrapPeer: options.BootstrapPeer, dryRun: options.DryRun, reconcile: options.Reconcile}
			var synced syncResult
			synced, err = syncer.run(ctx, options.Selection)
			if syncer.peer.ID != 0 {
				result.Peer = &syncer.peer
			}
			result.DialogTopMessageID = syncer.dialogTopMessageID
			result.StartingCursor, result.EndingCursor = synced.StartingCursor, synced.EndingCursor
			result.Selected, result.Saved, result.Reconciled, result.Missing, result.Media = synced.Selected, synced.Saved, synced.Reconciled, synced.Missing, synced.Media
			result.Warnings = synced.Warnings
			if !options.JSON {
				if err == nil {
					switch {
					case options.DryRun:
						fmt.Printf("would sync %d messages\n", synced.Selected)
					case options.Reconcile:
						fmt.Printf("synced %d messages; reconciled %d (%d missing)\n", synced.Saved, synced.Reconciled, synced.Missing)
					default:
						fmt.Printf("synced %d messages\n", synced.Saved)
					}
				}
				for _, warning := range synced.Warnings {
					fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
				}
			}
		}
	}
	result.finish(started, err)
	if wantsJSON(args) {
		if outputErr := writeResult(os.Stdout, result); err == nil {
			err = outputErr
		}
	}
	return err
}

func parseOptions(args []string) (*commandOptions, error) {
	options := &commandOptions{ConfigPath: "config.yaml", DataPath: "data.sqlite", JSON: wantsJSON(args)}
	if len(args) == 0 || args[0] != "sync" {
		return options, fmt.Errorf("usage: tdl archive sync [options]")
	}
	flags := flag.NewFlagSet("sync", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", options.ConfigPath, "tgarchive config path")
	dataPath := flags.String("data", options.DataPath, "tgarchive SQLite database path")
	chat := flags.String("chat", "", "@username, bare username, title, or TDLib peer ID")
	downloadMedia := flags.Bool("download-media", false, "download attached media")
	mediaDir := flags.String("media-dir", "", "media directory")
	mediaTypes := flags.String("media-type", "", "comma-separated MIME types to download")
	fetchBatchSize := flags.Int("fetch-batch-size", 100, "messages to process per checkpoint")
	fetchWait := flags.Int("fetch-wait", 0, "seconds to wait between full batches")
	fetchLimit := flags.Int("fetch-limit", 0, "maximum messages to sync")
	useTakeout := flags.Bool("takeout", false, "use Telegram's takeout API")
	dryRun := flags.Bool("dry-run", false, "preview without writing")
	reconcile := flags.Bool("reconcile", false, "refresh archived messages and record missing messages")
	bootstrapPeer := flags.Bool("bootstrap-peer", false, "bind a legacy database to the resolved peer")
	jsonOutput := flags.Bool("json", false, "emit a machine-readable result")
	sel := selection{}
	flags.Var(&sel.IDs, "id", "message ID")
	flags.IntVar(&sel.FromID, "from-id", 0, "first message ID, inclusive")
	flags.StringVar(&sel.Type, "type", "", "tdl selector: id, time, or last")
	flags.Var(&sel.Input, "input", "selector input")
	flags.IntVar(&sel.Topic, "topic", 0, "forum topic root message ID")
	flags.IntVar(&sel.Reply, "reply", 0, "reply thread root message ID")
	flags.StringVar(&sel.Filter, "filter", "", "tdl expression filter")
	if err := flags.Parse(normalizeListFlags(args[1:])); err != nil {
		return options, err
	}
	if flags.NArg() > 0 {
		return options, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	visited := map[string]bool{}
	flags.Visit(func(value *flag.Flag) { visited[value.Name] = true })
	options.ConfigPath, options.DataPath = *configPath, *dataPath
	cfg := config{MediaDir: "media", FetchBatchSize: 100}
	if *chat == "" || visited["config"] {
		var err error
		cfg, err = loadConfig(*configPath)
		if err != nil {
			return options, fmt.Errorf("load config: %w", err)
		}
	} else {
		options.ConfigPath = ""
	}
	if visited["chat"] {
		cfg.Group = *chat
	}
	if visited["download-media"] {
		cfg.DownloadMedia = *downloadMedia
	}
	if visited["media-dir"] {
		cfg.MediaDir = *mediaDir
	}
	if visited["media-type"] {
		cfg.MediaMIMETypes = splitList(*mediaTypes)
	}
	if visited["fetch-batch-size"] {
		cfg.FetchBatchSize = *fetchBatchSize
	}
	if visited["fetch-wait"] {
		cfg.FetchWait = *fetchWait
	}
	if visited["fetch-limit"] {
		cfg.FetchLimit = *fetchLimit
	}
	if visited["takeout"] {
		cfg.UseTakeout = *useTakeout
	}
	if err := sel.validate(); err != nil {
		return options, err
	}
	if err := cfg.validate(); err != nil {
		return options, err
	}
	if sel.Explicit && cfg.FetchLimit > 0 {
		return options, fmt.Errorf("--fetch-limit cannot be combined with an explicit selector")
	}
	if *dryRun {
		cfg.DownloadMedia = false
	}
	options.Config, options.Selection = cfg, sel
	options.BootstrapPeer, options.DryRun, options.Reconcile, options.JSON = *bootstrapPeer, *dryRun, *reconcile, *jsonOutput
	return options, nil
}

func wantsJSON(args []string) bool {
	value := false
	for _, arg := range args {
		if arg == "--json" {
			value = true
		} else if strings.HasPrefix(arg, "--json=") {
			parsed, err := strconv.ParseBool(strings.TrimPrefix(arg, "--json="))
			if err == nil {
				value = parsed
			}
		}
	}
	return value
}

func splitList(raw string) []string {
	result := []string{}
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
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
