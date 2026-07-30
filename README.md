# tdl-archive

> [!CAUTION]
> **Disclaimer:** This project was developed with heavy use of AI assistance. Tested and verified by the author.

A [`tdl`](https://github.com/iyear/tdl) extension that syncs Telegram messages and attached media into the SQLite format consumed by [`tg-archive`](https://github.com/knadh/tg-archive).

## Install

```sh
tdl extension install yshalsager/tdl-archive
tdl extension list
```

The executable name is `tdl-archive`, so tdl exposes it as `tdl archive`.

## Development

Go and GoReleaser are pinned in `mise.toml`.

```sh
mise install
mise run test
mise run build
mise run release:snapshot
mise run release
```

`release:snapshot` builds all archives locally. `release` requires a clean tagged commit and publishes it to GitHub Releases.

## Sync

Sync directly with native CLI options, or use an existing [`tg-archive`](https://github.com/knadh/tg-archive) `config.yaml`. Explicit CLI options override loaded config values. Authentication, session storage, proxy settings, and connection pooling always come from tdl; `api_id`, `api_hash`, and `proxy` in the tg-archive config are ignored.

Raw Telegram JSON is disabled by default. Add `json_dump: true` to the config or pass `--json-dump` to populate the nullable `messages.json_dump` column.

```sh
# Native configuration; no config.yaml is needed.
tdl archive sync --chat @group
tdl archive sync --chat @group --download-media --media-dir media
tdl archive sync --chat @group --download-media --media-type image/jpeg,video/mp4
tdl archive sync --chat @group --fetch-batch-size 500 --takeout

# tg-archive compatibility configuration.
tdl archive sync --config config.yaml
tdl archive sync --config config.yaml --fetch-limit 100

# Continue after the saved cursor (or the greatest existing message ID).
tdl archive sync

# Preview the same selection without changing the database or downloading media.
tdl archive sync --dry-run

# Replace exact messages in the DB and replace their media files.
tdl archive sync --id 120 121 140

# Replace everything from an ID through the latest message.
tdl archive sync --from-id 120

# tdl-native selectors.
tdl archive sync --type id --input 120 140
tdl archive sync --type time --input 1753747200 1753833600
tdl archive sync --type last --input 100

# Scope and filter modifiers.
tdl archive sync --topic 42 --type last --input 100
tdl archive sync --reply 42 --filter 'Media.Size > 0'

# Bind an existing unpinned database after verifying the resolved peer.
tdl archive sync --config config.yaml --bootstrap-peer --fetch-limit 100

# Emit one machine-readable result.
tdl archive sync --config config.yaml --dry-run --json
```

Options:

- `--config`: config path, default `config.yaml`
- `--data`: SQLite path, default `data.sqlite`
- `--chat`: chat ID, username, or title; enables native CLI configuration without `config.yaml`
- `--download-media`: override attached-media downloading
- `--media-dir`: override the media directory
- `--media-type`: override the comma-separated MIME filter
- `--fetch-batch-size`: override messages processed per database checkpoint; Telegram requests remain capped at `100`
- `--fetch-limit`: maximum incremental messages to sync; zero means unlimited and explicit selectors reject a nonzero limit
- `--takeout`: override Telegram takeout mode
- `--dry-run`: report how many messages would be synced without writing the database or downloading media
- `--id`: exact IDs; repeat, comma-separate, or space-separate them
- `--from-id`: inclusive lower ID
- `--type id|time|last` with `--input`
- `--topic` or `--reply`: mutually exclusive thread roots
- `--filter`: a [tdl expression](https://docs.iyear.me/tdl/guide/expr/)
- `--json-dump`: store raw Telegram JSON
- `--bootstrap-peer`: bind an existing unpinned database to the resolved peer
- `--json`: emit a versioned JSON result

Primary selectors are mutually exclusive. Explicit selectors upsert every selected message and atomically replace existing media. They do not move the normal incremental cursor.

Takeout mode may require approving Telegram's export request or waiting for the delay reported by Telegram. The sync fails rather than silently falling back to standard mode.

Each database is pinned to its resolved Telegram peer. New databases bind automatically; existing unpinned databases require one verified `--bootstrap-peer` run. Dry-run reports the pending binding without writing it.

Media downloads receive three total attempts. Exhausted per-message failures are stored in a private retry queue while the message and cursor advance; later media-enabled runs retry them automatically. Filesystem, cancellation, and deadline errors remain fatal.

`--json` writes one versioned object to stdout. Fatal handler errors use `status: "failed"` and a non-zero exit code; queued media errors use `status: "completed_with_warnings"` and remain retryable.

```json
{
  "version": 1,
  "status": "success",
  "dry_run": false,
  "config_path": "config.yaml",
  "data_path": "data.sqlite",
  "peer": {"selector": "@group", "title": "Group", "type": "channel", "id": -1000000000123},
  "starting_cursor": 100,
  "ending_cursor": 110,
  "dialog_top_message_id": 110,
  "selected": 10,
  "saved": 10,
  "media": {"downloaded": 2, "reused": 1, "skipped": 0, "failed": 0, "pending": 0},
  "json_dump": true,
  "duration_ms": 1234
}
```

The extension writes the existing `messages`, `users`, and `media` tables without otherwise changing their public shape. Its private tables are `sync_state`, `archive_metadata`, and `media_failures`. Existing databases missing `messages.json_dump` are migrated automatically; the column stays null unless enabled.

Profile-avatar downloading and Telegram deletion reconciliation are not included yet. Existing avatar paths are preserved when users are updated.
