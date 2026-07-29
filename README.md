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

Use an existing [`tg-archive`](https://github.com/knadh/tg-archive) `config.yaml`. Authentication, session storage, proxy settings, and connection pooling come from tdl; `api_id`, `api_hash`, and `proxy` in the tg-archive config are ignored.

Raw Telegram JSON is disabled by default. Add `json_dump: true` to the config or pass `--json-dump` to populate the nullable `messages.json_dump` column.

```sh
# Continue after the saved cursor (or the greatest existing message ID).
tdl archive sync

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
```

Options:

- `--config`: config path, default `config.yaml`
- `--data`: SQLite path, default `data.sqlite`
- `--id`: exact IDs; repeat, comma-separate, or space-separate them
- `--from-id`: inclusive lower ID
- `--type id|time|last` with `--input`
- `--topic` or `--reply`: mutually exclusive thread roots
- `--filter`: a [tdl expression](https://docs.iyear.me/tdl/guide/expr/)
- `--json-dump`: store raw Telegram JSON

Primary selectors are mutually exclusive. Explicit selectors upsert every selected message and atomically replace existing media. They do not move the normal incremental cursor.

The extension writes the existing `messages`, `users`, and `media` tables without changing their public shape. Its only private table is `sync_state`. Existing databases missing `messages.json_dump` are migrated automatically; the column stays null unless enabled.

Profile-avatar downloading and Telegram deletion reconciliation are not included yet. Existing avatar paths are preserved when users are updated.
