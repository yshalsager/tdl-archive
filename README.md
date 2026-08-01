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

Use native options or an existing [`tg-archive`](https://github.com/knadh/tg-archive) `config.yaml`. Explicit CLI options override config values. Authentication, sessions, proxies, and connection pooling always come from `tdl`; Telegram credentials in the tg-archive config are ignored.

Every archived message stores its raw Telegram JSON. This is an archive invariant, not an optional mode.

```sh
# Native configuration; no config.yaml is needed.
tdl archive sync --chat @group
tdl archive sync --chat @group --download-media --media-dir media
tdl archive sync --chat @group --download-media --media-type image/jpeg,video/mp4
tdl archive sync --chat @group --fetch-batch-size 500 --takeout

# tg-archive compatibility configuration.
tdl archive sync --config config.yaml
tdl archive sync --config config.yaml --fetch-limit 100

# Continue from the durable whole-chat cursor.
tdl archive sync

# Refresh all archived messages, record edits, and record missing messages.
tdl archive sync --reconcile --fetch-wait 1

# Preview the selected history without writing or downloading media.
tdl archive sync --dry-run

# Refresh exact messages without moving the normal cursor.
tdl archive sync --id 120 121 140
tdl archive sync --from-id 120

# tdl-native selectors.
tdl archive sync --type id --input 120 140
tdl archive sync --type time --input 1753747200 1753833600
tdl archive sync --type last --input 100

# Scope and filter modifiers.
tdl archive sync --topic 42 --type last --input 100
tdl archive sync --reply 42 --filter 'Media.Size > 0'

# Bind a verified legacy database, then repair old projections.
tdl archive sync --config config.yaml --bootstrap-peer --reconcile

# Emit one machine-readable result.
tdl archive sync --config config.yaml --json
```

Options:

- `--config`: config path, default `config.yaml`
- `--data`: SQLite path, default `data.sqlite`
- `--chat`: `@username`, bare username, unambiguous dialog title, or canonical TDLib peer ID
- `--download-media`: override attached-media downloading
- `--media-dir`: override the media directory
- `--media-type`: override the comma-separated MIME filter
- `--fetch-batch-size`: messages per database checkpoint and maintenance batch; Telegram requests remain capped at `100`
- `--fetch-limit`: maximum incremental messages; explicit selectors reject a nonzero limit
- `--fetch-wait`: seconds between full history or reconciliation batches
- `--takeout`: use Telegram's takeout API
- `--dry-run`: preview primary selection without writes, media, retries, or reconciliation
- `--reconcile`: continue a complete, checkpointed archive reconciliation
- `--id`: exact IDs; repeat, comma-separate, or space-separate them
- `--from-id`: inclusive lower ID
- `--type id|time|last` with `--input`
- `--topic` or `--reply`: mutually exclusive thread roots
- `--filter`: a [tdl expression](https://docs.iyear.me/tdl/guide/expr/)
- `--bootstrap-peer`: bind a legacy database to the resolved account and peer
- `--json`: emit a versioned JSON result

Primary selectors are mutually exclusive. Explicit selectors upsert their selected messages but never move the whole-chat cursor.

## Archive invariants

Each database is bound to both the authenticated Telegram account and one canonical peer. A later account or peer mismatch fails before archive writes. New databases bind automatically. A nonempty legacy database requires one explicit `--bootstrap-peer` run. An unpinned tg-archive database seeds its whole-chat cursor from its greatest existing message ID exactly once. A database pinned by an older plugin preserves its durable whole-chat cursor while adding the account binding; if that cursor is absent, synchronization starts from zero. Run the upgrade with `--reconcile` to refresh legacy projections and detect missing messages. After that, cursors come only from `sync_state` and are never inferred from archived rows.

The pinned peer cache is used before Telegram resolution. On first binding, `@username` and syntactically valid bare usernames resolve directly. Numeric selectors are canonical TDLib peer IDs; they resolve directly when Telegram already knows the access hash and otherwise use one binding-time dialog scan. Only remaining title selectors scan dialogs and must be unambiguous. Responses owned by another peer are rejected. Cross-peer reply targets, including linked-discussion links, are omitted because tg-archive's message-ID-only schema cannot represent them safely.

A basic group migration is persisted as a hard archive boundary. The old basic-group archive may continue fetching and reconciling its pre-migration history; once an ordinary history sync reaches Telegram's end, it fails with the migrated supergroup's canonical ID. Continue that supergroup in a separate database.

`--reconcile` imports new history first, then continues a checkpointed oldest-to-newest pass until the complete archive has been checked. Changes to archived message content preserve the previous raw payload in `message_revisions`. Missing messages receive a durable `message_tombstones` record while their archived content remains intact. Schedule it periodically; forward message IDs alone cannot reveal Telegram edits or messages that disappear.

## Media durability

Media objects are immutable and peer-namespaced. Their paths contain the message ID, Telegram media ID, and expected size. A legacy object attached to the same archived message is adopted locally when its size and, when available, stored Telegram media ID match. Published files are mode `0644`, size-checked, synced, and renamed from a unique temporary file after Telegram hash verification. Run `--reconcile` after enabling downloads or widening the MIME filter to backfill older messages.

Use absolute `--data` and `--media-dir` paths for unattended services. Run one writer per database and media directory; the archive is sequential by design. Back up the SQLite database and its media directory together.

Recoverable media downloads receive up to three attempts. Exhausted network failures are committed with the message and cursor, then retried in bounded fair batches after new messages have been archived. A message already attempted by primary sync or reconciliation is not retried again in the same run. Missing retry sources become durable unavailable records rather than disappearing. Local filesystem errors, cancellation, deadlines, and excessive flood waits remain fatal.

Takeout mode never silently falls back to ordinary history. Telegram may require approving an export request or waiting for its reported delay.

## Results

`--json` writes one versioned object to stdout. Fatal errors use `status: "failed"` and a nonzero exit code. Pending or unavailable media uses `status: "completed_with_warnings"`.

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
  "reconciled": 100,
  "missing": 1,
  "media": {"downloaded": 2, "reused": 1, "skipped": 0, "failed": 0, "pending": 0, "unavailable": 0},
  "json_dump": true,
  "duration_ms": 1234
}
```

The public `messages`, `users`, and `media` shape remains compatible with tg-archive. Private state lives in `sync_state`, `archive_metadata`, `archive_peer_cache`, `media_failures`, `message_revisions`, and `message_tombstones`. Existing compatible databases are migrated transactionally.

Profile-avatar downloading is not included. Existing avatar paths are preserved when users are refreshed.
