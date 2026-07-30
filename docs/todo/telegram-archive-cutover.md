# Telegram archive cutover implementation plan

## Source constraints

- `mediaMapper.message` currently swallows incremental download errors, while `synchronizer.sync` maps a fetched batch before calling `store.save`.
- `openStore` currently creates/migrates tables before Telegram resolves the configured peer.
- `run` treats config and native CLI modes as exclusive; flag defaults cannot be distinguished from explicit overrides.
- `--fetch-limit` is currently passed only to ordinary incremental collection.
- `extension.New` exits before `run` on bootstrap/authentication errors, so those failures cannot be emitted by this extension as structured JSON.

## Work order

### 1. Parse effective options and initialize results

Files: `main.go`, `config.go`, new `result.go`.

- Parse into a CLI options type and record explicitly visited flags with `FlagSet.Visit`.
- Detect `--json` before normal flag parsing so malformed arguments still produce structured handler-level failures.
- Load YAML as defaults unless `--chat` alone selects native mode, then overlay only visited flags, including explicit `false` values.
- Resolve YAML media paths relative to the config; resolve CLI media paths relative to the working directory; validate only after merging.
- Keep raw JSON config-driven and support explicit `--json-dump=false` overrides.
- Add `--bootstrap-peer` and `--json`.
- Have each phase return small typed values; compose the final run result only in `run`.

### 2. Defer database mutation and pin identity

Files: `store.go`, `sync.go`.

- Resolve Telegram first, deriving title, canonical peer type, and namespace-qualified `TDLibPeerID`.
- Open databases without schema mutation, inspect identity, then prepare schema only after validation.
- Add private `archive_metadata` with a single peer ID/type binding.
- Auto-bind a new database; require `--bootstrap-peer` for any existing unpinned database; reject mismatches before schema, message, or media writes.
- Keep dry-run read-only: report a pending bootstrap without persisting it.
- Keep the existing sync-scope format; database pinning makes cross-peer scope collisions impossible.

### 3. Add autonomous media recovery

Files: `media.go`, `store.go`, `sync.go`.

- Attempt each download once plus two retries with short context-aware backoff; keep temporary-file cleanup and atomic rename.
- Treat local storage/path errors as fatal. Treat exhausted per-message Telegram/network errors as recoverable.
- Add private `media_failures(message_id, attempts, last_error)`; the error contains the attempted destination path.
- Preserve existing media links when downloading is disabled, excluded, or recoverably failed; replace on success and clear only when Telegram no longer exposes eligible media.
- Commit each mapped batch's messages, recoverable failures, successful-failure removals, and cursor in one transaction. A failed refresh must preserve any existing good `media_id`.
- Retry pending failures at the start of later media-enabled runs via the existing exact-ID fetch path; clear each record atomically after success.
- If a successful exact-ID lookup shows that the message is unavailable or its media is no longer eligible, retire the failure with a warning; transport/query errors leave it queued.
- Track downloaded, reused, skipped, failed, and pending counts plus failed message IDs.

### 4. Make selection limits and sync accounting consistent

Files: `sync.go`, `takeout.go`, `selection.go`.

- Return a sync result rather than only an integer count.
- Keep `--fetch-limit` on incremental sync and reject it when combined with an explicit selector instead of adding separate limit behavior to every collector.
- Preserve batch checkpoints and the legacy `MAX(messages.id)` fallback.
- Capture `dialog_top_message_id` during existing dialog resolution; expose it as whole-dialog context, not as a scoped topic/filter cursor, and omit it when unavailable.
- Ensure media retry work is reported separately from newly selected/saved messages.

### 5. Emit stable output

Files: `main.go`, `result.go`.

- Human output remains the default.
- `--json` emits one versioned object containing paths, selector, resolved peer, starting/ending cursors, optional dialog-top ID, selected/saved counts, media statistics, raw-JSON policy, duration, warnings, error, and status.
- Use `success`, `completed_with_warnings`, or `failed`, with a separate dry-run field; recoverable queued media failures exit successfully, while fatal failures remain non-zero after JSON is emitted and the extension wrapper returns.
- Cover all errors after the extension handler starts. The fleet runner will normalize pre-handler `tdl` bootstrap/authentication failures into its JSONL result; do not duplicate `tdl`'s private lifecycle code here.

### 6. Document and verify

Files: `README.md`, focused `*_test.go` files.

- Document config precedence, raw-JSON default/opt-out, peer bootstrap, retry behavior, statuses, and the JSON schema.
- Add deterministic tests for explicit flag overlays, fetch bounds, legacy cursors, identity bootstrap/mismatch, dry-run immutability, retry success/exhaustion, atomic failure persistence/clearance, fatal storage errors, preserved media links, takeout limits, and success/warning/failure JSON.
- Run `mise run test`, `mise run lint`, and `mise run build`.

## Outside this repository

- Run a one-time cutover migration that verifies legacy JSON coverage and writes `json_dump: true` into all 161 configs; do not duplicate this policy with runtime database detection or a new global default.
- Build the fleet runner for discovery, locking, mount/storage checks, timeouts, rollout batches, and JSONL normalization/reruns.
- Deploy only after the Restic checkpoint and bounded dry/real canaries succeed.
