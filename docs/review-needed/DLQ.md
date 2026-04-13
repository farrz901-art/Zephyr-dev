# Delivery DLQ (Dead Letter Queue)

Zephyr-ingest treats "delivery" as a governed stage:

1) Partition produces artifacts + run_meta.
2) Destinations deliver a stable payload (DeliveryPayloadV1).
3) Delivery failures are written to DLQ so they can be replayed and pruned safely.

This doc covers the DLQ directory layout and the DLQ maintenance commands.

---

## 1) Directory layout

All paths below are under `<out_root>` (default: `.cache/out`).

- Pending delivery failures (needs replay):
- `_dlq/delivery/*.json`

- Replay succeeded (safe to prune aggressively):
- `_dlq/delivery_done/*.json`

- Pruned (moved by `dlq prune`):
- `_dlq/delivery_pruned/delivery/*.json`
- `_dlq/delivery_pruned/delivery_done/*.json`

We split pruned files by origin (`delivery/` vs `delivery_done/`) to avoid filename collisions and preserve provenance.

---

## 2) Replay delivery DLQ

Replay sends the same DeliveryPayloadV1 shape used by real destinations.

Example:

```bash
zephyr-ingest replay-delivery \
  --out .cache/out \
  --webhook-url http://localhost:9000/ingest \
  --webhook-timeout-s 10 \
  --move-done
```

Operational contract:
- Replay logs stable events: `replay_start`, `replay_attempt`, `replay_result`, `replay_done`.
- Successful replays can be moved into `_dlq/delivery_done/`.

---

## 3) Prune DLQ (retention / disk governance)

The prune command **moves** records (it does not delete by default).
It supports:
- age-based selection (required): `--older-than-days N`
- optional size cap: `--max-total-mb N`
- optional protection window: `--keep-last N` (global or per destination)

### 3.1 Safe defaults

By default:
- prune is **dry-run** (no file moves)
- only `_dlq/delivery_done/` is pruned (pending is preserved)

### 3.2 Examples

Dry-run prune "done" DLQ older than 30 days:

```bash
zephyr-ingest dlq prune --out .cache/out --older-than-days 30
```

Apply the same prune (actually move files):

```bash
zephyr-ingest dlq prune --out .cache/out --older-than-days 30 --apply
```

Include pending DLQ too (use with care):

```bash
zephyr-ingest dlq prune --out .cache/out --older-than-days 30 --include-pending --apply
```

Protect newest 50 records per destination:

```bash
zephyr-ingest dlq prune --out .cache/out --older-than-days 30 --keep-last 50 --keep-last-per-destination
```

Enforce a hard disk cap (may prune newer records to meet the cap):

```bash
zephyr-ingest dlq prune --out .cache/out --older-than-days 30 --max-total-mb 512 --apply
```

### 3.3 Output stats (JSON)

`dlq prune` prints JSON. Key fields:

- `scanned`: total dlq json files scanned
- `selected`: total files selected for pruning
- `moved`: total files moved (0 in dry-run)
- `protected`: number of files protected by `--keep-last`

Selection breakdown:
- `selected_by_age`: selected due to `older-than-days`
- `selected_by_size`: additional selected to satisfy `max-total-mb`
- `bytes_over_limit`: when size cap is active, how many bytes must be freed

Origin breakdown:
- `selected_pending`, `selected_done`
- `moved_pending`, `moved_done`

---

## 4) Operational recommendation

1) Always start with:
- prune `delivery_done/` only
- dry-run first

2) For production, a pragmatic baseline:
- `--older-than-days 30`
- `--keep-last 50 --keep-last-per-destination`
- optionally `--max-total-mb` as a hard cap

3) If `selected_by_size > 0`, it means size cap forced selection of newer records:
- increase cap or keep_last
- or accept that the oldest-first policy is working as designed.
