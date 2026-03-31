# Benchmarking (P2-M6-05 B5)

`zephyr-ingest bench` runs multiple iterations of the ingest pipeline and prints a JSON report.

This is intended as a **local, repeatable performance baseline** for:
- throughput regressions
- tuning `workers`, backend choice, and destination choices
- comparing changes across commits (with the same inputs)

---

## 1) Quick start

Benchmark a directory (local backend, filesystem-only destination by default):

```bash
zephyr-ingest bench \
  --path ./samples \
  --glob "**/*" \
  --iterations 5 \
  --warmup 1 \
  --bench-out .cache/bench
```

Use Unstructured API backend:

```bash
zephyr-ingest bench \
  --path ./samples \
  --glob "**/*" \
  --backend uns-api \
  --uns-api-url http://localhost:8001/general/v0/general \
  --iterations 5 \
  --warmup 1
```

---

## 2) What bench does (important)

- Runs `warmup + iterations` total runs.
- **Warmup runs are not included** in summary stats.
- Each run writes artifacts and a `batch_report.json` under:
- `<bench_out>/<session_dir>/iter_<i>/`
- Bench reads `batch_report.json.metrics` fields:
- `run_wall_ms`
- `docs_per_min`

---

## 3) Output JSON shape (high-level)

Bench prints JSON to stdout:

- `bench_root`: the bench output root
- `session_dir`: the unique session directory created for this bench run
- `results`: per-iteration list (including warmup entries)
- `summary`: aggregated stats for measured iterations only

---

## 4) Best practices for stable benchmarks

1) **Keep inputs constant**
- use a fixed directory of test docs
- avoid including large random downloads or changing files

2) **Avoid external destinations unless you are testing them**
- webhook/kafka/weaviate can dominate runtime due to network latency
- prefer filesystem-only when benchmarking partition throughput

3) **Always include warmup**
- first run is often slower due to imports, caches, and JIT-like effects in dependencies

4) Control concurrency explicitly
- set `--workers N` and keep it constant across comparisons

5) Beware of machine noise
- close heavy apps
- consider pinning CPU frequency / using a quiet machine for regressions

---

## 5) Common pitfalls

- If you re-use the same out_root, `--skip-existing` can skip work and produce misleading results.
- Bench avoids this by using a unique out_root per iteration.

- If `docs_per_min` is `null`, the measured wall time may be zero (extremely short runs).
- Increase the input set size or iterations.
