# P6K OCR1 PaddleOCR Default

## Purpose

P6K-OCR1 switches the `uns-stream` local OCR default for `pdf` and `image` partitioning from
Tesseract-oriented behavior to Unstructured-supported PaddleOCR behavior, while preserving explicit
Tesseract override support and leaving non-OCR partition kinds unchanged.

This is a Zephyr-dev core hardening step. It does not modify Zephyr-base, Web-core, Pro, Site, or
commercial logic.

## What Changed

- `packages/uns-stream` now exposes a `paddleocr` optional extra through
  `unstructured[paddleocr]`.
- Local `pdf` and `image` partition calls now default-inject PaddleOCR through:
  - `ocr_agent = unstructured.partition.utils.ocr_models.paddle_ocr.OCRAgentPaddle`
  - `table_ocr_agent = unstructured.partition.utils.ocr_models.paddle_ocr.OCRAgentPaddle`
- Explicit caller overrides still win through `setdefault` behavior.
- `tesseract` remains a supported explicit override alias.
- New enhanced profiles were added:
  - `zh_paddle`
  - `invoice_paddle`
- Friendly dash aliases are accepted:
  - `zh-paddle`
  - `invoice-paddle`
- CLI flags were added:
  - `--ocr-agent`
  - `--table-ocr-agent`

## Why PaddleOCR Is Default For PDF/Image

PaddleOCR is a better default fit for the OCR-heavy PDF/Image slice targeted by current Zephyr-dev
partition hardening:

- better Chinese-oriented OCR coverage than the old default path
- aligned with Unstructured-supported OCR agent routing
- compatible with the existing P6K-M1 enhanced partition profile work

The default only applies to `pdf` and `image`. It does not affect:

- `text`
- `md`
- `html`
- `docx`
- `xlsx`
- `csv`
- other non-PDF/Image partition kinds

## Tesseract Fallback

Tesseract remains explicitly supported.

Accepted aliases:

- `paddle`
- `tesseract`

Qualified Unstructured OCR agent names are also accepted when they match the expected
`unstructured.partition.utils.ocr_models.*` namespace.

## Google Vision

Google Vision is not added in OCR1.

Reasons:

- it is not required for the current core-default switch
- it would widen dependency and external-service surface unnecessarily
- OCR1 is a local-core defaultization step, not a cloud OCR expansion step

## Runtime Safety

Non-PDF/Image kinds do not receive OCR agent injection.

The local backend now also ensures a writable PaddleOCR cache root before invoking Paddle-backed
OCR. If neither `ZEPHYR_PADDLE_OCR_BASE_DIR` nor `PADDLE_OCR_BASE_DIR` is set, Zephyr uses:

- `.tmp/paddleocr`

This avoids hard dependence on `~/.paddleocr` being writable in constrained environments.

For Windows runtimes that expose a Paddle/Torch import-order conflict, Zephyr now preloads
`torch` before PaddleOCR runtime initialization, but only on the actual `pdf` / `image` +
PaddleOCR path. Explicit Tesseract calls and non-OCR kinds do not trigger that preload.

## Install Paths

Ordinary CI remains lightweight and does not use `--all-extras`.

Ordinary CI install:

```powershell
uv sync --locked --all-groups --all-packages
```

Full local capability install:

```powershell
make install-all
make install-full
make install-dev-full
```

OCR-focused local install:

```powershell
make install-ocr
```

Equivalent direct command:

```powershell
uv sync --locked --all-groups --all-packages --extra paddleocr
```

## Benchmark

Paddle vs Tesseract comparison tool:

- `tools/p6k_ocr1_paddle_vs_tesseract_benchmark.py`

Planned local sample set:

- `E:\Github_Projects\Zephyr-zh-test\fapiao.jpeg`
- `E:\Github_Projects\Zephyr-zh-test\fapiao3.jpg`
- `E:\Github_Projects\Zephyr-zh-test\hetong2-jiashuiyin.pdf`

Planned output root:

- `E:\Github_Projects\Zephyr-zh-test\tmp`

Example:

```powershell
uv run --locked --no-sync python tools/p6k_ocr1_paddle_vs_tesseract_benchmark.py --input-dir "E:\Github_Projects\Zephyr-zh-test" --out-dir "E:\Github_Projects\Zephyr-zh-test\tmp" --profile invoice_paddle --json
```

## Current Validation State

Repository-side implementation, lockfile update, CLI/profile wiring, and focused tests are in
place.

Current environment findings after `P6K-OCR1-FIX`:

- `unstructured-paddleocr==2.10.0` is installed and the runtime diagnosis now correctly classifies
  the original blocker as a Windows Paddle/Torch import-order conflict
- `torch` preload was added before PaddleOCR runtime initialization on the Zephyr `pdf` / `image`
  Paddle path
- the real local Paddle vs Tesseract benchmark now completes against:
  - `fapiao.jpeg`
  - `fapiao3.jpg`
  - `hetong2-jiashuiyin.pdf`
- benchmark outcome in this local environment:
  - default PaddleOCR: `0/3` ok, `3/3` error
  - explicit Tesseract: `3/3` ok, `0/3` error
- the import-order failure is no longer the only blocker; a remaining local Paddle runtime failure
  still affects real partition execution on all three planned samples

Current judgment:

- implementation complete for OCR defaultization and Windows torch-preload hardening
- repository quality gates pass
- real Paddle runtime evidence now exists
- after `P6K-OCR1-FIX2`, the default OCR user path is product-safe in this environment because
  known Paddle runtime failures now fall back to explicit Tesseract
- current local benchmark outcome:
  - default path final ok: `3/3`
  - Paddle native ok: `0/3`
  - Paddle fallback ok: `3/3`
  - explicit Tesseract ok: `3/3`
- OCR1 final status therefore becomes `risk_reduced_pass`, not full native-Paddle pass
