# P6K OCR1 FIX2 Language Fallback

## Purpose

`P6K-OCR1-FIX2` hardens the Zephyr-dev default OCR user path after the torch-preload fix.

The problem after `P6K-OCR1-FIX` was no longer import order. The remaining issue was real Paddle
runtime execution on Windows:

- Paddle native path attempted real OCR
- all three planned local samples failed
- explicit Tesseract still passed on the same samples

The goal of FIX2 is product-safe default behavior:

- Paddle-first
- bounded language normalization
- known-failure Tesseract fallback
- no effect on non-PDF/Image kinds

## Root Cause Before FIX2

The focused runtime failure diagnostic captured the remaining Paddle path failure against:

- `fapiao.jpeg`
- `fapiao3.jpg`
- `hetong2-jiashuiyin.pdf`

Observed behavior:

- Zephyr profile languages resolved to `["zho", "eng"]`
- Zephyr now normalizes Paddle path languages to `["ch"]`
- Paddle native OCR still fails in this local Windows environment with a real runtime execution
  error inside `unstructured_paddleocr`
- the captured traceback tail ends in Paddle detector execution with:
  - `NotFoundError: OneDnnContext does not have the input Filter`
  - `operator < fused_conv2d > error`

So the remaining blocker was classified as:

- `paddle_runtime_execution_or_language_mapping_failure`

## Language Normalization

Zephyr now adds a bounded PaddleOCR language normalizer:

- `zho`, `zh`, `chi_sim`, `zh-cn`, `zh-hans` and similar simplified-Chinese aliases normalize to
  `["ch"]`
- traditional-Chinese aliases normalize to `["chinese_cht"]`
- English aliases normalize to `["en"]`
- mixed Chinese + English Paddle cases prefer Chinese
- unknown values fall back safely to `["en"]`

Important boundary:

- no Tesseract-style combined language strings are sent into Paddle
- no change is applied to explicit Tesseract paths
- no change is applied to non-PDF/Image kinds

## Safe Fallback

When all of the following are true:

- kind is `pdf` or `image`
- resolved OCR path is PaddleOCR
- the Paddle attempt fails with a known Paddle runtime failure signature

Zephyr now retries once with explicit Tesseract agents:

- `ocr_agent = OCRAgentTesseract`
- `table_ocr_agent = OCRAgentTesseract`

This fallback is not silent. Zephyr records structured runtime notes that are surfaced into
partition warnings and benchmark diagnostics:

- Paddle language normalization before/after
- Paddle original error type/message/traceback tail
- whether fallback was applied
- fallback target agents
- fallback success/failure

## Real Benchmark Result

Real local benchmark result after FIX2:

- default path final ok: `3/3`
- Paddle native ok: `0/3`
- Paddle fallback ok: `3/3`
- explicit Tesseract ok: `3/3`

Per current OCR1 status rules, that means:

- `P6K-OCR1-FIX2 = risk_reduced_pass`
- `P6K-OCR1 final status after FIX2 = risk_reduced_pass`

The default OCR user path is now product-safe in this environment even though Paddle native
execution is still not clean.

## Boundary

FIX2 preserves:

- `unstructured==0.22.28`
- explicit Tesseract support
- Paddle default support
- ordinary CI without `--all-extras`
- Base unchanged
- commercial logic unchanged
