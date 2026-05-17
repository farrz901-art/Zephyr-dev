# P6K OCR1 FIX Runtime Preload

## Purpose

`P6K-OCR1-FIX` is a narrow Windows runtime remediation for the Zephyr-dev PaddleOCR default path.
It does not change `unstructured==0.22.28`, does not modify Base, and does not widen ordinary CI.

## Confirmed Root Cause

The local Windows runtime proved a dependency-chain import-order conflict:

- `import paddle` succeeds
- `import torch` succeeds
- `import unstructured.partition.utils.ocr_models.paddle_ocr.OCRAgentPaddle` succeeds
- direct `import unstructured_paddleocr` fails
- `import paddle; import torch` fails with `WinError 127`
- `import torch; import paddle; import unstructured_paddleocr` succeeds

Zephyr therefore classified the issue as:

- `windows_paddle_torch_import_order_conflict`

This is a Windows runtime ordering problem in the PaddleOCR dependency chain, not a missing extra
and not a Zephyr wiring failure.

## Fix Strategy

Zephyr now preloads `torch` lazily before PaddleOCR runtime preparation when all of the following
are true:

- partition kind is `pdf` or `image`
- the resolved OCR path is PaddleOCR

The runtime order is now:

1. `preload_torch_for_paddleocr()`
2. `ensure_paddleocr_base_dir()`
3. invoke the underlying Unstructured partition function

This does not run for:

- explicit Tesseract calls
- non-PDF/Image kinds

## Diagnostics

Runtime diagnosis tool:

- `tools/p6k_ocr1_runtime_diag.py`

Validation artifact:

- `validation/p6k_ocr1_runtime_diag.json`

The diagnosis tool reports:

- Python/platform/runtime versions
- Paddle/Torch import probes
- `unstructured_paddleocr` import probes
- OCR agent import probe
- Zephyr Paddle base-dir facts
- diagnosis and recommended action

## Benchmark Status After Fix

The real local Paddle vs Tesseract benchmark now completes against the planned sample set:

- `fapiao.jpeg`
- `fapiao3.jpg`
- `hetong2-jiashuiyin.pdf`

Current local outcome:

- default PaddleOCR: `0/3` ok, `3/3` error
- explicit Tesseract: `3/3` ok, `0/3` error

This means the import-order blocker was real and is now explicitly mitigated, but a remaining
Paddle runtime execution issue still exists in this environment.

## Boundary

The fix preserves:

- `unstructured==0.22.28`
- explicit Tesseract fallback
- non-PDF/Image non-OCR behavior
- ordinary CI without `--all-extras`
- Base unchanged
- commercial logic unchanged
