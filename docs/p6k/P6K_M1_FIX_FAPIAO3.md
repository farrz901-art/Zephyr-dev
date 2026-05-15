# P6K-M1-FIX: fapiao3 Benchmark Remediation

## Final status

- target sample: `E:\Github_Projects\Zephyr-zh-test\fapiao3.jpg`
- status after fix: `ok`
- full benchmark: `4/4 passed`
- product-grade strong-core status: `achieved`

## Diagnosis

The failing sample is named `fapiao3.jpg`, but the actual image payload is:

- format: `GIF`
- mode: `P`
- dimensions: `440x330`

Under the `invoice` profile, Zephyr enables image/table extraction payloads. On this sample,
`unstructured==0.22.28` reached the image-block extraction path and attempted to save a cropped
palette-mode image as JPEG. The upstream failure chain was:

1. Pillow raised `OSError: cannot write mode P as JPEG`
2. upstream image extraction then surfaced `IndexError: list index out of range`
3. Zephyr wrapped that as `ZE-UNS-PARTITION-FAILED`

This was not a contract/profile problem and not a Base-boundary issue. It was a bounded image
preparation gap for extraction-heavy image partitioning.

## Fix strategy

Zephyr now applies a bounded image preflight only when all of these are true:

- kind = `image`
- heavy image extraction is enabled
- the input image mode is JPEG-unsafe for the extraction/save path

For the `fapiao3.jpg` case, Zephyr:

- probes the image safely
- detects palette mode `P`
- writes a deterministic temporary normalized `RGB` image copy
- preserves the original logical filename through `metadata_filename`
- calls the existing unstructured backend on the normalized temporary image
- records structured warning evidence:
  - `image_preflight_applied=true`
  - `image_preflight_reason=palette_mode_jpeg_save_risk`
  - `original_image_mode=P`
  - `normalized_image_mode=RGB`
  - `normalized_image_used=true`

Important boundaries preserved:

- `invoice` profile is not weakened
- heavy features remain opt-in
- no degraded empty-success path was introduced
- no dependency downgrade occurred
- no Base change occurred
- no commercial logic was added

## Benchmark evidence

The real local benchmark set now passes:

- `fapiao.jpeg`: `ok`
- `fapiao2.jpg`: `ok`
- `fapiao3.jpg`: `ok` with image preflight evidence
- `hetong2-jiashuiyin.pdf`: `ok`

Observed `fapiao3.jpg` evidence after the fix:

- element types present: `Image`
- metadata coverage includes:
  - `coordinates`
  - `image_base64`
  - `image_mime_type`
  - `layout_width`
  - `layout_height`
  - `page_number`
  - `languages`

## Non-regression note

The preflight path is intentionally narrow:

- it does not run for the lightweight default image path
- it does not change non-image partition behavior
- it does not broaden Zephyr runtime ownership
- it does not change P6K-M1 profile merge rules, CLI flags, or metadata redaction policy
