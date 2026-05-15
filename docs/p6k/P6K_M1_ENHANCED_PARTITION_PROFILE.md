# P6K-M1 Enhanced Partition Profile

## Scope

P6K-M1 upgrades `uns-stream` from a lightweight text-oriented partition wrapper into a stronger
core partition surface for future Pro/Web work. This slice stays inside Zephyr-dev core boundaries:

- no commercial logic
- no Base product changes
- no Web-core / Pro / Site changes
- no PackageManifest or workflow runtime work
- no VLM / `ocr_only` route rewrite

## Dependency change

- previous `unstructured` version: `0.21.5`
- target `unstructured` version: `0.22.28`
- package declaration now pins `unstructured==0.22.28`
- `uv.lock` is updated consistently with the M1 kernel upgrade

This slice does not claim that Base should inherit heavy OCR/PDF/image behavior by default. Heavy
partition behavior remains opt-in through profile and CLI choices.

## Profile layer

P6K-M1 adds an explicit enhanced partition profile layer with these profile names:

- `default`
- `zh`
- `html_heavy`
- `invoice`
- `contract`

Merge rule is deterministic:

1. base defaults
2. profile defaults
3. explicit function args
4. explicit CLI args

Explicit caller input always wins over profile defaults.

### `default`

`default` preserves current lightweight behavior as much as possible:

- no forced languages
- `detect_language_per_element=False`
- no forced image/table extraction payloads
- no heavy defaults enabled automatically

### `zh`

`zh` enables:

- `languages=["zho", "eng"]`
- `detect_language_per_element=True`

### `html_heavy`

`html_heavy` enables a high-detail layout path without becoming the global default:

- `strategy=hi_res`
- `skip_infer_table_types=[]`
- `extract_image_block_types=["Image", "Table"]`
- `extract_image_block_to_payload=True`

### `invoice`

`invoice` enables:

- `strategy=hi_res`
- `languages=["zho", "eng"]`
- `skip_infer_table_types=[]`
- `extract_image_block_types=["Image", "Table"]`
- `extract_image_block_to_payload=True`

### `contract`

`contract` enables:

- `strategy=auto`
- `languages=["zho", "eng"]`
- `detect_language_per_element=True`
- `skip_infer_table_types=[]`
- `extract_image_block_types=["Image", "Table"]`
- `extract_image_block_to_payload=True`

## Wrapper and passthrough surface

P6K-M1 promotes these partition parameters to first-class Zephyr wrapper inputs:

- `profile`
- `languages`
- `detect_language_per_element`
- `language_fallback`
- `skip_infer_table_types`
- `infer_table_structure`
- `pdf_infer_table_structure`
- `extract_image_block_types`
- `extract_image_block_output_dir`
- `extract_image_block_to_payload`
- `data_source_metadata`
- `metadata_filename`
- `hi_res_model_name`
- `model_name`
- `starting_page_number`
- `extra_partition_kwargs`

The backend protocol is not widened. Instead, Zephyr resolves profile defaults at the service layer
and passes explicit backend kwargs through the existing local/backend bridge.

### Unknown kwarg policy

Unknown partition kwargs are not silently swallowed.

- known fields must use the first-class API
- extra backend kwargs must go through `extra_partition_kwargs`
- direct unknown `**partition_kwargs` raise a clear `ZephyrError`
- `extra_partition_kwargs` cannot shadow known fields

This keeps runtime behavior auditable.

### Alias policy

`infer_table_structure` is supported as a compatibility alias.

- for the local backend and HTTP UNS API bridge, Zephyr maps deterministically to the appropriate
  downstream shape
- if both `infer_table_structure` and `pdf_infer_table_structure` are provided with conflicting
  values, Zephyr raises a clear error instead of guessing

## CLI surface

P6K-M1 wires these CLI flags into `zephyr-ingest`:

- `--profile`
- `--languages`
- `--detect-language-per-element`
- `--skip-infer-table-types`
- `--extract-image-block-types`
- `--extract-image-block-output-dir`
- `--extract-image-block-to-payload`
- `--hi-res-model-name`
- `--starting-page-number`
- `--pdf-infer-table-structure`
- `--infer-table-structure`

The CLI keeps old default behavior compatible:

- omitted `--strategy` does not force a new heavy path
- list-like values accept repeatable and comma-separated input
- heavy behavior is documented as opt-in

## Governance metadata preserved

Enhanced partition settings are additive. They do not replace Zephyr governance fields.

Preserved governance facts include:

- `run_id`
- `pipeline_version`
- `sha256`
- `size_bytes`
- `filename`
- `strategy`
- `backend`
- `unique_element_ids`
- engine name/version
- duration
- warnings
- retryability/error semantics

## Metadata guard coverage

P6K-M1 adds explicit guard tests for enriched metadata survival through normalization and Zephyr
element serialization:

- `text_as_html`
- `image_base64`
- `image_mime_type`
- `parent_id`
- `is_extracted`
- `detection_class_prob`
- `coordinates.system`
- `layout_width`
- `layout_height`
- `page_number`
- `filetype`
- `filename`
- `languages`
- `data_source`

Local-sensitive path redaction remains preserved:

- `file_directory` stays excluded

## Benchmark helper

P6K-M1 adds:

- `tools/p6k_m1_enhanced_partition_benchmark.py`

Planned benchmark root:

- `E:\Github_Projects\Zephyr-zh-test`

Planned benchmark inputs:

- `fapiao.jpeg`
- `fapiao2.jpg`
- `fapiao3.jpg`
- `hetong2-jiashuiyin.pdf`

Planned output root:

- `E:\Github_Projects\Zephyr-zh-test\tmp`

The tool reports element type presence, metadata coverage, warnings, errors, and normalized output
paths. Missing benchmark inputs are non-fatal by default so CI is not coupled to the local Chinese
sample folder.

## Base non-regression boundary

P6K-M1 hardens Zephyr-dev core only. It does not back-port heavier behavior into Base.

Boundary notes:

- heavy partition features remain opt-in
- Base is not changed by this slice
- Base should not be forced into heavy OCR/PDF/image extras by default
- output compatibility must stay bounded so Base-compatible wrappers can continue to exist

## Current implementation judgment

Implementation status and product-grade evidence are now both achieved.

Current local benchmark findings:

- real benchmark samples were run locally against `unstructured==0.22.28`
- `fapiao.jpeg` executed successfully
- `fapiao2.jpg` executed successfully
- `fapiao3.jpg` executed successfully after bounded Zephyr image preflight normalization
- `hetong2-jiashuiyin.pdf` executed successfully
- full benchmark result: `4/4 passed`

For the remaining previously failing image:

- `fapiao3.jpg` is actually a palette-mode `GIF` payload named with a `.jpg` extension
- under the `invoice` profile, upstream image-block extraction attempted JPEG save on palette data
- Zephyr now applies a narrow RGB preflight only for extraction-enabled image inputs that are
  JPEG-unsafe
- the benchmark records `image_preflight_applied=true` and `normalized_image_used=true`

Therefore:

- implementation completeness: achieved
- product-grade strong-core evidence: achieved
- next required validation: proceed to `P6K-M2` without reopening Base or commercial boundaries
