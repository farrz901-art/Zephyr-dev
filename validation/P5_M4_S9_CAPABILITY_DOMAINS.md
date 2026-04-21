# P5-M4-S9 Capability Domains

This closeout package freezes Base / Pro / Extra as technical capability domains. They are not pricing, subscription, licensing, or entitlement domains.

## Domain Truth

Base is the current shared core capability surface:

- all current `it-stream` capabilities
- the non-extra `uns-stream` surface backed by the direct `unstructured` dependency
- all current source connectors
- all current destination connectors
- all current connectors as non-enterprise connectors

Pro is Base plus the current `uns-stream` optional document extras:

- `all-docs`
- `pdf`
- `doc`
- `docx`
- `ppt`
- `pptx`
- `xlsx`
- `csv`
- `tsv`
- `md`
- `rtf`
- `rst`
- `epub`
- `odt`
- `org`
- `image`

Extra is an incubation and forward-looking technical domain. It currently reserves:

- `audio`
- `model_init`

Extra to Pro promotion is explicit: a mature Extra capability must update the machine-readable matrix, dependency boundary manifest, build-cut manifest, and focused drift tests. Promotion is not implicit from implementation presence.

## Connector Assignment

All retained current connectors are Base and non-enterprise today.

The current `it-stream` Base source surface is:

- `http_json_cursor_v1`
- `postgresql_incremental_v1`
- `clickhouse_incremental_v1`
- `kafka_partition_offset_v1`
- `mongodb_incremental_v1`

The current `uns-stream` Base source surface is:

- `http_document_v1`
- `s3_document_v1`
- `git_document_v1`
- `google_drive_document_v1`
- `confluence_document_v1`

The current Base destination surface is:

- `filesystem`
- `webhook`
- `sqlite`
- `kafka`
- `weaviate`
- `s3`
- `opensearch`
- `clickhouse`
- `mongodb`
- `loki`

`base.py` in the destination layer remains an abstract base, not a connector capability.

## Mixed `uns-stream` Reality

`uns-stream is mixed`: it is not modeled as a whole-package Pro surface.

The Base surface is the package direct dependency on `unstructured` and the non-extra document processing capability. The Pro surface is represented by optional package extras over `unstructured[...]`. Extra is not currently packaged as a Pro extra.

This distinction is required because the root dev environment currently includes `uns-stream[all-docs]`, which is evidence of missing build-cut isolation, not evidence that all of `uns-stream` is Pro.

## Non-Claims

S9 does not implement commercial licensing, authorization, payment, subscription, or entitlement logic.

S9 does not implement enterprise connectors.

S9 does not make Base, Pro, or Extra directly buildable product cuts. It creates machine-readable groundwork and drift protection for later build-cut work.
