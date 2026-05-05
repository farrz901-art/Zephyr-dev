# P6-M3 Base Bridge Contract

## Purpose

This bridge contract freezes the local-only contract between:

- Base desktop shell and Rust command bridge
- Python public core process/artifact producer
- Web UI result rendering layer

It is a scaffold planning contract only.
It does not create a production desktop runtime.

## Contract Set

The bridge contract includes:

- `base_run_request_v1`
- `base_run_result_v1`
- `base_receipt_view_v1`
- `base_error_v1`

## Boundary Rules

These contracts must not include:

- billing
- entitlement
- license
- account identity
- Web-core user state
- private connector state

`usage_fact` remains technical fact only.
It explicitly does not carry billing semantics.

## Secret Safety

Error and result views must remain safe for local user display.

- `technical_detail_safe` may explain bounded technical cause
- `secret_safe` must remain `true`
- no secret or credential value may be serialized into bridge outputs
