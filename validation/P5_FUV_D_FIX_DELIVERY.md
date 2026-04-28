# P5 FUV D Delivery Fix

This patch fixes retained P5 final user validation D delivery blockers; it does not start P6 work.
It keeps the current TaskV1 -> FlowProcessor -> Delivery execution chain and only hardens bounded local delivery visibility, S3 readiness, and fanout readability.

## What This Fix Changes

- Delivery payloads keep `schema_version=1`, `sha256`, `run_meta`, and `artifacts`, and add an optional bounded `content_evidence` field.
- Kafka, OpenSearch, Webhook, and S3 can now carry marker-visible normalized text or IT record previews without removing artifact-reference payload behavior.
- S3 dependency durability is declared through the `zephyr-ingest` optional `s3` extra backed by `boto3`.
- S3 bucket readiness is checkable or explicitly ensureable from the external runtime-home configuration without printing secrets.
- fanout remains composition/orchestration, not a sink connector.

## What This Fix Does Not Claim

- No installer implementation, deployment packaging, or product-shell work is added here.
- No distributed runtime, cloud, Kubernetes, or large-scale load-testing claim is made here.
- No billing, entitlement, RBAC, or enterprise connector claim is introduced here.

## User Revalidation Intent

- Filesystem visibility still comes from delivered artifacts such as `normalized.txt` and `records.jsonl` rather than from a remote endpoint.
- Remote endpoint validation becomes easier because payload content evidence now carries bounded marker-visible previews.
- S3 readiness is durable only when dependency and bucket checks are both green.
- Fanout readability now includes explicit child destination and summary surfaces at the top-level receipt layer.
