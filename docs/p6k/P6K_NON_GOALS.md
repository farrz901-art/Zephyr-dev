# P6K Non-Goals

P6K non-goals are explicit and binding.

Global non-goals:

- no Base feature backfill
- no commercial logic
- no license, entitlement, billing, payment, quota, or download authorization logic
- no Web-core implementation inside Zephyr-dev
- no SaaS, multi-tenant, cloud, or Kubernetes work

P6K-M0 non-goals:

- no unstructured upgrade
- no `uv.lock` change
- no partition runtime behavior change
- no enhanced profile implementation
- no CLI flag implementation for enhanced partition parameters
- no `PackageManifest` implementation
- no workflow runtime implementation
- no new nodes
- no Feishu source implementation
- no broad connector expansion
- no new LLM or OCR model implementation
- no Base modification

P6K-M1 and later are allowed to implement kernel hardening only when the milestone explicitly calls
for it, but they still do not authorize commercial logic inside Zephyr-dev.
