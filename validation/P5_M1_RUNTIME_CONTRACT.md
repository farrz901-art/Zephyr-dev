# P5-M1 Runtime Contract

## Scope
This artifact makes the current P5 runtime model explicit without pretending a finished deployment platform already exists.

It captures the current production runtime contract for the already-audited retained support surface:

- control plane: repo-owned code, tests, helpers, templates, validation tooling, audit artifacts
- runtime plane: external runtime-home rooted at `E:\zephyr_env\.config\zephyr\p45`
- lifecycle / preflight / sanity: repo-side runtime resolution, repo-side preflight validation, repo-side healthcheck tooling

This is not a cloud/Kubernetes/Helm packaging claim.

## Canonical Runtime Home
Current verified runtime home:

- `E:\zephyr_env\.config\zephyr\p45`

Current verified canonical external files:

- `E:\zephyr_env\.config\zephyr\p45\docker-compose.yml`
- `E:\zephyr_env\.config\zephyr\p45\env\.env.p45.local`
- `E:\zephyr_env\.config\zephyr\p45\env\p45.local.env`
- `E:\zephyr_env\.config\zephyr\p45\env\p45.gdrive.env`
- `E:\zephyr_env\.config\zephyr\p45\env\p45.confluence.env`

Current verified required runtime layout:

- `bin`
- `data`
- `env`
- `logs`

The repo remains the control plane. The external runtime-home remains the runtime plane.

## Control Plane vs Runtime Plane
Control plane stays in repo:

- code
- tests
- helpers
- templates/examples
- audit artifacts
- validation tooling

Runtime plane stays external:

- real compose file
- real env files
- real secrets/tokens/passwords
- runtime logs
- runtime data
- runtime-adjacent local state

Repo files that remain templates/examples only:

- `.env.example`
- `.env.p45.example`
- `docker-compose.p45-validation.yml`

They are allowed as fallback/bootstrap aids, but they are not the canonical live runtime files.

## Env and Secret Contract
The machine-readable contract is [p5_runtime_contract.json](/E:/Github_Projects/Zephyr/validation/p5_runtime_contract.json).

The current contract distinguishes:

- startup-required local substrate/runtime groups
- validation-required SaaS auth groups
- public settings
- secret-bearing variables
- explicit secret-carrier variables that may embed credentials in full URI/URL form

Explicit secret-carrier env names currently treated as redaction-sensitive:

- `ZEPHYR_P45_OPENSEARCH_URL`
- `ZEPHYR_P45_CLICKHOUSE_URL`
- `ZEPHYR_P45_LOKI_URL`
- `ZEPHYR_P45_MONGODB_URI`

This is intentionally stricter than treating only `*_PASSWORD` / `*_TOKEN` names as secret-bearing, because full connection strings can leak credentials too.

## Runtime Resolution and Startup Contract
Runtime resolution rules:

- runtime home is resolved from `ZEPHYR_P45_HOME` or the external default home convention
- env dir resolves under runtime home as `env`
- canonical compose file is expected at runtime home as `docker-compose.yml`
- canonical live env files are expected in the external runtime env dir, not in repo-root

Startup/preflight expectations:

- use repo-side preflight tooling to validate runtime-home resolution, required layout, canonical file presence, env group presence, redaction-safe reporting, proxy assumptions, and OpenSearch TLS reality
- use repo-side healthcheck tooling for local-real/service-live readiness rather than pretending compose-native `healthcheck:` blocks are the primary truth surface

Current contract does not claim:

- distributed runtime ownership
- cloud/Kubernetes/Helm packaging
- a broad deployment matrix
- a generalized secret management platform

## Network / TLS / Proxy Realities
Current realities that are part of the contract:

- live-validation HTTP layers must preserve `trust_env=False`
- checked terminal is expected to have no proxy env set
- current health/readiness is primarily repo-side
- OpenSearch currently runs as local HTTPS with skip-TLS-verify behavior
- Google Drive access depends on TUN/VPN/network condition

These are not smoothed over into fake “uniform production networking” claims.

## Preflight Tooling
Executable repo-side preflight:

- `uv run --locked --no-sync python tools/p5_runtime_preflight.py --show-contract-summary --show-env`

Optional repo-side readiness extension:

- `uv run --locked --no-sync python tools/p5_runtime_preflight.py --check-health --health-tier all`

The preflight output must stay redaction-safe.

## Deferred / Bounded Realities
Still intentionally bounded after P5-M1:

- Windows absolute-path coupling is current reality, not a permanent cross-platform promise
- repo-side healthcheck tooling is still the primary readiness surface
- Google Drive reachability still depends on network/TUN/VPN condition
- this milestone does not introduce benchmark/SLI, autoscaling, dashboards/alerting, or cloud packaging
- this milestone does not redesign queue/lock/runtime ownership

## Audit Result
P5-M1 makes the current runtime model explicit, enforceable, and auditable without pretending a finished deployment platform already exists.
