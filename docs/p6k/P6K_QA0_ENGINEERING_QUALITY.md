# P6K-QA0 Engineering Quality and Test Control

## Why QA0 exists

P6K-QA0 is the first low-risk quality-control patch for `Zephyr-dev`.

Its purpose is not to add a large CI matrix. Its purpose is to make Zephyr quality gates clearer,
more parallelizable, and safer for future kernel hardening work.

Zephyr is not a single-library OCR project. It is a layered core covering:

- stable connector contracts
- record and delivery envelopes
- error vocabulary and retryability
- checkpoint and resume semantics
- evidence and governance facts
- UNS document flows
- IT structured flows
- P4.5 authenticity substrate drills

That means Zephyr should not blindly copy Unstructured's CI layout. The engineering split has to
follow Zephyr's own boundaries.

## QA0 design principles

QA0 keeps ordinary PR CI focused on reproducible pure-core behavior:

- fast static quality gates
- stable contract tests
- deterministic fixture tests
- broad stable core tests
- minimal package build smoke

QA0 explicitly keeps these out of ordinary PR CI:

- `auth_local_real`
- `auth_service_live`
- `auth_recovery_drill`
- `slow`

Those remain manual, dispatch, or release-candidate style paths.

## Install modes

QA0 keeps ordinary developer and CI install behavior lightweight:

- `make install` -> `uv sync --locked --all-groups --all-packages`

QA0-FIX adds explicit full local development install targets:

- `make install-all`
- `make install-full`
- `make install-dev-full`

These full targets intentionally keep `--all-extras` available for local capability-heavy work such
as later OCR, PDF, image, or broader connector validation, without putting `--all-extras` back
into ordinary CI.

## Pytest marker taxonomy

QA0 adds these markers:

- `unit`: fast deterministic unit test
- `contract`: stable core contract/schema test
- `fixture`: fixture or golden-output regression test
- `uns`: `uns-stream` related test
- `it`: `it-stream` related test
- `connector`: source or destination connector contract test
- `p45`: P4.5 authenticity or substrate related test
- `slow`: slow test excluded from ordinary PR CI
- `integration`: optional-extras or fixture-heavy integration test

Existing authenticity markers remain and are clarified:

- `auth_contract`
- `auth_orchestration`
- `auth_local_real`
- `auth_service_live`
- `auth_saas_live`
- `auth_recovery_drill`

QA0 does not require every historical stable test to be marked immediately. Unmarked stable tests
still run under the default core test target.

## Default test exclusion

Ordinary `make test` now routes to `make test-core`, which uses:

`not auth_local_real and not auth_service_live and not auth_recovery_drill and not slow`

This keeps live/service/recovery tests out of ordinary PR CI while preserving the existing stable
unmarked core suite.

## Makefile changes

QA0 splits the former broad `check` and `test` surface into:

- `check-format`
- `lint`
- `typecheck`
- `check-tools`
- `check`
- `test-core`
- `test-contracts`
- `test-fixtures`
- `test-uns`
- `test-it`
- `test-connectors`
- `test`
- `build-smoke`
- `offline-proof`

Important notes:

- `check-format` and `lint` now target repo-owned Python package surfaces under `packages/`.
- `check-tools` still compiles `tools/*.py` to keep local helper scripts syntactically valid.
- marker-specific targets use dedicated `--basetemp` directories and `-n 0` on Windows to avoid
  xdist temp cleanup noise from leaking across layered targets.
- `tools/pytest_target.py` now pre-creates `--basetemp` directories for both `--basetemp=PATH` and
  `--basetemp PATH` forms so local and remote CI do not fail on missing temp roots.
- `test-core` remains parallel and broad.

## P4.5 authenticity naming

Old targets are preserved:

- `p45-substrate-up`
- `p45-substrate-down`
- `p45-substrate-healthcheck`
- `p45-test-local-real`
- `p45-test-service-live`
- `p45-test-recovery-drill`

Clearer aliases are added:

- `p45-auth-substrate-up`
- `p45-auth-substrate-down`
- `p45-auth-substrate-healthcheck`
- `p45-auth-test-local-real`
- `p45-auth-test-service-live`
- `p45-auth-test-recovery-drill`
- `p45-auth-compose-config`
- `p45-auth-evidence-proof`

`p45-auth-compose-config` is print-only. It does not start Docker. It reports:

- runtime home
- compose path
- env-file path
- existence checks
- platform path note

QA0 does not fully rework the legacy `p45-substrate-up/down` Windows-oriented env-file path join.
That remains a documented limitation for later cleanup.

## CI split

QA0 replaces the old single `check` job with these jobs:

- `quality`
- `unit`
- `contracts`
- `fixtures`
- `build`

These jobs are more understandable and parallelizable than the previous single funnel.

QA0 intentionally does not add:

- local-real authenticity jobs
- service-live authenticity jobs
- recovery-drill jobs
- Docker substrate jobs
- PaddleOCR/OCR-heavy jobs
- broad extras-matrix jobs

Ordinary CI also stops using `--all-extras`.

This remains true after QA0-FIX:

- CI jobs keep `uv sync --locked --all-groups --all-packages`
- full extras stay opt-in through `make install-all` / `make install-dev-full`

Current limitation:

- ordinary CI still uses `uv sync --locked --all-groups --all-packages`
- the root `dev` group still pulls heavy optional capability through existing workspace choices
- dependency/extras minimization is deferred

## Minimal contract and fixture coverage

QA0 adds a small deterministic contract/fixture start point rather than inventing a new runtime
schema system.

Covered now:

- `RunMetaV1`
- health and lifecycle contract exports
- `DeliveryPayloadV1` stable shape
- `DeliveryReceiptV1` stable shape

The new fixture/contract tests verify:

- stable schema discriminator facts
- JSON round-trip
- metadata preservation
- receipt detail preservation for retryability and error-code-like facts
- invalid fixture detection for missing required top-level fields

Current gaps intentionally documented for QA1/QA2:

- no first-class `ErrorEnvelope` runtime contract object
- no first-class `CheckpointState` shared core contract object
- no first-class `EvidenceRecord` runtime contract object
- no first-class `AuthTier` shared core enum/config contract
- no broad golden output framework yet

## Why this does not lower quality

QA0 does not disable broad tests. It makes the default path safer and more inspectable.

- ordinary `test-core` still runs the large stable suite
- contract and fixture targets are now explicit
- UNS / IT / connector targets can run in isolation
- real/service/recovery tiers are fenced off instead of accidentally leaking into PR CI

This is a stronger quality layout than the previous single-job funnel, not a weaker one.

## What remains for QA1 / QA2 / QA3

Suggested follow-up areas:

- QA1: widen marker adoption and add more stable contract coverage
- QA2: introduce broader golden/fixture output coverage
- QA3: reduce install weight and make extras/job selection more selective
- later P4.5 cleanup: make legacy authenticity compose/env-file path handling more portable

QA0 is the first control-layer patch, not the last one.
