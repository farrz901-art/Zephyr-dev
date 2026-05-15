# P6-M3-S5 Public Core Bundle Policy

## Purpose

P6-M3-S5 introduces the first bundled public-core runtime slice for
`Zephyr-base`.

This bundle is a public-subset runtime artifact.
It is not a copy of the entire `Zephyr-dev` working tree.

## Boundary

The bundle may include:

- the public-core local runner entrypoint
- the bounded public package subset needed for local `.txt` and `.md` processing
- bundle lineage manifests
- bundle file hashes

The bundle must not include:

- Web-core
- Pro
- payment
- license
- entitlement
- billing authority
- quota authority
- risk decision code
- runtime secrets
- P4.5 substrate config
- `.env` files
- `runtime-home`

## Generation vs Execution

Bundle generation may require the local `Zephyr-dev` working tree.

Bundle execution must not require the local `Zephyr-dev` working tree.

S5 therefore allows:

- `bundle_generation_requires_zephyr_dev_working_tree = true`
- `bundle_execution_requires_zephyr_dev_working_tree = false`

## Runtime Status

S5 is the first runtime packaging slice for Base.

It is not yet:

- a final installer runtime
- a Tauri-integrated desktop runtime
- an embedded Python runtime
- a wheelhouse-complete offline runtime

S5 may still rely on the current Python environment at execution time.
That dependency must be recorded truthfully as:

- `uses_current_python_environment = true`
- `embedded_python_runtime = false`
- `wheelhouse_bundled = false`
- `installer_runtime_complete = false`

## Product Rule

The Base bundled runtime remains:

- local-only
- public-subset only
- non-commercial
- non-cloud

It must not require:

- P4.5 substrate boot
- Docker compose startup
- Web-core
- Pro
- private core export

## Follow-on

Later M3 slices will connect this bundle to:

- Tauri/Rust command bridge
- installer alpha
- clean-machine packaging strategy

S5 itself does not complete those steps.
