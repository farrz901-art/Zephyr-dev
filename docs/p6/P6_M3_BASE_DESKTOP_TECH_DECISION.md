# P6-M3 Base Desktop Technical Decision

## Frozen Decision

P6-M3 Base desktop uses:

- Tauri desktop shell
- Rust shell and command bridge
- Web UI
- local process and adapter bridge into Zephyr public core

This is the frozen M3 route.

## Python and Rust Boundary

Rust does not replace the Zephyr Python core in M3.

Python public core continues to own:

- document processing
- normalized_text generation
- content_evidence generation
- receipt-shaped technical output facts

Rust and Tauri own:

- file selection
- desktop shell lifecycle
- UI state
- command bridge
- local process orchestration
- local artifact/result loading
- user-facing error presentation

## Bridge Strategy

M3 does not require Python-to-Rust full rewrite.

Allowed bridge strategies:

- process bridge
- JSON artifact bridge
- local IPC bridge

Recommended M3 initial strategy:

- JSON artifact bridge
- process bridge

These are preferred because they preserve Zephyr-dev pure-core truth without
forcing early runtime rewrite.

## Explicit Non-Requirement

The following is explicitly not required for M3:

- full Python to Rust automatic translation
- Rust-native rewrite of Zephyr core processing
- Rust ownership of connector runtime logic

If a later phase evaluates Rust-native rewrite, that is P7-or-later analysis and
not an M3 blocker.

## Runtime Dependency Rule

Base must not require ordinary users to boot the P4.5 Docker substrate.

M3 default path is local-only.

External connectors, Docker substrate, and broader service orchestration remain
developer, Pro, or later-phase paths rather than the default Base route.
