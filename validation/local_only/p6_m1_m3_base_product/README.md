## P6-M1~M3 Base Product Local-Only Validation

These files were created for local validation during the P6-M1~M3 Zephyr-base product build-out.

They are not Zephyr-dev pure-core CI tests because they depend on one or more of the following:

- a local external `Zephyr-base` repository
- local Windows product paths such as `E:\Github_Projects\Zephyr-base`
- local Tauri or installer/package artifacts
- local desktop-product runtime layout or proof artifacts

They are intentionally excluded from automated Zephyr-dev CI.

Tracked repository content in this directory is limited to historical notes and selected non-pytest helper references.

Any local pytest-style test copies kept under the nested `packages/.../tests/` subtree are workstation-only artifacts. They are intentionally not meant to live in the remote GitHub repository and may be ignored locally.

This directory remains only as a historical and local validation reference for Base product work that was performed outside the Zephyr-dev pure-core CI boundary.
