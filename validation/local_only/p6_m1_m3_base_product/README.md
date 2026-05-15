## P6-M1~M3 Base Product Local-Only Validation

These files were created for local validation during the P6-M1~M3 Zephyr-base product build-out.

They are not Zephyr-dev pure-core CI tests because they depend on one or more of the following:

- a local external `Zephyr-base` repository
- local Windows product paths such as `E:\Github_Projects\Zephyr-base`
- local Tauri or installer/package artifacts
- local desktop-product runtime layout or proof artifacts

They are intentionally excluded from automated Zephyr-dev CI.

They remain here only as historical and local validation references for the Base product work that was performed outside the Zephyr-dev pure-core CI boundary.
