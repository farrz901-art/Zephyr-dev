# P6-M3 Base UI Design Brief

## Scope

This brief defines the design-window input needed for Zephyr-base.

M3-0 does not create final UI, animations, or brand assets.
It only freezes the design brief and requests the assets needed later.

## Product Feel

Target feel:

- modern enterprise AI and productivity desktop product
- aerodynamic Zephyr lines
- wind-tunnel and cut aesthetic
- red accent direction
- disciplined and technical, not game-like

## Required UI Surfaces

Design window input is required for:

- welcome screen
- local file drag and drop area
- selected file state
- progress and running animation
- success result page
- normalized_text preview card
- content_evidence and elements summary card
- receipt and output-folder card
- usage-fact non-billing card
- error diagnosis panel
- empty, loading, success, and failure states
- app icon draft

## Architecture Fit

The UI is expected to sit on:

- Tauri shell
- Rust command bridge
- Web UI
- local artifact and process bridge to Python public core

The design must not assume:

- cloud runtime
- entitlement UI
- payment UI
- Pro-only connector management

## Deferred

The following remain outside M3-0:

- final animation implementation
- final brand pack
- final iconography system
- final marketing visuals
