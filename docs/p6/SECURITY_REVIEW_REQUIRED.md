# Security Review Required

## Scope

Codex may generate boundary docs, manifests, scanning tools, and draft policy
surfaces.

Codex may not independently decide final security policy or commercial policy.

Human review is mandatory before accepting any implementation that introduces or
changes the following:

- entitlement logic
- payment callback handling
- billing or quota decisions
- license generation, activation, or revocation
- device binding
- download authorization
- signed URL issuing
- package signing policy
- update manifest signing or update entitlement logic
- risk or fraud decision logic
- API gate or rate-limit decision logic
- secret handling

## Repo Placement Rule

These concerns belong in Zephyr-Web-core or the appropriate downstream private
repo.

They do not belong in Zephyr-dev runtime code.

## Review Rule

The presence of a draft manifest, scanner, or CODEOWNERS placeholder in
Zephyr-dev does not mean a security or commercial policy has been approved.

Any implementation work in future private repos must receive human review.
