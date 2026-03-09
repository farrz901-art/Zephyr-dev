# typings

Third-party stub files for libraries that:
- have no typing information, or
- ship incomplete typings, causing pyright strict failures.

Rule:
- Prefer upstream type packages first (types-requests, etc.).
- Only add stubs here when necessary.
- Each stub file should include a short comment with why it exists.
