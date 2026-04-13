# 6.工具链与门禁

# CI/Tooling & Quality Gates (工具链与门禁)

This document describes the tooling infrastructure, local development workflow, and CI gates that maintain code quality and prevent regressions.

---

## 1) Overview: Three-layer Quality Assurance

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 1: Pre-commit Hooks (Local, before git commit)       │
│  ├─ File checks (trailing whitespace, TOML/YAML/JSON)       │
│  ├─ Ruff formatting & linting (auto-fix)                    │
│  └─ Blocked patterns: mixed line endings (on non-Windows)   │
├─────────────────────────────────────────────────────────────┤
│  Layer 2: Local Make targets (before push)                  │
│  ├─ make tidy    → ruff format + ruff check --fix           │
│  ├─ make check   → ruff check + pyright + mypy              │
│  ├─ make test    → pytest unit + contract tests             │
│  └─ make install → uv sync (lock enforcement)               │
├─────────────────────────────────────────────────────────────┤
│  Layer 3: GitHub Actions CI (.github/workflows/ci.yml)      │
│  ├─ Lint & Typecheck (make check)                           │
│  ├─ Test (make test)                                        │
│  ├─ Tools gate (py_compile)                                 │
│  └─ Runs on: push to main, all PRs                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 2) Local Development Workflow

### 2.1 Initial Setup

```bash
# Clone repo
git clone https://github.com/farrz901-art/Zephyr-dev.git
cd Zephyr-dev

# (Optional) Configure Git for Windows developers
git config core.autocrlf input    # or global: --global
git config core.safecrlf true

# Install Python via uv (or use system Python 3.12+)
uv python install

# Install all dependencies (including dev)
make install

# Install pre-commit hooks (prevents bad commits)
pre-commit install
```

### 2.2 Before every commit

```bash
# Option A: Use pre-commit (runs automatically on git commit)
git add ...
git commit -m "..."  # pre-commit runs automatically

# Option B: Manual - reformat + lint + typecheck
make tidy           # Auto-format code
make check          # Lint + type check (no auto-fix)
make test           # Run tests

# Only push if all three pass
git push origin feature-branch
```

### 2.3 Iterating on code

**During development (multiple runs):**

```bash
# Quick format check (ruff only, no pyright/mypy overhead)
uv run ruff check .

# Full check once before push
make check

# Run specific test
uv run pytest packages/zephyr-ingest/src/zephyr_ingest/tests/test_runner.py -k test_retry
```

**Common patterns:**

```bash
# Fix all auto-fixable issues
make tidy

# Check specific package
uv run pyright packages/uns-stream/src

# Check with detailed output
uv run --locked ruff check . --show-fixes
```

---

## 3) Tool Configuration & Rationale

### 3.1 Ruff (Format + Lint)

**Location:** `pyproject.toml` → `[tool.ruff]` + `[tool.ruff.lint]`

**What it does:**
- Format: auto-formats to consistent style (double quotes, 100-char lines)
- Lint: catches style issues and common mistakes

**Config highlights:**

| Setting | Value | Reason |
|---------|-------|--------|
| line-length | 100 | Readable in terminal, GitHub diffs |
| quote-style | double | Convention for Python ecosystem |
| select | C4, COM, E, W, F, I, PT, SIM, UP* | Catch common mistakes |
| ignore | COM812, PT006, SIM115, SIM117 | Too aggressive/noisy for tests |

**Selected rule codes:**
- `C4` — Flake8 comprehensions (unnecessary comprehensions)
- `COM` — Flake8 commas (trailing commas consistency)
- `E/W` — Pycodestyle errors/warnings (PEP 8)
- `F` — Pyflakes (undefined names, unused imports)
- `I` — isort (import sorting)
- `PT` — Pytest style (test best practices)
- `SIM` — Flake8 simplify (refactor suggestions)
- `UP015, UP018, UP032, UP034` — PyUpgrade (modernize syntax)

**Usage:**

```bash
# Format all files
uv run ruff format .

# Lint + fix auto-fixable issues
uv run ruff check . --fix

# Show what would be fixed (dry run)
uv run ruff check . --show-fixes

# Check without fixing
uv run ruff check .
```

---

### 3.2 Pyright (Type Checking)

**Location:** `pyproject.toml` → `[tool.pyright]`

**What it does:**
- Strict static type checking (catches type errors before runtime)
- Protocol validation (keyword-only mismatch)
- Unknown type narrowing (detects unhandled Any values)

**Config highlights:**

```toml
[tool.pyright]
pythonPlatform = "Linux"
pythonVersion = "3.12"
typeCheckingMode = "strict"          # NO lenient mode!
reportUnnecessaryCast = true         # Reject cast() if unnecessary
reportUnnecessaryTypeIgnoreComment = true
strictListInference = true           # [] is list[Unknown], not list[Any]
strictDictionaryInference = true     # {} is dict[Unknown, Unknown]
strictSetInference = true
```

**Why strict mode matters:**

| Mode | Behavior | Zephyr choice |
|------|----------|---------------|
| off | No type checking | ❌ Not acceptable |
| basic | Minimal checks | ❌ Too permissive |
| standard | Default | ❌ Misses edge cases |
| strict | All checks + Unknown narrowing | ✅ Our choice |

**Usage:**

```bash
# Full check (all packages)
uv run pyright

# Check specific package
uv run pyright packages/uns-stream/src

# JSON output (for CI)
uv run pyright --outputjson

# Quick check (fewer diagnostics)
uv run pyright --level basic  # (not recommended; use strict)
```

**Common fixes:**

| Error | Cause | Fix |
|-------|-------|-----|
| `Unknown` from json()` | Untyped JSON | Use `isinstance` + `cast` (see MAINTAINER_GUIDE.md §2.3) |
| `Expected N positional args` | Protocol mismatch | Check keyword-only signature |
| `Unnecessary cast` | Redundant narrowing | Remove the cast() call |
| `Unused # type: ignore` | Type safe now | Remove comment |

---

### 3.3 Mypy (Legacy type checking)

**Location:** `pyproject.toml` → `[tool.mypy]`

**Status:** Secondary to Pyright (kept for compatibility)

**Config:**

```toml
[tool.mypy]
python_version = "3.12"
warn_unused_ignores = true
no_implicit_optional = true
ignore_missing_imports = true  # Allow untyped 3rd-party libs
```

**When to run:**

```bash
uv run mypy packages/
```

**Note:** Pyright output is authoritative; mypy errors are informational.

---

### 3.4 Pytest (Testing)

**Location:** `pyproject.toml` → `[tool.pytest.ini_options]`

**Config:**

```toml
[tool.pytest.ini_options]
testpaths = ["packages"]
addopts = "-q"  # quiet output
markers = [
    "integration: requires optional extras and fixture files"
]
```

**Usage:**

```bash
# Run all tests (parallel via xdist plugin)
uv run pytest -n auto

# Specific test file
uv run pytest packages/zephyr-ingest/src/zephyr_ingest/tests/test_runner.py

# Specific test
uv run pytest packages/zephyr-ingest/src/zephyr_ingest/tests/test_runner.py::test_retry_then_success

# With verbose output + coverage
uv run pytest -v --cov=packages

# Integration tests only
uv run pytest -m integration
```

---

## 4) Pre-commit Hooks

**Location:** `.pre-commit-config.yaml`

**What it does:**
- Runs before `git commit` to catch issues early
- Can auto-fix (ruff format) or block (yaml syntax errors)

**Hook list:**

| Hook | Action | Auto-fix? |
|------|--------|-----------|
| check-added-large-files | Prevent >5MB files | ❌ Block |
| check-toml | Validate TOML syntax | ❌ Block |
| check-yaml | Validate YAML syntax | ❌ Block |
| check-json | Validate JSON syntax | ❌ Block |
| end-of-file-fixer | Add final newline | ✅ Auto-fix |
| trailing-whitespace | Remove EOL spaces | ✅ Auto-fix |
| ~~mixed-line-ending~~ | LF/CRLF check | ⚠️ Disabled on Windows |
| ruff | Format + lint | ✅ Auto-fix (--fix) |
| ruff-format | Format only | ✅ Auto-fix |

**Usage:**

```bash
# Install hooks (run once after clone)
pre-commit install

# Run manually on all files
pre-commit run -a

# Run on staging area only
pre-commit run

# Skip hooks on commit (not recommended)
git commit --no-verify
```

**Troubleshooting:**

```bash
# Hooks keep failing on mixed line endings?
# (On Windows) Either:
# Option 1: Accept CRLF locally
git config core.autocrlf false

# Option 2: Force LF (recommended)
git config core.autocrlf input

# Then re-check files
pre-commit run -a
```

---

## 5) GitHub Actions CI Workflow

**Location:** `.github/workflows/ci.yml`

**What it does:**
- Runs on every push to `main` and all pull requests
- Enforces all quality gates before merge

**Workflow steps:**

```yaml
1. Checkout code (pinned SHA)
2. Read .python-version → detect Python version
3. Install uv (pinned SHA for reproducibility)
4. uv python install $PYTHON_VERSION
5. uv sync --locked --all-groups --all-packages
6. make check  # ruff check + pyright + mypy
7. make test   # pytest -n auto
```

**CI gates:**

| Step | Failure = | Impact |
|------|-----------|--------|
| ruff check | Lint error | ❌ Block merge |
| pyright | Type error | ❌ Block merge |
| mypy | Type warning | ⚠️ Warn (informational) |
| pytest | Test failure | ❌ Block merge |

**Viewing CI results:**

```
1. Go to GitHub PR → Checks tab
2. Click "Details" on failed check
3. Scroll to failing step
4. View full output logs
```

**Local reproduction:**

```bash
# Replicate CI environment locally
export PYTHON_VERSION=3.12
python$(eval "echo $PYTHON_VERSION") -m uv sync --locked --all-groups --all-packages
make check
make test
```

---

## 6) Make Targets (Developer commands)

**Location:** `Makefile`

**Available targets:**

```makefile
make install      # uv sync --locked --all-groups --all-packages
make lock         # uv lock --upgrade
make tidy         # ruff format . + ruff check --fix-only
make check        # ruff check + pyright + mypy (no auto-fix)
make test         # pytest -n auto
```

**Typical workflow:**

```bash
# 1. Make changes
vim packages/uns-stream/src/uns_stream/service.py

# 2. Auto-format
make tidy

# 3. Check for errors
make check

# 4. Run tests
make test

# 5. If all pass, push
git add .
git commit -m "..."
git push
```

**One-liner for full gate:**

```bash
make tidy && make check && make test && echo "✅ Ready to push"
```

---

## 7) Tools Scripts (Gatekeeping & Code Gen)

### 7.1 Stub Generation for uns-stream.partition

**Location:** `tools/rewrite_uns_stream_partition_stubs.py`

**Purpose:**
- Auto-generates stub functions for 28+ partition modules
- Ensures consistent function signatures
- Prevents accidental incomplete implementations

**How it works:**

```bash
uv run -m tools.rewrite_uns_stream_partition_stubs
```

Generates files like `packages/uns-stream/src/uns_stream/partition/{pdf,docx,xlsx,...}.py` with:

```python
def partition_pdf(*args: Any, **kwargs: Any) -> NoReturn:
    """Placeholder for uns_stream.partition.pdf.partition_pdf."""
    raise NotImplementedError(...)
```

**When to run:**
- After adding new partition module names to PARTITION_DIR
- Part of setup/onboarding

---

### 7.2 py_compile Gate

**Location:** CI step (implicit via make check)

**Purpose:**
- Verify all Python files are syntactically valid
- Detect missing imports before runtime

**Usage (CI automatically runs):**

```bash
# Manual check
python -m py_compile tools/*.py
python -m py_compile packages/*/src/**/*.py
```

**If it fails:**
- Fix syntax error in the file
- Re-run: `python -m py_compile <file>`

---

## 8) Dependency Management (uv)

### 8.1 Workspace Structure

**Location:** `pyproject.toml` + `uv.lock`

**Packages:**
- `zephyr-core` — Contracts & governance
- `uns-stream` — Unstructured adapter
- `zephyr-ingest` — Batch processor
- `zephyr-api` — (Future) REST layer

**How uv resolves dependencies:**

```toml
[tool.uv.workspace]
members = ["packages/*"]

[tool.uv.sources]
uns-stream = { workspace = true }
zephyr-core = { workspace = true }
...
```

→ When you `uv sync`, it installs local packages in editable mode.

### 8.2 Updating Dependencies

```bash
# Add a new dependency to root
uv add httpx --group dev

# Add to specific package
cd packages/uns-stream
uv add pydantic

# Upgrade all (regenerate lock)
make lock  # uv lock --upgrade

# Then sync
make install
```

### 8.3 Lock File (uv.lock)

**What it is:**
- Frozen snapshot of all dependency versions
- Committed to repo for reproducibility

**When to update:**
- Intentionally: `make lock` (then commit)
- Automatically: when PR changes dependency

**CI enforces:**
```bash
uv sync --locked  # FAILS if lock file doesn't match pyproject.toml
```

**If lock becomes stale:**

```bash
# Regenerate (locally)
make lock

# Commit
git add uv.lock
git commit -m "chore: update dependencies"
```

---

## 9) Cross-platform Considerations

### 9.1 Line Endings (Windows/Unix)

**Problem:** Windows uses CRLF (\\r\\n), Unix uses LF (\\n).

**Solution:** `.gitattributes` enforces LF

```gitattributes
* text=auto
*.py text eol=lf
*.md text eol=lf
*.yml text eol=lf
```

**For Windows developers:**

```bash
# Option 1: Configure Git
git config core.autocrlf input

# Option 2: Configure IDE
# VS Code: "files.eol": "\n"
# PyCharm: Settings → Editor → Line Separators → LF
```

**If you committed CRLF by accident:**

```bash
# Convert file
dos2unix file.py

# Or with Git
git config --global core.autocrlf true
git rm --cached file.py
git add file.py
```

### 9.2 Path Handling

**❌ Bad (breaks on Windows):**
```python
path = "packages/uns-stream/src/uns_stream"  # forward slashes in string
```

**✅ Good:**
```python
from pathlib import Path
path = Path("packages") / "uns-stream" / "src" / "uns_stream"
```

### 9.3 OS-specific code

**When you need OS-specific behavior:**

```python
import sys

if sys.platform == "win32":
    # Windows-only code
    ...
else:
    # Unix-like code
    ...
```

---

## 10) Troubleshooting CI Failures

### 10.1 "Expected N positional arguments" (Pyright)

**Cause:** Protocol signature doesn't match call site.

```python
# Protocol says:
class Writer(Protocol):
    def __call__(self, *, out_root: Path, ...) -> None: ...

# But called as:
writer(out_root, sha256)  # ❌ positional, not keyword-only
```

**Fix:**

```python
# Call correctly:
writer(out_root=out_root, sha256=sha256)
```

### 10.2 "Unknown type" (Pyright)

**Cause:** Narrowing not recognized.

```python
# Before
data = response.json()  # type: Any
x = data["key"]  # Error: Unknown

# After (narrow first)
data = response.json()
if isinstance(data, dict):
    data_dict = cast(dict[str, Any], data)
    x = data_dict["key"]  # ✅ Safe
```

### 10.3 Pre-commit keeps failing

**Common:** `trailing-whitespace` or `end-of-file-fixer`

```bash
# Let pre-commit auto-fix
pre-commit run -a

# Then re-stage
git add .
```

### 10.4 Lock file conflict (merge)

**Cause:** Both branches updated dependencies.

```bash
# Resolve conflict by regenerating
make lock

# Commit merged lock
git add uv.lock
git commit -m "chore: resolve lock conflict"
```

### 10.5 "Module not found" in CI

**Cause:** Package not in `pyproject.toml` or workspace member.

**Check:**

```bash
# Is it in workspace?
grep -A5 "\[tool.uv.workspace\]" pyproject.toml

# Is it listed in dependencies?
grep "my-package" pyproject.toml
```

**Fix:**

```bash
# Add workspace member
# in pyproject.toml: members = ["packages/*", "packages/my-new-package"]

uv sync --locked --all-packages
```

---

## 11) Performance & Optimization

### 11.1 Speeding up local checks

**Fast check (ruff only, ~1s):**
```bash
uv run ruff check .
```

**Full check (ruff + pyright + mypy, ~15s):**
```bash
make check
```

**Parallel tests (~20s with 8 cores):**
```bash
uv run pytest -n auto
```

**Single-core test (if parallel fails):**
```bash
uv run pytest  # No -n flag
```

### 11.2 CI performance

**Current:** ~2-3 minutes per PR

**Breakdown:**
- uv install: ~30s
- ruff check: ~5s
- pyright: ~10s
- pytest: ~45s
- Total: ~90s + overhead = ~2min

**Ways to improve (future):**
- Cache uv dependencies
- Split pyright into package-level checks
- Cache pytest fixtures

---

## 12) Security & Dependency Scanning

### 12.1 Vulnerability scanning

**Manual:**
```bash
# Check for known vulnerabilities
uv pip audit
```

**Automated (GitHub):**
- Dependabot alerts on vulnerable dependencies (enable in repo settings)

### 12.2 Pinning SHAs for reproducibility

**In CI (already done):**

```yaml
- uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683
- uses: astral-sh/setup-uv@4db96194c378173c656ce18a155ffc14a9fc4355
```

- Not: `@latest` or `@v4` (can change)
- Always use full commit SHA

**Why:** Prevents supply chain attacks (compromised action version)

---

## 13) Checklist for maintainers

Before releasing a new version:

- [ ] All CI checks pass on main
- [ ] Changelog updated (CHANGELOG.md, if exists)
- [ ] Schema version bumped if contracts changed
- [ ] `Makefile` and CI workflow reviewed
- [ ] Tool scripts run without errors
- [ ] Dependencies audited for vulnerabilities
- [ ] Performance benchmarks reviewed (if available)

Before onboarding new contributor:

- [ ] Provide link to this guide
- [ ] Have them run `make install && make check && make test`
- [ ] Request they set up pre-commit: `pre-commit install`
- [ ] Link to ENGINEERING_INVARIANTS.md for code style
- [ ] If on Windows: explain line ending setup

---

**Questions?** See `docs/MAINTAINER_GUIDE.md` for testing strategy, known issues, and roadmap.
