.PHONY: install
install:
	uv sync --locked --all-groups --all-packages

.PHONY: lock
lock:
	uv lock --upgrade

.PHONY: tidy
tidy:
	uv run --locked --no-sync ruff format .
	uv run --locked --no-sync ruff check --fix-only --show-fixes .

.PHONY: check
check:
	uv run --locked --no-sync ruff check .
	uv run --locked --no-sync pyright
	uv run --locked --no-sync mypy packages

.PHONY: test
test:
	uv run --locked --no-sync pytest -n auto
