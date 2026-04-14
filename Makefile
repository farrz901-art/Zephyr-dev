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
	uv run --locked --no-sync ruff format --check .
	uv run --locked --no-sync ruff check .
	uv run --locked --no-sync pyright
	uv run --locked --no-sync mypy packages
	uv run --locked --no-sync python -c "import glob, py_compile; [py_compile.compile(p, doraise=True) for p in glob.glob('tools/*.py')]"

.PHONY: test
test:
	uv run --locked --no-sync pytest -n auto

P45_PYTEST = uv run --locked --no-sync pytest -n 0

.PHONY: p45-substrate-up
p45-substrate-up:
	docker compose -f docker-compose.p45-validation.yml up -d

.PHONY: p45-substrate-down
p45-substrate-down:
	docker compose -f docker-compose.p45-validation.yml down --remove-orphans

.PHONY: p45-substrate-healthcheck
p45-substrate-healthcheck:
	uv run --locked --no-sync python tools/p45_substrate_healthcheck.py --tier all

.PHONY: p45-test-local-real
p45-test-local-real:
	$(P45_PYTEST) --auth-tier local-real -m auth_local_real

.PHONY: p45-test-service-live
p45-test-service-live:
	$(P45_PYTEST) --auth-tier service-live -m auth_service_live

.PHONY: p45-test-recovery-drill
p45-test-recovery-drill:
	$(P45_PYTEST) --auth-tier recovery-drill -m auth_recovery_drill
