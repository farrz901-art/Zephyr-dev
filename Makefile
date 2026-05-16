.PHONY: install
install:
	uv sync --locked --all-groups --all-packages

.PHONY: install-all
install-all:
	uv sync --locked --all-groups --all-packages --all-extras

.PHONY: install-full
install-full: install-all

.PHONY: install-dev-full
install-dev-full: install-all

.PHONY: lock
lock:
	uv lock --upgrade

.PHONY: tidy
tidy:
	uv run --locked --no-sync ruff format packages
	uv run --locked --no-sync ruff check --fix-only --show-fixes packages

PYTEST_MARKER_EXCLUSION = not auth_local_real and not auth_service_live and not auth_recovery_drill and not slow
P45_PYTEST = uv run --locked --no-sync pytest -n 0
P45_HEALTHCHECK = uv run --locked --no-sync python tools/p45_substrate_healthcheck.py
P45_RUNTIME_HOME = $(shell uv run --locked --no-sync python tools/p45_substrate_healthcheck.py --print-runtime-home)
P45_ENV_DIR = $(shell uv run --locked --no-sync python tools/p45_substrate_healthcheck.py --print-env-dir)
P45_COMPOSE_PATH = $(shell uv run --locked --no-sync python tools/p45_substrate_healthcheck.py --print-compose-path)
P45_COMPOSE_CONFIG = uv run --locked --no-sync python tools/p45_auth_compose_config.py

.PHONY: check-format
check-format:
	uv run --locked --no-sync ruff format --check packages

.PHONY: lint
lint:
	uv run --locked --no-sync ruff check packages

.PHONY: typecheck
typecheck:
	uv run --locked --no-sync pyright
	uv run --locked --no-sync mypy packages

.PHONY: check-tools
check-tools:
	uv run --locked --no-sync python -c "import glob, py_compile; [py_compile.compile(p, doraise=True) for p in glob.glob('tools/*.py')]"

.PHONY: check
check: check-format lint typecheck check-tools

.PHONY: test-core
test-core:
	uv run --locked --no-sync python tools/pytest_target.py --marker-expr "$(PYTEST_MARKER_EXCLUSION)" --pytest-arg=--basetemp=.pytest_tmp_artifacts/core

.PHONY: test-contracts
test-contracts:
	uv run --locked --no-sync python tools/pytest_target.py --allow-no-tests --marker-expr "(contract) and ($(PYTEST_MARKER_EXCLUSION))" --pytest-arg=-n --pytest-arg=0 --pytest-arg=--basetemp=.pytest_tmp_artifacts/contracts

.PHONY: test-fixtures
test-fixtures:
	uv run --locked --no-sync python tools/pytest_target.py --allow-no-tests --marker-expr "(fixture) and ($(PYTEST_MARKER_EXCLUSION))" --pytest-arg=-n --pytest-arg=0 --pytest-arg=--basetemp=.pytest_tmp_artifacts/fixtures

.PHONY: test-uns
test-uns:
	uv run --locked --no-sync python tools/pytest_target.py --allow-no-tests --marker-expr "(uns) and ($(PYTEST_MARKER_EXCLUSION))" --pytest-arg=-n --pytest-arg=0 --pytest-arg=--basetemp=.pytest_tmp_artifacts/uns

.PHONY: test-it
test-it:
	uv run --locked --no-sync python tools/pytest_target.py --allow-no-tests --marker-expr "(it) and ($(PYTEST_MARKER_EXCLUSION))" --pytest-arg=-n --pytest-arg=0 --pytest-arg=--basetemp=.pytest_tmp_artifacts/it

.PHONY: test-connectors
test-connectors:
	uv run --locked --no-sync python tools/pytest_target.py --allow-no-tests --marker-expr "(connector) and ($(PYTEST_MARKER_EXCLUSION))" --pytest-arg=-n --pytest-arg=0 --pytest-arg=--basetemp=.pytest_tmp_artifacts/connectors

.PHONY: test
test: test-core

.PHONY: build-smoke
build-smoke:
	uv build --all-packages

.PHONY: offline-proof
offline-proof:
	$(MAKE) check-tools
	uv run --locked --no-sync python tools/p6k_m0_baseline_report.py

.PHONY: p45-substrate-up
p45-substrate-up:
	@echo P4.5 runtime home: $(P45_RUNTIME_HOME)
	@echo P4.5 runtime env dir: $(P45_ENV_DIR)
	@echo P4.5 compose file: $(P45_COMPOSE_PATH)
	docker compose \
  		--env-file "$(P45_RUNTIME_HOME)\env\.env.p45.local" \
  		-f "$(P45_COMPOSE_PATH)" \
  		up -d --quiet-pull

.PHONY: p45-substrate-down
p45-substrate-down:
	@echo P4.5 runtime home: $(P45_RUNTIME_HOME)
	@echo P4.5 runtime env dir: $(P45_ENV_DIR)
	@echo P4.5 compose file: $(P45_COMPOSE_PATH)
	docker compose \
	  --env-file "$(P45_RUNTIME_HOME)\env\.env.p45.local" \
	  -f "$(P45_COMPOSE_PATH)" \
	  down --remove-orphans

.PHONY: p45-substrate-healthcheck
p45-substrate-healthcheck:
	$(P45_HEALTHCHECK) --tier all

.PHONY: p45-test-local-real
p45-test-local-real:
	$(P45_PYTEST) --auth-tier local-real -m auth_local_real

.PHONY: p45-test-service-live
p45-test-service-live:
	$(P45_PYTEST) --auth-tier service-live -m auth_service_live

.PHONY: p45-test-recovery-drill
p45-test-recovery-drill:
	$(P45_PYTEST) --auth-tier recovery-drill -m auth_recovery_drill

.PHONY: p45-auth-substrate-up
p45-auth-substrate-up: p45-substrate-up

.PHONY: p45-auth-substrate-down
p45-auth-substrate-down: p45-substrate-down

.PHONY: p45-auth-substrate-healthcheck
p45-auth-substrate-healthcheck: p45-substrate-healthcheck

.PHONY: p45-auth-test-local-real
p45-auth-test-local-real: p45-test-local-real

.PHONY: p45-auth-test-service-live
p45-auth-test-service-live: p45-test-service-live

.PHONY: p45-auth-test-recovery-drill
p45-auth-test-recovery-drill: p45-test-recovery-drill

.PHONY: p45-auth-compose-config
p45-auth-compose-config:
	$(P45_COMPOSE_CONFIG)

.PHONY: p45-auth-evidence-proof
p45-auth-evidence-proof:
	$(P45_PYTEST) -m auth_contract
