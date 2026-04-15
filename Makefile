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
P45_HEALTHCHECK = uv run --locked --no-sync python tools/p45_substrate_healthcheck.py
P45_RUNTIME_HOME = $(shell uv run --locked --no-sync python tools/p45_substrate_healthcheck.py --print-runtime-home)
P45_COMPOSE_PATH = $(shell uv run --locked --no-sync python tools/p45_substrate_healthcheck.py --print-compose-path)

.PHONY: p45-substrate-up
p45-substrate-up:
	@echo P4.5 runtime home: $(P45_RUNTIME_HOME)
	@echo P4.5 compose file: $(P45_COMPOSE_PATH)
	docker compose \
  		--env-file "$(P45_RUNTIME_HOME)\env\.env.p45.local" \
  		-f "$(P45_COMPOSE_PATH)" \
  		up -d --quiet-pull

.PHONY: p45-substrate-down
p45-substrate-down:
	@echo P4.5 runtime home: $(P45_RUNTIME_HOME)
	@echo P4.5 compose file: $(P45_COMPOSE_PATH)
	#docker compose -f "$(P45_COMPOSE_PATH)" down --remove-orphans
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
