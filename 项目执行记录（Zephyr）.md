# 项目执行记录（Zephyr）


## 第一阶段

### Phase0
初始阶段：
uv init

然后我的Zephyr根目录下的 pyproject.toml：

[project]
name = "zephyr"
version = "0.1.0"
description = "Zephyr Intelligent Data Extraction Pipeline"
readme = "README.md"
requires-python = ">=3.12"

[tool.uv.workspace]
members = ["packages/*"]

```
# 1. 创建存放子包的集装箱目录
mkdir packages
cd packages

# 2. 初始化核心层和 I/O 层
# (注：使用 --lib 参数是因为它们是库，不是可以直接运行的 App)
uv init --lib zephyr-core
uv init --lib zephyr-ingest

# 3. 初始化 API 层
uv init --lib zephyr-api
cd zephyr-api

# 4. 为 API 层添加 FastAPI 依赖
uv add fastapi uvicorn

# 5. 将核心层和 I/O 层作为本地依赖，链接到 API 层
uv add --editable ../zephyr-core
uv add --editable ../zephyr-ingest
```

uv sync

uv sync --all-packages


开始P0：
.python-version     3.12

Zephyr根目录命令：cd Zephyr    和echo "3.12" > .python-version

根目录下pyproject.toml:
[project]
name = "zephyr"
version = "0.1.0"
description = "Zephyr Intelligent Data Extraction Pipeline"
readme = "README.md"
requires-python = ">=3.12"
dependencies = []

[dependency-groups]
lint = [
  "ruff>=0.15.0,<1.0.0",
  "mypy>=1.19.1,<2.0.0",
  "types-requests>=2.32.4,<3.0.0",
  "pyright>=1.1.0",
]
test = [
  { include-group = "lint" },
  "pytest>=9.0.0,<10.0.0",
  "pytest-cov>=7.0.0,<8.0.0",
  "pytest-xdist>=3.8.0,<4.0.0",
]
dev = [
  "pre-commit>=4.0.0,<5.0.0",
]

[tool.uv.workspace]
members = ["packages/*"]

# 参考 unstructured / unstructured-api：pyright 严格模式写在 pyproject <!--citation:1-->
[tool.pyright]
pythonPlatform = "Linux"
pythonVersion = "3.12"
typeCheckingMode = "strict"
stubPath = "./typings"
reportUnnecessaryCast = true
reportUnnecessaryTypeIgnoreComment = true

# 针对 monorepo：明确扫描范围（避免把无关目录也扫进来）
include = ["packages/*/src"]
exclude = ["**/.venv", "**/dist", "**/build", "**/.mypy_cache", "**/.ruff_cache"]

[tool.pytest.ini_options]
testpaths = ["packages"]
addopts = "-q"

[tool.mypy]
python_version = "3.12"
mypy_path = "typings"
namespace_packages = true
explicit_package_bases = true
warn_unused_ignores = true
no_implicit_optional = true
# 先别把历史/搬运代码逼死：初期允许缺失导入，后续再收紧
ignore_missing_imports = true

根目录命令：uv lock   和uv sync --all-groups --all-packages

根目录makefile：
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

根目录.pre-commit-config.yaml:
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: check-added-large-files
      - id: check-toml
      - id: check-yaml
      - id: check-json
      - id: end-of-file-fixer
      - id: trailing-whitespace
      - id: mixed-line-ending

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.15.0
    hooks:
      - id: ruff
        args: ["--fix"]
      - id: ruff-format

根目录命令：pre-commit install 和pre-commit run -a

根目录创建了.github/workflows/ci.yml：
name: CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5

      - name: Read Python version from .python-version
        run: echo "PYTHON_VERSION=$(cat .python-version)" >> $GITHUB_ENV

      - name: Install uv
        uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true
          cache-dependency-glob: "uv.lock"

      - name: Set up Python
        run: uv python install ${{ env.PYTHON_VERSION }}

      - name: Install deps
        run: uv sync --locked --all-groups --all-packages

      - name: Lint & Typecheck
        run: make check

      - name: Test
        run: make test

根目录创建 typings/README.md:
# typings

Third-party stub files for libraries that:
- have no typing information, or
- ship incomplete typings, causing pyright strict failures.

Rule:
- Prefer upstream type packages first (types-requests, etc.).
- Only add stubs here when necessary.
- Each stub file should include a short comment with why it exists.

进入 packages/zephyr-api并执行命令：uv remove zephyr-core zephyr-ingest 和uv add zephyr-core zephyr-ingest

根目录命令：uv lock 和uv sync --locked --all-groups --all-packages

packages/zephyr-api中的pyproject.toml变为:
[project]
name = "zephyr-api"
version = "0.1.0"
description = "Add your description here"
readme = "README.md"
authors = [
    { name = "111", email = "..." }
]
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.135.1",
    "uvicorn>=0.41.0",
    "zephyr-core",
    "zephyr-ingest",
]

[build-system]
requires = ["uv_build>=0.10.8,<0.11.0"]
build-backend = "uv_build"

[tool.uv.sources]
zephyr-core = { workspace = true }
zephyr-ingest = { workspace = true }
