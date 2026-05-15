from __future__ import annotations

import ast
import inspect
import json
import re
import subprocess
from importlib import metadata
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs" / "p6k"
VALIDATION_DIR = ROOT / "validation"

REPOSITORY_NAME = "Zephyr-dev"
BASELINE_SHA = "d899f640cc9f084bc5457ea8ab725a4d4856d052"
TARGET_UNSTRUCTURED_VERSION = "0.22.28"

TARGET_PARTITION_PARAMS = [
    "languages",
    "detect_language_per_element",
    "language_fallback",
    "skip_infer_table_types",
    "infer_table_structure",
    "pdf_infer_table_structure",
    "extract_image_block_types",
    "extract_image_block_output_dir",
    "extract_image_block_to_payload",
    "data_source_metadata",
    "metadata_filename",
    "hi_res_model_name",
    "model_name",
    "starting_page_number",
    "**kwargs",
]

REQUIRED_METADATA_FIELDS = [
    "text_as_html",
    "image_base64",
    "image_mime_type",
    "parent_id",
    "is_extracted",
    "detection_class_prob",
    "coordinates.system",
    "layout_width",
    "layout_height",
    "page_number",
    "filetype",
    "filename",
    "languages",
    "data_source",
]

REQUIRED_CLI_FLAGS = [
    "--profile",
    "--languages",
    "--detect-language-per-element",
    "--skip-infer-table-types",
    "--extract-image-block-types",
    "--extract-image-block-output-dir",
    "--extract-image-block-to-payload",
    "--hi-res-model-name",
    "--starting-page-number",
    "--pdf-infer-table-structure",
    "--infer-table-structure",
]

BENCHMARK_INPUT_ROOT = r"E:\Github_Projects\Zephyr-zh-test"
BENCHMARK_OUTPUT_ROOT = r"E:\Github_Projects\Zephyr-zh-test\tmp"
BENCHMARK_INPUTS = [
    "fapiao.jpeg",
    "fapiao2.jpg",
    "fapiao3.jpg",
    "hetong2-jiashuiyin.pdf",
]

PROFILE_PLAN: dict[str, dict[str, object]] = {
    "default_profile": {
        "policy": [
            "preserve existing behavior as much as possible",
            "do not turn on all heavy features by default",
        ]
    },
    "zh_profile": {
        "languages": ["zho", "eng"],
        "detect_language_per_element": True,
    },
    "html_heavy_profile": {
        "strategy": "hi_res or auto",
        "skip_infer_table_types": [],
        "extract_image_block_types": ["Image", "Table"],
        "extract_image_block_to_payload": True,
    },
    "invoice_profile": {
        "strategy": "hi_res or auto",
        "languages": ["zho", "eng"],
        "skip_infer_table_types": [],
        "extract_image_block_types": ["Image", "Table"],
        "extract_image_block_to_payload": True,
    },
    "contract_profile": {
        "strategy": "auto or hi_res",
        "languages": ["zho", "eng"],
        "detect_language_per_element": True,
        "skip_infer_table_types": [],
        "extract_image_block_types": ["Image", "Table"],
        "extract_image_block_to_payload": True,
    },
}

EXECUTION_PLAN: list[dict[str, object]] = [
    {
        "milestone": "P6K-M0",
        "title": "Baseline Audit",
        "objective": (
            "Freeze the current Zephyr-dev kernel baseline and the next-step "
            "hardening plan."
        ),
        "input_prerequisites": ["P6 M1-M3 complete", "No dependency upgrade in scope"],
        "deliverables": [
            "P6K scope, roadmap, non-goal, and execution-plan docs",
            "Gap audit tools and validation JSON outputs",
            "Aggregate baseline audit report",
        ],
        "completion_criteria": [
            "Current kernel baseline is documented",
            "Unstructured 0.22.28 gap is documented without runtime change",
            "PackageManifest, it-stream, destination, and Base boundaries are documented",
        ],
        "dependencies": [],
        "non_goals": [
            "No runtime implementation",
            "No CLI enhancement",
            "No dependency upgrade",
        ],
        "risk": (
            "Audit can drift if later milestones change implementation without "
            "updating the frozen baseline."
        ),
        "parallelizable": False,
    },
    {
        "milestone": "P6K-M1",
        "title": "Unstructured 0.22.28 Enhanced Partition Profile",
        "objective": (
            "Upgrade to unstructured 0.22.28 and add selective enhanced "
            "partition profiles."
        ),
        "input_prerequisites": [
            "P6K-M0 audit approved",
            "Benchmark input path recorded",
        ],
        "deliverables": [
            "Version upgrade and compatibility layer",
            "Profile selection layer",
            "CLI flags for enhanced partition parameters",
            "Metadata preservation tests",
        ],
        "completion_criteria": [
            "Selective enhanced profiles work",
            "Governance metadata is preserved",
            "Base regression boundary remains intact",
        ],
        "dependencies": ["P6K-M0"],
        "non_goals": [
            "No PackageManifest yet",
            "No workflow runtime yet",
            "No commercial logic",
        ],
        "risk": (
            "Version and metadata-shape drift can regress Base bundle "
            "compatibility if not bounded."
        ),
        "parallelizable": False,
    },
    {
        "milestone": "P6K-M2",
        "title": "PackageManifestV1 / package_id / artifact descriptors",
        "objective": "Add package-aware artifact identity without changing commercial boundaries.",
        "input_prerequisites": ["P6K-M1 partition outputs stabilized"],
        "deliverables": [
            "PackageManifestV1",
            "package_id and artifact descriptor model",
            "package-aware content evidence plan",
        ],
        "completion_criteria": [
            "Artifact identity collision risk is removed",
            "Destination mapping can consume package-aware descriptors",
        ],
        "dependencies": ["P6K-M1"],
        "non_goals": ["No destination remap yet", "No workflow runtime yet"],
        "risk": (
            "Artifact layout changes can break replay and Base compatibility if "
            "wrapper paths are not preserved."
        ),
        "parallelizable": False,
    },
    {
        "milestone": "P6K-M3",
        "title": "Partitioner enhanced profiles real benchmark",
        "objective": (
            "Benchmark enhanced partition behavior against planned Chinese "
            "invoice and contract samples."
        ),
        "input_prerequisites": ["P6K-M1 complete", "Benchmark samples available locally"],
        "deliverables": ["Benchmark scripts", "Evidence snapshots", "Regression summary"],
        "completion_criteria": [
            "Expected element classes appear",
            "Metadata richness improves on target samples",
        ],
        "dependencies": ["P6K-M1"],
        "non_goals": ["No new runtime surface beyond M1"],
        "risk": "Sample variance can hide regressions if evidence is not normalized.",
        "parallelizable": True,
    },
    {
        "milestone": "P6K-M4",
        "title": "it-stream Airbyte-like structured sync hardening",
        "objective": (
            "Add bounded Airbyte-like structured sync semantics on the "
            "existing it-stream path."
        ),
        "input_prerequisites": ["P6K-M0 gap audit", "Checkpoint semantics remain bounded"],
        "deliverables": [
            "Structured sync contract layer",
            "Per-stream/per-slice state model",
            "Bounded retry and recovery semantics",
        ],
        "completion_criteria": [
            "Airbyte-like subset is explicit",
            "No uncontrolled runtime expansion occurs",
        ],
        "dependencies": ["P6K-M0"],
        "non_goals": ["No batch workflow runtime yet", "No SaaS sync platform behavior"],
        "risk": (
            "Broadening into consumer-group or platform semantics would "
            "violate current it-stream boundaries."
        ),
        "parallelizable": False,
    },
    {
        "milestone": "P6K-M5",
        "title": "Package-aware destination mapping",
        "objective": "Map future package-aware artifacts into retained destinations.",
        "input_prerequisites": ["P6K-M2"],
        "deliverables": [
            "Destination-specific mapping rules",
            "Schema/index/table/key migration plan",
            "Compatibility wrappers where needed",
        ],
        "completion_criteria": [
            "Each retained destination has a package-aware mapping plan",
            "Replay and delivery contracts remain bounded",
        ],
        "dependencies": ["P6K-M2"],
        "non_goals": ["No new destination families"],
        "risk": "Destination schema churn can fragment replay and inspection surfaces.",
        "parallelizable": True,
    },
    {
        "milestone": "P6K-M6",
        "title": "HTML Composer Node",
        "objective": "Add an internal HTML composition node after package-aware artifacts exist.",
        "input_prerequisites": ["P6K-M2"],
        "deliverables": ["HTML node contract", "Artifact mapping", "Node-level tests"],
        "completion_criteria": ["HTML node is bounded and package-aware"],
        "dependencies": ["P6K-M2"],
        "non_goals": ["No public Base HTML support"],
        "risk": "HTML behavior can blur kernel vs product boundaries if surfaced too early.",
        "parallelizable": True,
    },
    {
        "milestone": "P6K-M7",
        "title": "Chunker Node + ZephyrChunk",
        "objective": "Add typed chunk artifacts for downstream embed/search work.",
        "input_prerequisites": ["P6K-M2"],
        "deliverables": ["ZephyrChunk contract", "Chunker node", "Chunk artifact descriptors"],
        "completion_criteria": ["Chunk artifacts are package-aware and replay-safe"],
        "dependencies": ["P6K-M2"],
        "non_goals": ["No vector DB product layer"],
        "risk": "Chunk identity drift can multiply collision and replay complexity.",
        "parallelizable": True,
    },
    {
        "milestone": "P6K-M8",
        "title": "Embedder Node + EmbeddingProvider",
        "objective": "Add core embedding provider abstractions without commercial gating.",
        "input_prerequisites": ["P6K-M7"],
        "deliverables": ["EmbeddingProvider interface", "Embedder node", "Embedding artifacts"],
        "completion_criteria": ["Embeddings are typed, package-aware, and optional"],
        "dependencies": ["P6K-M7"],
        "non_goals": ["No Pro billing or quota logic"],
        "risk": (
            "Heavy dependency and model-surface creep can violate Base "
            "non-regression boundaries."
        ),
        "parallelizable": False,
    },
    {
        "milestone": "P6K-M9",
        "title": "Batch Workflow Runtime minimum",
        "objective": (
            "Introduce the minimum workflow runtime after node and package "
            "primitives exist."
        ),
        "input_prerequisites": ["P6K-M2", "P6K-M6", "P6K-M7", "P6K-M8"],
        "deliverables": ["Workflow model", "Node execution chain", "Batch runtime minimum"],
        "completion_criteria": ["Workflow execution remains bounded and replayable"],
        "dependencies": ["P6K-M2", "P6K-M6", "P6K-M7", "P6K-M8"],
        "non_goals": ["No SaaS orchestration plane"],
        "risk": (
            "Runtime expansion can reopen orchestration boundaries if "
            "introduced before package identity is stable."
        ),
        "parallelizable": False,
    },
    {
        "milestone": "P6K-M10",
        "title": "Pro/Web SDK/API surface",
        "objective": (
            "Expose the hardened kernel to future Pro/Web product layers "
            "without commercial logic in Zephyr-dev."
        ),
        "input_prerequisites": ["P6K-M9"],
        "deliverables": [
            "Stable SDK/API surfaces",
            "Compatibility wrappers",
            "Integration guidance",
        ],
        "completion_criteria": [
            "Kernel surfaces are stable and product-facing layers remain external"
        ],
        "dependencies": ["P6K-M9"],
        "non_goals": ["No Web-core entitlement or billing in Zephyr-dev"],
        "risk": "Surface leakage can collapse kernel and commercial boundaries.",
        "parallelizable": False,
    },
    {
        "milestone": "P6K-M11",
        "title": "Chinese benchmark suite",
        "objective": (
            "Expand benchmark coverage with Chinese-focused document samples "
            "and evidence capture."
        ),
        "input_prerequisites": ["P6K-M1", "P6K-M3"],
        "deliverables": ["Benchmark suite", "Evidence matrix", "Regression thresholds"],
        "completion_criteria": ["Benchmark evidence is reproducible and versioned"],
        "dependencies": ["P6K-M1", "P6K-M3"],
        "non_goals": ["No product feature expansion"],
        "risk": "Benchmark sprawl can turn into a hidden runtime contract if not scoped.",
        "parallelizable": True,
    },
    {
        "milestone": "P6K-M12",
        "title": "LLMProvider / Extract / NER / Table Description",
        "objective": (
            "Add bounded AI extraction provider contracts after workflow "
            "primitives exist."
        ),
        "input_prerequisites": ["P6K-M9"],
        "deliverables": ["Provider interfaces", "Extract/NER/table-description nodes"],
        "completion_criteria": ["Provider layer is typed, optional, and non-commercial"],
        "dependencies": ["P6K-M9"],
        "non_goals": ["No billing or entitlement logic"],
        "risk": "Model-provider coupling can leak product policy into the kernel.",
        "parallelizable": True,
    },
    {
        "milestone": "P6K-M13",
        "title": "DeepSeek-OCR2 fallback experiment",
        "objective": (
            "Run a bounded OCR fallback experiment without changing the "
            "default runtime path."
        ),
        "input_prerequisites": ["P6K-M1", "P6K-M12"],
        "deliverables": ["Experiment harness", "Evidence summary", "Boundary assessment"],
        "completion_criteria": ["Fallback remains experimental and optional"],
        "dependencies": ["P6K-M1", "P6K-M12"],
        "non_goals": ["No default OCR route change"],
        "risk": "Experimental OCR can silently broaden dependency and runtime surfaces.",
        "parallelizable": True,
    },
    {
        "milestone": "P6K-M14",
        "title": "Feishu UNS + IT sources",
        "objective": (
            "Add bounded Feishu document and structured sources after prior "
            "kernel layers are ready."
        ),
        "input_prerequisites": ["P6K-M4", "P6K-M9"],
        "deliverables": ["Feishu source contracts", "Flow-specific adapters", "Focused tests"],
        "completion_criteria": ["Feishu sources fit existing flow ownership rules"],
        "dependencies": ["P6K-M4", "P6K-M9"],
        "non_goals": ["No SaaS workspace sync platform"],
        "risk": "Feishu source breadth can blur structured vs document flow boundaries.",
        "parallelizable": False,
    },
]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: object) -> None:
    ensure_parent(path)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def read_text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def git_output(*args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip()


def current_branch() -> str:
    return git_output("branch", "--show-current") or "unknown"


def current_head_sha() -> str:
    return git_output("rev-parse", "HEAD") or "unknown"


def parse_ast(relative_path: str) -> ast.Module:
    return ast.parse(read_text(relative_path), filename=relative_path)


def _iter_named_functions(module: ast.Module) -> list[ast.FunctionDef]:
    functions: list[ast.FunctionDef] = []
    for node in module.body:
        if isinstance(node, ast.FunctionDef):
            functions.append(node)
            continue
        if isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, ast.FunctionDef):
                    functions.append(child)
    return functions


def function_parameter_names(relative_path: str, function_name: str) -> list[str]:
    module = parse_ast(relative_path)
    for node in _iter_named_functions(module):
        if node.name == function_name:
            names = [arg.arg for arg in node.args.args]
            names.extend(arg.arg for arg in node.args.kwonlyargs)
            return names
    raise ValueError(f"Function {function_name!r} not found in {relative_path}")


def function_has_var_keyword(relative_path: str, function_name: str) -> bool:
    module = parse_ast(relative_path)
    for node in _iter_named_functions(module):
        if node.name == function_name:
            return node.args.kwarg is not None
    raise ValueError(f"Function {function_name!r} not found in {relative_path}")


def file_contains(relative_path: str, needle: str) -> bool:
    return needle in read_text(relative_path)


def argparse_flags(relative_path: str) -> list[str]:
    flags = set(re.findall(r"--[a-z0-9][a-z0-9-]*", read_text(relative_path)))
    return sorted(flags)


def current_unstructured_version() -> str:
    try:
        return metadata.version("unstructured")
    except metadata.PackageNotFoundError:
        return "not_installed"


def current_partition_signature() -> str:
    from unstructured.partition.auto import partition

    return str(inspect.signature(partition))


def current_pdf_partition_signature() -> str:
    from unstructured.partition.pdf import partition_pdf

    return str(inspect.signature(partition_pdf))


def current_image_partition_signature() -> str:
    from unstructured.partition.image import partition_image

    return str(inspect.signature(partition_image))


def explicit_metadata_test_coverage() -> list[str]:
    text = read_text("packages/uns-stream/src/uns_stream/tests/test_metadata_normalize.py")
    covered: list[str] = []
    if 'out["is_extracted"]' in text:
        covered.append("is_extracted")
    return covered


def benchmark_plan() -> dict[str, object]:
    return {
        "input_root": BENCHMARK_INPUT_ROOT,
        "inputs": list(BENCHMARK_INPUTS),
        "output_root": BENCHMARK_OUTPUT_ROOT,
        "requires_path_to_exist_in_m0": False,
        "expected_evidence": [
            "Table",
            "Image",
            "Header",
            "Footer",
            "Form or FormKeysValues if locally supported",
            "text_as_html",
            "image_base64",
            "image_mime_type",
            "parent_id",
            "more complete coordinates and layout metadata",
        ],
    }


def execution_plan_report() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_id": "zephyr.dev.p6k.m0.execution_plan.v1",
        "repository": REPOSITORY_NAME,
        "branch": current_branch(),
        "baseline_sha": BASELINE_SHA,
        "milestones": EXECUTION_PLAN,
        "next_milestone": "P6K-M1",
    }


def render_execution_plan_markdown() -> str:
    lines = [
        "# P6K Execution Plan",
        "",
        "P6K is a Zephyr-dev core hardening line for future Pro/Web common-kernel needs.",
        "It does not modify Zephyr-base and does not add commercial logic.",
        "",
    ]
    for item in EXECUTION_PLAN:
        lines.append(f"## {item['milestone']}: {item['title']}")
        lines.append("")
        lines.append(f"- Objective: {item['objective']}")
        lines.append(
            "- Input prerequisites: "
            + ", ".join(cast(list[str], item["input_prerequisites"]))
        )
        lines.append(f"- Deliverables: {', '.join(cast(list[str], item['deliverables']))}")
        lines.append(
            f"- Completion criteria: {', '.join(cast(list[str], item['completion_criteria']))}"
        )
        dependencies = cast(list[str], item["dependencies"])
        lines.append(f"- Dependencies: {', '.join(dependencies) if dependencies else 'None'}")
        lines.append(f"- Non-goals: {', '.join(cast(list[str], item['non_goals']))}")
        lines.append(f"- Risk: {item['risk']}")
        lines.append(f"- Parallelizable: {'yes' if item['parallelizable'] else 'no'}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
