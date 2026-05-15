from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any

from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import PartitionStrategy, ZephyrElement

REPOSITORY = "Zephyr-dev"
PLANNED_INPUTS = [
    "fapiao.jpeg",
    "fapiao2.jpg",
    "fapiao3.jpg",
    "hetong2-jiashuiyin.pdf",
]
EXPECTED_ELEMENT_TYPES = ["Table", "Image", "Header", "Footer", "Form", "FormKeysValues"]
METADATA_KEYS = [
    "text_as_html",
    "image_base64",
    "image_mime_type",
    "parent_id",
    "coordinates",
    "layout_width",
    "layout_height",
    "page_number",
    "languages",
    "data_source",
]


@dataclass(frozen=True, slots=True)
class BenchmarkArgs:
    input_dir: Path
    out_dir: Path
    profile: str
    strategy: PartitionStrategy | None
    json_out: bool
    markdown_out: bool
    fail_on_missing_inputs: bool


def _safe_dirname(path: Path) -> str:
    return path.name.replace(" ", "_")


def _element_dicts(elements: list[ZephyrElement]) -> list[dict[str, Any]]:
    return [
        {
            "element_id": element.element_id,
            "type": element.type,
            "text": element.text,
            "metadata": dict(element.metadata),
        }
        for element in elements
    ]


def _metadata_coverage(elements: list[dict[str, Any]]) -> dict[str, bool]:
    coverage = dict.fromkeys(METADATA_KEYS, False)
    for element in elements:
        metadata_obj = element.get("metadata")
        if not isinstance(metadata_obj, dict):
            continue
        metadata = metadata_obj
        if "text_as_html" in metadata:
            coverage["text_as_html"] = True
        if "image_base64" in metadata:
            coverage["image_base64"] = True
        if "image_mime_type" in metadata:
            coverage["image_mime_type"] = True
        if "parent_id" in metadata:
            coverage["parent_id"] = True
        coordinates_obj = metadata.get("coordinates")
        if isinstance(coordinates_obj, dict):
            coverage["coordinates"] = True
            if "layout_width" in coordinates_obj:
                coverage["layout_width"] = True
            if "layout_height" in coordinates_obj:
                coverage["layout_height"] = True
        if "layout_width" in metadata:
            coverage["layout_width"] = True
        if "layout_height" in metadata:
            coverage["layout_height"] = True
        if "page_number" in metadata:
            coverage["page_number"] = True
        if "languages" in metadata:
            coverage["languages"] = True
        if "data_source" in metadata:
            coverage["data_source"] = True
    return coverage


def _element_type_summary(elements: list[dict[str, Any]]) -> tuple[dict[str, int], dict[str, bool]]:
    counts: dict[str, int] = {}
    for element in elements:
        type_value = element.get("type")
        if not isinstance(type_value, str):
            continue
        counts[type_value] = counts.get(type_value, 0) + 1
    presence = {type_name: counts.get(type_name, 0) > 0 for type_name in EXPECTED_ELEMENT_TYPES}
    return counts, presence


def generate_report(args: BenchmarkArgs) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    missing_inputs: list[str] = []
    resolved_input_dir = args.input_dir.expanduser().resolve(strict=False)
    resolved_out_dir = args.out_dir.expanduser().resolve(strict=False)

    for planned_name in PLANNED_INPUTS:
        input_path = resolved_input_dir / planned_name
        output_root = resolved_out_dir / _safe_dirname(Path(planned_name))
        if not input_path.exists():
            missing_inputs.append(planned_name)
            results.append(
                {
                    "input_file": planned_name,
                    "profile_used": args.profile,
                    "strategy_used": None if args.strategy is None else str(args.strategy),
                    "status": "missing_input",
                    "skipped_missing_inputs": True,
                    "element_type_counts": {},
                    "element_type_presence": dict.fromkeys(EXPECTED_ELEMENT_TYPES, False),
                    "metadata_coverage": dict.fromkeys(METADATA_KEYS, False),
                    "normalized_output_path": None,
                    "elements_output_path": None,
                    "warnings": [f"missing input: {input_path}"],
                    "errors": [],
                }
            )
            continue

        output_root.mkdir(parents=True, exist_ok=True)
        warnings: list[str] = []
        try:
            result = auto_partition(
                filename=str(input_path),
                profile=args.profile,
                strategy=args.strategy,
            )
            elements_payload = _element_dicts(result.elements)
            element_counts, element_presence = _element_type_summary(elements_payload)
            metadata_coverage = _metadata_coverage(elements_payload)

            normalized_output_path = output_root / "normalized_text.txt"
            elements_output_path = output_root / "elements.json"
            normalized_output_path.write_text(result.normalized_text, encoding="utf-8")
            elements_output_path.write_text(
                json.dumps(elements_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            warnings.extend(result.warnings)
            results.append(
                {
                    "input_file": planned_name,
                    "profile_used": args.profile,
                    "strategy_used": None if args.strategy is None else str(args.strategy),
                    "status": "ok",
                    "skipped_missing_inputs": False,
                    "element_type_counts": element_counts,
                    "element_type_presence": element_presence,
                    "metadata_coverage": metadata_coverage,
                    "normalized_output_path": str(normalized_output_path),
                    "elements_output_path": str(elements_output_path),
                    "warnings": warnings,
                    "errors": [],
                }
            )
        except Exception as exc:
            results.append(
                {
                    "input_file": planned_name,
                    "profile_used": args.profile,
                    "strategy_used": None if args.strategy is None else str(args.strategy),
                    "status": "error",
                    "skipped_missing_inputs": False,
                    "element_type_counts": {},
                    "element_type_presence": dict.fromkeys(EXPECTED_ELEMENT_TYPES, False),
                    "metadata_coverage": dict.fromkeys(METADATA_KEYS, False),
                    "normalized_output_path": None,
                    "elements_output_path": None,
                    "warnings": warnings,
                    "errors": [f"{type(exc).__name__}: {exc}"],
                }
            )

    executed = [item for item in results if item["status"] == "ok"]
    error_results = [item for item in results if item["status"] == "error"]
    return {
        "repository": REPOSITORY,
        "report_id": "zephyr.dev.p6k.m1.enhanced_partition_benchmark.v1",
        "unstructured_version": version("unstructured"),
        "input_dir": str(resolved_input_dir),
        "out_dir": str(resolved_out_dir),
        "profile": args.profile,
        "strategy": None if args.strategy is None else str(args.strategy),
        "planned_inputs": list(PLANNED_INPUTS),
        "results": results,
        "summary": {
            "planned_input_count": len(PLANNED_INPUTS),
            "executed_count": len(executed),
            "error_count": len(error_results),
            "missing_inputs": missing_inputs,
            "missing_count": len(missing_inputs),
            "manual_benchmark_pending": len(missing_inputs) > 0,
            "product_grade_ready": len(missing_inputs) == 0
            and len(error_results) == 0
            and len(executed) == len(PLANNED_INPUTS),
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# P6K M1 Enhanced Partition Benchmark",
        "",
        f"- Repository: {report['repository']}",
        f"- Unstructured version: {report['unstructured_version']}",
        f"- Input dir: {report['input_dir']}",
        f"- Output dir: {report['out_dir']}",
        f"- Profile: {report['profile']}",
        f"- Strategy: {report['strategy']}",
        f"- Manual benchmark pending: {summary['manual_benchmark_pending']}",
        "",
    ]
    for result in report["results"]:
        lines.append(f"## {result['input_file']}")
        lines.append("")
        lines.append(f"- Status: {result['status']}")
        lines.append(f"- Skipped missing inputs: {result['skipped_missing_inputs']}")
        lines.append(f"- Element counts: {result['element_type_counts']}")
        lines.append(f"- Metadata coverage: {result['metadata_coverage']}")
        lines.append(f"- Normalized output: {result['normalized_output_path']}")
        lines.append(f"- Elements output: {result['elements_output_path']}")
        lines.append(f"- Warnings: {result['warnings']}")
        lines.append(f"- Errors: {result['errors']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _parse_args(argv: list[str] | None = None) -> BenchmarkArgs:
    parser = argparse.ArgumentParser(prog="p6k_m1_enhanced_partition_benchmark")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--profile", default="default")
    parser.add_argument(
        "--strategy",
        default=None,
        choices=["auto", "fast", "hi_res", "ocr_only"],
    )
    parser.add_argument("--json", action="store_true", dest="json_out", default=False)
    parser.add_argument("--markdown", action="store_true", dest="markdown_out", default=False)
    parser.add_argument("--fail-on-missing-inputs", action="store_true", default=False)
    ns = parser.parse_args(argv)
    strategy = None if ns.strategy is None else PartitionStrategy(ns.strategy)
    return BenchmarkArgs(
        input_dir=Path(ns.input_dir),
        out_dir=Path(ns.out_dir),
        profile=str(ns.profile),
        strategy=strategy,
        json_out=bool(ns.json_out),
        markdown_out=bool(ns.markdown_out),
        fail_on_missing_inputs=bool(ns.fail_on_missing_inputs),
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report = generate_report(args)
    if args.markdown_out:
        print(render_markdown(report), end="")
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if args.fail_on_missing_inputs and report["summary"]["missing_count"] > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
