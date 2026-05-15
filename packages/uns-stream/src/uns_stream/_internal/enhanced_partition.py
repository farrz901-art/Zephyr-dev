from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final, Literal, Mapping, cast

from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError

PartitionProfileName = Literal["default", "zh", "html_heavy", "invoice", "contract"]

_UNSET: Final = object()
UNSET_PARTITION_OPTION: Final = _UNSET

_DIRECT_KNOWN_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "profile",
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
        "coordinates",
        "include_page_breaks",
    }
)


class PartitionOptionKind(str, Enum):
    DIRECT = "direct"
    EXTRA = "extra"


def _new_object_dict() -> dict[str, object]:
    return {}


@dataclass(frozen=True, slots=True)
class ResolvedPartitionOptions:
    profile: PartitionProfileName
    strategy: PartitionStrategy | None
    option_values: dict[str, object] = field(default_factory=_new_object_dict)
    extra_partition_kwargs: dict[str, object] = field(default_factory=_new_object_dict)

    def merged_backend_kwargs(self) -> dict[str, object]:
        merged: dict[str, object] = dict(self.option_values)
        merged.update(self.extra_partition_kwargs)
        return merged


@dataclass(frozen=True, slots=True)
class PartitionOptionSpec:
    profile: PartitionProfileName = "default"
    strategy: PartitionStrategy | None = None
    languages: list[str] | None = None
    detect_language_per_element: bool | None = None
    language_fallback: object | None = None
    skip_infer_table_types: list[str] | None = None
    infer_table_structure: bool | None = None
    extract_image_block_types: list[str] | None = None
    extract_image_block_output_dir: str | None = None
    extract_image_block_to_payload: bool | None = None
    data_source_metadata: object | None = None
    metadata_filename: str | None = None
    hi_res_model_name: str | None = None
    model_name: str | None = None
    starting_page_number: int | None = None
    extra_partition_kwargs: dict[str, object] = field(default_factory=_new_object_dict)


_PROFILE_SPECS: Final[dict[PartitionProfileName, PartitionOptionSpec]] = {
    "default": PartitionOptionSpec(profile="default"),
    "zh": PartitionOptionSpec(
        profile="zh",
        languages=["zho", "eng"],
        detect_language_per_element=True,
    ),
    "html_heavy": PartitionOptionSpec(
        profile="html_heavy",
        strategy=PartitionStrategy.HI_RES,
        skip_infer_table_types=[],
        extract_image_block_types=["Image", "Table"],
        extract_image_block_to_payload=True,
    ),
    "invoice": PartitionOptionSpec(
        profile="invoice",
        strategy=PartitionStrategy.HI_RES,
        languages=["zho", "eng"],
        skip_infer_table_types=[],
        extract_image_block_types=["Image", "Table"],
        extract_image_block_to_payload=True,
    ),
    "contract": PartitionOptionSpec(
        profile="contract",
        strategy=PartitionStrategy.AUTO,
        languages=["zho", "eng"],
        detect_language_per_element=True,
        skip_infer_table_types=[],
        extract_image_block_types=["Image", "Table"],
        extract_image_block_to_payload=True,
    ),
}


def direct_known_partition_fields() -> frozenset[str]:
    return _DIRECT_KNOWN_FIELDS


def supported_partition_profiles() -> tuple[PartitionProfileName, ...]:
    return tuple(_PROFILE_SPECS.keys())


def _coerce_profile(value: str | None) -> PartitionProfileName:
    profile_name = "default" if value is None else value
    if profile_name not in _PROFILE_SPECS:
        raise ZephyrError(
            code=ErrorCode.UNS_PARTITION_FAILED,
            message=f"Unsupported partition profile: {profile_name}",
            details={
                "retryable": False,
                "profile": profile_name,
                "supported_profiles": list(_PROFILE_SPECS.keys()),
            },
        )
    return profile_name


def _normalize_str_list(value: object, *, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ZephyrError(
            code=ErrorCode.UNS_PARTITION_FAILED,
            message=f"{field_name} must be list[str]",
            details={"retryable": False, "field": field_name},
        )
    items = cast("list[object]", value)
    for item in items:
        if not isinstance(item, str):
            raise ZephyrError(
                code=ErrorCode.UNS_PARTITION_FAILED,
                message=f"{field_name} must be list[str]",
                details={"retryable": False, "field": field_name},
            )
    return cast("list[str]", items)


def _normalize_extra_partition_kwargs(value: Mapping[str, object] | None) -> dict[str, object]:
    if value is None:
        return {}
    out: dict[str, object] = {}
    for key in value:
        if key in _DIRECT_KNOWN_FIELDS:
            raise ZephyrError(
                code=ErrorCode.UNS_PARTITION_FAILED,
                message=f"extra_partition_kwargs cannot shadow known field '{key}'",
                details={
                    "retryable": False,
                    "field": key,
                    "kind": PartitionOptionKind.EXTRA.value,
                },
            )
        out[str(key)] = value[key]
    return out


def _coerce_int_option(*, field_name: str, value: object) -> int:
    if isinstance(value, bool):
        raise ZephyrError(
            code=ErrorCode.UNS_PARTITION_FAILED,
            message=f"{field_name} must be an integer",
            details={"retryable": False, "field": field_name},
        )
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise ZephyrError(
                code=ErrorCode.UNS_PARTITION_FAILED,
                message=f"{field_name} must be an integer",
                details={"retryable": False, "field": field_name},
            ) from exc
    raise ZephyrError(
        code=ErrorCode.UNS_PARTITION_FAILED,
        message=f"{field_name} must be an integer",
        details={"retryable": False, "field": field_name},
    )


def validate_unknown_partition_kwargs(raw_partition_kwargs: Mapping[str, object]) -> None:
    if not raw_partition_kwargs:
        return
    unknown_keys = sorted(raw_partition_kwargs.keys())
    raise ZephyrError(
        code=ErrorCode.UNS_PARTITION_FAILED,
        message=(
            "Unknown partition kwargs are not accepted directly; use explicit fields "
            "or extra_partition_kwargs"
        ),
        details={
            "retryable": False,
            "unknown_partition_kwargs": unknown_keys,
            "direct_known_fields": sorted(_DIRECT_KNOWN_FIELDS),
        },
    )


def build_explicit_partition_kwargs(
    *,
    profile: str | None = None,
    languages: list[str] | None = None,
    detect_language_per_element: bool | None = None,
    language_fallback: object | None = None,
    skip_infer_table_types: list[str] | None = None,
    infer_table_structure: bool | None = None,
    pdf_infer_table_structure: bool | None = None,
    extract_image_block_types: list[str] | None = None,
    extract_image_block_output_dir: str | None = None,
    extract_image_block_to_payload: bool | None = None,
    data_source_metadata: object | None = None,
    metadata_filename: str | None = None,
    hi_res_model_name: str | None = None,
    model_name: str | None = None,
    starting_page_number: int | None = None,
    extra_partition_kwargs: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    partition_kwargs: dict[str, Any] = {}
    if profile is not None:
        partition_kwargs["profile"] = profile
    if languages is not None:
        partition_kwargs["languages"] = languages
    if detect_language_per_element is not None:
        partition_kwargs["detect_language_per_element"] = detect_language_per_element
    if language_fallback is not None:
        partition_kwargs["language_fallback"] = language_fallback
    if skip_infer_table_types is not None:
        partition_kwargs["skip_infer_table_types"] = skip_infer_table_types
    if infer_table_structure is not None:
        partition_kwargs["infer_table_structure"] = infer_table_structure
    if pdf_infer_table_structure is not None:
        partition_kwargs["pdf_infer_table_structure"] = pdf_infer_table_structure
    if extract_image_block_types is not None:
        partition_kwargs["extract_image_block_types"] = extract_image_block_types
    if extract_image_block_output_dir is not None:
        partition_kwargs["extract_image_block_output_dir"] = extract_image_block_output_dir
    if extract_image_block_to_payload is not None:
        partition_kwargs["extract_image_block_to_payload"] = extract_image_block_to_payload
    if data_source_metadata is not None:
        partition_kwargs["data_source_metadata"] = data_source_metadata
    if metadata_filename is not None:
        partition_kwargs["metadata_filename"] = metadata_filename
    if hi_res_model_name is not None:
        partition_kwargs["hi_res_model_name"] = hi_res_model_name
    if model_name is not None:
        partition_kwargs["model_name"] = model_name
    if starting_page_number is not None:
        partition_kwargs["starting_page_number"] = starting_page_number
    if extra_partition_kwargs is not None:
        partition_kwargs["extra_partition_kwargs"] = dict(extra_partition_kwargs)
    return partition_kwargs


def resolve_partition_options(
    *,
    profile: str | None,
    strategy: PartitionStrategy | None,
    languages: object = _UNSET,
    detect_language_per_element: object = _UNSET,
    language_fallback: object = _UNSET,
    skip_infer_table_types: object = _UNSET,
    infer_table_structure: object = _UNSET,
    pdf_infer_table_structure: object = _UNSET,
    extract_image_block_types: object = _UNSET,
    extract_image_block_output_dir: object = _UNSET,
    extract_image_block_to_payload: object = _UNSET,
    data_source_metadata: object = _UNSET,
    metadata_filename: object = _UNSET,
    hi_res_model_name: object = _UNSET,
    model_name: object = _UNSET,
    starting_page_number: object = _UNSET,
    extra_partition_kwargs: Mapping[str, object] | None = None,
) -> ResolvedPartitionOptions:
    resolved_profile = _coerce_profile(profile)
    profile_defaults = _PROFILE_SPECS[resolved_profile]

    resolved_strategy = profile_defaults.strategy if strategy is None else strategy
    resolved_kwargs: dict[str, object] = {}

    def _pick(explicit_value: object, profile_value: object) -> object:
        if explicit_value is _UNSET:
            return profile_value
        return explicit_value

    languages_value = _pick(languages, profile_defaults.languages)
    if languages_value is not None and languages_value is not _UNSET:
        resolved_kwargs["languages"] = _normalize_str_list(
            languages_value,
            field_name="languages",
        )

    detect_value = _pick(
        detect_language_per_element,
        profile_defaults.detect_language_per_element,
    )
    if detect_value is not None and detect_value is not _UNSET:
        resolved_kwargs["detect_language_per_element"] = bool(detect_value)

    fallback_value = _pick(
        language_fallback,
        profile_defaults.language_fallback,
    )
    if fallback_value is not None and fallback_value is not _UNSET:
        resolved_kwargs["language_fallback"] = fallback_value

    skip_value = _pick(
        skip_infer_table_types,
        profile_defaults.skip_infer_table_types,
    )
    if skip_value is not None and skip_value is not _UNSET:
        resolved_kwargs["skip_infer_table_types"] = _normalize_str_list(
            skip_value,
            field_name="skip_infer_table_types",
        )

    local_infer: bool | None = None
    if infer_table_structure is not _UNSET and infer_table_structure is not None:
        local_infer = bool(infer_table_structure)
    if pdf_infer_table_structure is not _UNSET and pdf_infer_table_structure is not None:
        pdf_infer = bool(pdf_infer_table_structure)
        if local_infer is not None and local_infer != pdf_infer:
            raise ZephyrError(
                code=ErrorCode.UNS_PARTITION_FAILED,
                message=(
                    "infer_table_structure and pdf_infer_table_structure conflict; "
                    "provide only one value or keep them equal"
                ),
                details={
                    "retryable": False,
                    "infer_table_structure": local_infer,
                    "pdf_infer_table_structure": pdf_infer,
                },
            )
        local_infer = pdf_infer
    if local_infer is not None:
        resolved_kwargs["infer_table_structure"] = local_infer

    extract_types_value = _pick(
        extract_image_block_types,
        profile_defaults.extract_image_block_types,
    )
    if extract_types_value is not None and extract_types_value is not _UNSET:
        resolved_kwargs["extract_image_block_types"] = _normalize_str_list(
            extract_types_value,
            field_name="extract_image_block_types",
        )

    output_dir_value = _pick(
        extract_image_block_output_dir,
        profile_defaults.extract_image_block_output_dir,
    )
    if output_dir_value is not None and output_dir_value is not _UNSET:
        resolved_kwargs["extract_image_block_output_dir"] = str(output_dir_value)

    to_payload_value = _pick(
        extract_image_block_to_payload,
        profile_defaults.extract_image_block_to_payload,
    )
    if to_payload_value is not None and to_payload_value is not _UNSET:
        resolved_kwargs["extract_image_block_to_payload"] = bool(to_payload_value)

    data_source_metadata_value = _pick(
        data_source_metadata,
        profile_defaults.data_source_metadata,
    )
    if data_source_metadata_value is not None and data_source_metadata_value is not _UNSET:
        resolved_kwargs["data_source_metadata"] = data_source_metadata_value

    metadata_filename_value = _pick(
        metadata_filename,
        profile_defaults.metadata_filename,
    )
    if metadata_filename_value is not None and metadata_filename_value is not _UNSET:
        resolved_kwargs["metadata_filename"] = str(metadata_filename_value)

    hi_res_model_value = _pick(
        hi_res_model_name,
        profile_defaults.hi_res_model_name,
    )
    if hi_res_model_value is not None and hi_res_model_value is not _UNSET:
        resolved_kwargs["hi_res_model_name"] = str(hi_res_model_value)

    model_name_value = _pick(
        model_name,
        profile_defaults.model_name,
    )
    if model_name_value is not None and model_name_value is not _UNSET:
        resolved_kwargs["model_name"] = str(model_name_value)

    starting_page_value = _pick(
        starting_page_number,
        profile_defaults.starting_page_number,
    )
    if starting_page_value is not None and starting_page_value is not _UNSET:
        resolved_kwargs["starting_page_number"] = _coerce_int_option(
            field_name="starting_page_number",
            value=starting_page_value,
        )

    normalized_extra = _normalize_extra_partition_kwargs(extra_partition_kwargs)
    return ResolvedPartitionOptions(
        profile=resolved_profile,
        strategy=resolved_strategy,
        option_values=resolved_kwargs,
        extra_partition_kwargs=normalized_extra,
    )
