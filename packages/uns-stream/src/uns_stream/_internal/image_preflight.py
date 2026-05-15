from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator, Mapping

from PIL import Image, ImageOps

_PREFLIGHT_WARNING_KIND = "zephyr_image_preflight_v1"
_IMAGE_EXTRACTION_KEYS = frozenset(
    {
        "extract_image_block_types",
        "extract_image_block_output_dir",
        "extract_image_block_to_payload",
    }
)
_JPEG_UNSAFE_MODES = frozenset({"1", "P", "PA", "RGBA", "LA"})


@dataclass(frozen=True, slots=True)
class ImageProbe:
    path: Path
    file_exists: bool
    file_size: int | None
    image_format: str | None
    image_mode: str | None
    image_dimensions: tuple[int, int] | None
    exif_orientation: int | None


@dataclass(frozen=True, slots=True)
class PreparedImageInput:
    path: Path
    metadata_filename: str | None
    warning: str | None
    applied: bool
    normalized_image_used: bool
    image_preflight_reason: str | None
    original_image_mode: str | None
    normalized_image_mode: str | None
    image_format: str | None


def image_preflight_warning_kind() -> str:
    return _PREFLIGHT_WARNING_KIND


def probe_image(path: Path) -> ImageProbe:
    if not path.exists():
        return ImageProbe(
            path=path,
            file_exists=False,
            file_size=None,
            image_format=None,
            image_mode=None,
            image_dimensions=None,
            exif_orientation=None,
        )

    try:
        with Image.open(path) as image:
            exif_orientation: int | None = None
            try:
                exif_value = image.getexif().get(274)
                if isinstance(exif_value, int):
                    exif_orientation = exif_value
            except Exception:
                exif_orientation = None

            return ImageProbe(
                path=path,
                file_exists=True,
                file_size=path.stat().st_size,
                image_format=image.format,
                image_mode=image.mode,
                image_dimensions=(image.width, image.height),
                exif_orientation=exif_orientation,
            )
    except Exception:
        return ImageProbe(
            path=path,
            file_exists=True,
            file_size=path.stat().st_size,
            image_format=None,
            image_mode=None,
            image_dimensions=None,
            exif_orientation=None,
        )


def image_extraction_enabled(partition_kwargs: Mapping[str, object]) -> bool:
    for key in _IMAGE_EXTRACTION_KEYS:
        value = partition_kwargs.get(key)
        if key == "extract_image_block_to_payload":
            if value is True:
                return True
            continue
        if value is not None:
            return True
    return False


def needs_image_preflight(*, probe: ImageProbe, partition_kwargs: Mapping[str, object]) -> bool:
    if not probe.file_exists:
        return False
    if not image_extraction_enabled(partition_kwargs):
        return False
    return probe.image_mode in _JPEG_UNSAFE_MODES


def _build_preflight_warning(
    *,
    reason: str,
    original_mode: str | None,
    normalized_mode: str | None,
    image_format: str | None,
) -> str:
    payload = {
        "kind": _PREFLIGHT_WARNING_KIND,
        "image_preflight_applied": True,
        "image_preflight_reason": reason,
        "normalized_image_used": True,
        "original_image_mode": original_mode,
        "normalized_image_mode": normalized_mode,
        "image_format": image_format,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


@contextmanager
def prepared_image_input(
    *, original_path: Path, metadata_filename: str | None, partition_kwargs: Mapping[str, object]
) -> Iterator[PreparedImageInput]:
    if not image_extraction_enabled(partition_kwargs):
        yield PreparedImageInput(
            path=original_path,
            metadata_filename=metadata_filename,
            warning=None,
            applied=False,
            normalized_image_used=False,
            image_preflight_reason=None,
            original_image_mode=None,
            normalized_image_mode=None,
            image_format=None,
        )
        return

    probe = probe_image(original_path)
    if not needs_image_preflight(probe=probe, partition_kwargs=partition_kwargs):
        yield PreparedImageInput(
            path=original_path,
            metadata_filename=metadata_filename,
            warning=None,
            applied=False,
            normalized_image_used=False,
            image_preflight_reason=None,
            original_image_mode=probe.image_mode,
            normalized_image_mode=probe.image_mode,
            image_format=probe.image_format,
        )
        return

    with TemporaryDirectory(prefix="zephyr-uns-image-preflight-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        temp_path = temp_dir / f"{original_path.stem}.png"
        with Image.open(original_path) as source_image:
            transposed = ImageOps.exif_transpose(source_image)
            normalized = transposed if transposed.mode == "RGB" else transposed.convert("RGB")
            normalized.save(temp_path, format="PNG")
            normalized_mode = normalized.mode

        yield PreparedImageInput(
            path=temp_path,
            metadata_filename=metadata_filename or original_path.name,
            warning=_build_preflight_warning(
                reason="palette_mode_jpeg_save_risk",
                original_mode=probe.image_mode,
                normalized_mode=normalized_mode,
                image_format=probe.image_format,
            ),
            applied=True,
            normalized_image_used=True,
            image_preflight_reason="palette_mode_jpeg_save_risk",
            original_image_mode=probe.image_mode,
            normalized_image_mode=normalized_mode,
            image_format=probe.image_format,
        )
