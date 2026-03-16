from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError


# 1. 内部元数据校验模型：真正利用 Pydantic 的自动转型能力
class _MetadataCleaner(BaseModel):
    model_config = ConfigDict(extra="allow")  # 允许并保留未定义的其他字段

    # Pydantic 会自动把 "true"/"1"/True 转为布尔值 True
    is_extracted: bool | None = None

    # 明确排除掉的敏感字段
    file_directory: Any = Field(None, exclude=True)


class _UnstructuredCoords(BaseModel):
    model_config = ConfigDict(extra="ignore")
    points: list[list[float]]

    @property
    def bbox(self) -> dict[str, float]:
        xs = [p[0] for p in self.points]
        ys = [p[1] for p in self.points]
        return {
            "x_min": min(xs),
            "y_min": min(ys),
            "x_max": max(xs),
            "y_max": max(ys),
        }


def normalize_unstructured_metadata(meta: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """
    规范化元数据。
    返回: (规范化后的字典, 警告信息列表)
    """
    warnings: list[str] = []

    # 1) 利用 Pydantic 模型进行字段脱敏和类型转换
    try:
        cleaner = _MetadataCleaner.model_validate(meta)
        # 导出字典，exclude_none=True 保持干净，同时 Pydantic 会自动移除 Field(exclude=True) 的字段
        m = cleaner.model_dump(exclude_unset=True)
    except ValidationError as e:
        # TODO: P1-07-03 引入 logger.warning
        warnings.append(f"Metadata validation partially failed: {e}")
        m = dict(meta)
        m.pop("file_directory", None)  # 兜底脱敏

    # 2) 坐标转换
    coords_raw = m.get("coordinates")
    if isinstance(coords_raw, dict):
        try:
            coords_obj = _UnstructuredCoords.model_validate(coords_raw)
            m["bbox"] = coords_obj.bbox
        except ValidationError as e:
            # 关键改进：不再悄悄 pass，记录下原因
            # TODO: P1-07-03 记录到 logger.debug
            warnings.append(f"Failed to derive bbox from coordinates: {e}")
        except Exception as e:
            warnings.append(f"Unexpected error during bbox derivation: {e}")

    return m, warnings
