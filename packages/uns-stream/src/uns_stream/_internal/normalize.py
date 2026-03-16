from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


# 1. 定义一个仅供内部使用的“数据清洗模型”
class _UnstructuredCoords(BaseModel):
    model_config = ConfigDict(extra="ignore")  # 忽略掉不需要的字段

    points: list[list[float]]
    system: str | None = None
    layout_width: float | None = None
    layout_height: float | None = None

    @property
    def bbox(self) -> dict[str, float]:
        """逻辑内聚：在模型内部计算 bbox"""
        xs = [p[0] for p in self.points]
        ys = [p[1] for p in self.points]
        return {
            "x_min": min(xs),
            "y_min": min(ys),
            "x_max": max(xs),
            "y_max": max(ys),
        }


def normalize_unstructured_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    """使用 Pydantic 强校验并清洗元数据"""
    m = dict(meta)

    # 1) 路径脱敏
    m.pop("file_directory", None)

    # 2) 自动类型转换 (Pydantic 会自动把 "true" 转为 True)
    # 如果你以后元数据字段多了，可以定义一个完整的 MetadataModel
    if isinstance(m.get("is_extracted"), str):
        val = m["is_extracted"].lower()
        if val in ("true", "false"):
            m["is_extracted"] = val == "true"

    # 3) 健壮的坐标转换
    coords_raw = m.get("coordinates")
    if isinstance(coords_raw, dict):
        try:
            # 这一步会瞬间解决所有 Pyright 的类型推断问题
            coords_obj = _UnstructuredCoords.model_validate(coords_raw)
            m["bbox"] = coords_obj.bbox
        except Exception:
            # 如果 Unstructured 吐出的坐标格式不对，不影响主流程
            pass

    return m
