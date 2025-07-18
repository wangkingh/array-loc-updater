"""
FileMatcher 专用于仪器响应文件（RESP / StationXML 等）  
特点：完全复用父类逻辑，但 **不派生 time 字段**。
"""
from __future__ import annotations
from SeisHandler.file_matcher import FileMatcher


class RespMatcher(FileMatcher):
    """Same as FileMatcher，但 `_gen_time_from_fields()` 恒返回 None。"""

    # 关键：关闭时间推导
    def _gen_time_from_fields(self, fields):  # type: ignore[override]
        return None
