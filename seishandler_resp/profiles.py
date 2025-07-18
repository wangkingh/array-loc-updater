# seishandler_resp/profiles.py
"""Profile & helper utilities for RESP file support."""
from __future__ import annotations

from typing import Set

from SeisHandler.pattern_utils import FieldRegistry

# ── 响应文件必须包含的字段 ───────────────────────────────────────────────────
REQUIRED_RESPONSE_FIELDS: Set[str] = {"station", "component", "resp_type"}


def register_resp_fields() -> None:
    """冗余注册函数（重复 import 也安全）"""
    FieldRegistry.register("resp_type", r"(RESP|StationXML|PAZ|FAP)")
    FieldRegistry.register("version", r"v\d{2}")


def check_resp_pattern(pattern: str) -> None:
    """验证 pattern 是否含必需占位符；缺失则抛 ValueError"""
    fields = set(FieldRegistry.get_fields(pattern))
    missing = REQUIRED_RESPONSE_FIELDS - fields
    if missing:
        raise ValueError(f"RESP pattern 缺少字段: {sorted(missing)} — {pattern}")
