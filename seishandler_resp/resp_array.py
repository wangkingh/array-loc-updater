from __future__ import annotations
from SeisHandler.seis_array import SeisArray
from .resp_matcher import RespMatcher
import SeisHandler.pattern_utils as PU, SeisHandler.seis_array as SA


class RespArray(SeisArray):
    """
    专用于 RESP / StationXML 的 SeisArray 派生类
    —— 跳过日期校验；match 时用 RespMatcher（不生成 time 字段）
    """

    def __init__(self, resp_dir: str, pattern: str,
                 *, custom_fields: dict | None = None, **kwargs):
        # ------------------------------------------------------------------ #
        # 0) 基础自定义字段
        base = {"resptype": r"(RESP|StationXML|PAZ|FAP)",
                "version":  r"v\d{2}"}
        if custom_fields:
            base.update(custom_fields)
        # ------------------------------------------------------------------ #
        # 1) 临时关闭日期字段检查
        def _no_date(_dir, pat, reg):          # noqa: ANN001
            return reg.build_regex_pattern(pat)

        _orig_pu, _orig_sa = PU.check_pattern, SA.check_pattern
        PU.check_pattern = SA.check_pattern = _no_date
        try:
            super().__init__(resp_dir, pattern, custom_fields=base, **kwargs)
        finally:
            PU.check_pattern, SA.check_pattern = _orig_pu, _orig_sa

    # ---------------------------------------------------------------------- #
    # 2) 关键：把 match() 换成不用 FileMatcher、而是 RespMatcher
    # ---------------------------------------------------------------------- #
    def match(self, threads: int = 1):
        """
        复写父类方法：用 RespMatcher（不会派生 time 字段）
        """
        matcher = RespMatcher(directory=self.array_dir,
                              regex_pattern=self.pattern)
        self.files = matcher.match_files(num_threads=threads)
