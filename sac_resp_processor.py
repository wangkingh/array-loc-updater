#!/usr/bin/env python3
"""
sac_resp_processor.py
=====================
* TEST_MODE=True  → 仅打印 SAC↔RESP 对应关系
* TEST_MODE=False → 并行 remove_response (+可选带通滤波)
* DAEMON_MODE=True → 自身后台运行，将 PID 写入 rmresp.pid
* 已存在输出文件可选择跳过（SKIP_EXISTING）
"""
from __future__ import annotations

import atexit
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
from obspy import read,read_inventory
from obspy.io.sac import SACTrace
from tqdm import tqdm
from SeisHandler import SeisArray
from seishandler_resp import RespArray

###############################################################################
# >>> CONFIGURATION <<<
###############################################################################
# 输入路径与模式
SAC_ROOT: Path = Path("/home/ludan/wjx_work/data_y3_1Hz")
SAC_PATTERN: str =     "{home}/{*}/{*}/{network}.{station}.{*}.{component}.D.{YYYY}.{JJJ}.sac" 

# 2) pattern 中把第二个 .* 改成 {location}
RESP_PATTERN = "{home}/{resptype}.{network}.{station}.{location}.{component}"
RESP_ROOT: Path = Path("/data/arrayDATA/dataY3/INFO_Y3_V20200212/RESP_Y3_V20210220_FINAL/Y3/")
CUSTOM_RESP = {
    "resptype":  r"(RESP|StationXML|PAZ|FAP)",
    "location":  r"\d{2}",          # ← 新增
}


# 输出
OUT_DIR: Path = Path("/data/userdata/ludan/wjx_data/y3_1Hz")

# 并行 / 日志
THREADS: int = 90
LOG_LEVEL: str = "INFO"
LOG_FILE: Path = Path(f"rmresp_{datetime.now():%Y%m%d_%H%M%S}.log")

# 控制开关
# TEST_MODE   = True    # 仅做配对检查
# DAEMON_MODE = False   # 前台运行
# SKIP_EXISTING = True
TEST_MODE   = False    # 正式运行
DAEMON_MODE = True   # 后台运行
SKIP_EXISTING = True

# 带通滤波 (None = 不滤波)
FREQ_MIN: float | None = 0.01
FREQ_MAX: float | None = 0.5

# PID 文件
PID_FILE: Path = Path("rmresp.pid")

# ---------------- CONFIGURATION ----------------
RESP_FILTER: Dict = {
    "resptype": {"type": "list", "value": ["RESP"]}
}
WATER_LEVEL: int = 60


###############################################################################
# 守护进程工具
###############################################################################
def _daemonize() -> None:
    """UNIX 双 fork + 日志重定向 → 后台运行."""
    if os.fork():
        sys.exit(0)  # 第 1 个父进程退出
    os.setsid()  # 创建新会话
    if os.fork():
        sys.exit(0)  # 第 2 个父进程退出

    # 把 stdout / stderr 重定向到日志文件
    sys.stdout.flush()
    sys.stderr.flush()
    with open(LOG_FILE, "a+", buffering=1) as log_fd:
        os.dup2(log_fd.fileno(), sys.stdout.fileno())
        os.dup2(log_fd.fileno(), sys.stderr.fileno())

    # 写 PID 文件S
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")

    def _cleanup_pid():
        try:
            PID_FILE.unlink()
        except FileNotFoundError:
            pass
    
    # 退出时删除 PID 文件
    atexit.register(_cleanup_pid)
    # atexit.register(lambda: PID_FILE.unlink(missing_ok=True))

    # 捕获 SIGTERM → 正常退出
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))


###############################################################################
# 工具函数
###############################################################################
KEY_FIELDS = ("network", "station", "component")


def _key(rec) -> Tuple[str, str, str]:
    return tuple(rec[k] for k in KEY_FIELDS)


def build_resp_lookup(resp_records: List[dict]) -> Dict[Tuple[str, str, str], str]:
    lut: Dict[Tuple[str, str, str], str] = {}
    for rec in resp_records:
        k = _key(rec)
        if k in lut:
            logging.warning("Duplicate resp for %s.%s.%s; keep first", *k)
            continue
        lut[k] = rec["path"]
    return lut


###############################################################################
# 预滤波窗口工具
###############################################################################
def _make_prefilt(fmin: float | None, fmax: float | None
                  ) -> tuple[float, float, float, float] | None:
    """
    根据目标带通边界构造 (f1, f2, f3, f4) 形式的 pre_filt。
    若任一端为空则返回 None，意味着不做预滤波。
    """
    if fmin is None or fmax is None:
        return None
    return (0.8 * fmin, fmin, fmax, 1.2 * fmax)


###############################################################################
# 核心处理单元（仅 RESP）
###############################################################################
def process_one(sac: Path, resp: str, rel: Path) -> bool:
    """
    读取 SAC → 轻量预处理 → 去响应 (RESP) → 写出 SAC。
    返回 True 表示成功。
    """
    out_path = OUT_DIR / rel
    if SKIP_EXISTING and out_path.exists():
        return True

    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # 1) 读取 SAC
        tr = read(str(sac))[0]

        # 1.1) 轻量预处理：去均值 & 5% taper（汉宁窗）
        tr.detrend("demean")
        tr.taper(max_percentage=0.05, type="hann")

        # 2) 计算预滤波窗口
        prefilt = _make_prefilt(FREQ_MIN, FREQ_MAX)

        # 3) 去仪器响应（RESP 格式）
        inv = read_inventory(resp, format="RESP")   # <60 ms, 会缓存
        tr.remove_response(
            inventory=inv,
            output="VEL",     # 如需 DIS/ACC 可改
            pre_filt=prefilt,
            water_level=WATER_LEVEL,   # 防止低频奇异
        )

        # 4) 写出
        SACTrace.from_obspy_trace(tr).write(str(out_path), byteorder="little")
        return True

    except Exception:
        logging.exception("rmresp failed %s with %s", sac, resp)
        return False


###############################################################################
# 主流程
###############################################################################
def main() -> None:
    # 日志配置（先指向控制台；若 daemonize 后会被重定向）
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s %(levelname)s %(message)s",
    )
    np.seterr(invalid="ignore")

    if DAEMON_MODE:
        _daemonize()
        logging.info("Daemon started. PID=%s  LOG=%s", os.getpid(), LOG_FILE)

    # 1) 扫描 SAC
    sac_arr = SeisArray(SAC_ROOT, SAC_PATTERN)
    sac_arr.match(threads=THREADS)
    sac_records = sac_arr.files
    if not sac_records:
        logging.error("No SAC files matched.")
        sys.exit(1)
    logging.info("SAC matched: %d", len(sac_records))

    # 2) 扫描 RESP
    resp_arr = RespArray(RESP_ROOT, RESP_PATTERN,custom_fields=CUSTOM_RESP)
    resp_arr.match(threads=THREADS)
    if RESP_FILTER:
        resp_arr.filter(RESP_FILTER)
    resp_records = resp_arr.files

    # 配对
    resp_lut = build_resp_lookup(resp_records)

    # 3) TEST 模式
    if TEST_MODE:
        missing = 0
        print("\n===== TEST MODE: SAC ↔ RESP =====")
        for rec in sac_records:
            key        = _key(rec)                 # network, station, component
            sac_path   = rec["path"]               # ← SAC 全路径
            resp_path  = resp_lut.get(key)         # 找对应 RESP

            # 打印：SAC → RESP  一条一条
            print(f"SAC:  {sac_path}\n"
                f"RESP: {resp_path or 'MISSING'}\n"
                f"KEY:  {'.'.join(key)}\n"
                "─" * 60)

            if resp_path is None:
                missing += 1

        print(f"\nTotal {len(sac_records)}, Missing RESP {missing}\n")
        return


    # 4) 正式并行处理
    missing = 0
    pbar = tqdm(total=len(sac_records), desc="rmRESP", unit="file")

    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        futs = []
        for rec in sac_records:
            k = _key(rec)
            r = resp_lut.get(k)
            if not r:
                missing += 1
                pbar.update(1)
                continue
            rel = Path(rec["path"]).relative_to(SAC_ROOT)
            futs.append(exe.submit(process_one, Path(rec["path"]), r, rel))

        for fut in as_completed(futs):
            fut.result()
            pbar.update(1)

    pbar.close()
    logging.info(
        "DONE. SAC=%d | RESP missing=%d | Output=%s",
        len(sac_records),
        missing,
        OUT_DIR,
    )


if __name__ == "__main__":
    main()
