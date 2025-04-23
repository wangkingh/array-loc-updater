#!/usr/bin/env python3
"""
seis_station_updater.py (auto-delimiter, tqdm, console-only)
===========================================================

* 批量写入 SAC 头 (stla/stlo/stel) —— 带 tqdm 进度条。
* 自动检测台站列表分隔符：支持逗号、制表符或任意空白。
* 绘制 station_map.png，校验后直接在终端列出问题台站。

运行： `python seis_station_updater.py`
"""
from __future__ import annotations

import csv
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from obspy.io.sac import SACTrace
from tqdm.auto import tqdm
from SeisHandler import SeisArray

###############################################################################
# >>> CONFIGURATION <<<
###############################################################################
# ARRAY_DIR: Path = Path("/data/X2")
# PATTERN: str = "{home}/{YYYY}/{network}.{station}/*.{component}.{JJJ}.SAC"

# STATION_FILE: Path = Path("stations.txt")   # 支持逗号、Tab、空格自动识别
# STATION_COL: int = 0
# LAT_COL: int = 1
# LON_COL: int = 2
# ELEV_COL: Optional[int] = 3                 # None → 不写海拔

# THREADS: int = 12
# MAP_FILE: Path = Path("station_map.png")
# LOG_LEVEL: str = "INFO"

###############################################################################
# >>> CONFIGURATION (for example data set) <<<
###############################################################################
ARRAY_DIR: Path = Path("./example/data")        # 与 generate_test_sac.py 的 ROOT 一致
PATTERN: str = "{home}/{YYYY}/{*}.{*}/{network}.{station}.{component}.{JJJ}.SAC"

STATION_FILE: Path = Path("./example/test_stations.txt")  # 与 STATION_FILE 一致
STATION_COL: int = 0
LAT_COL: int = 1
LON_COL: int = 2
ELEV_COL: Optional[int] = 3         # 不写海拔就改为 None

THREADS: int = 4                    # 小数据集用 4 线程足够
MAP_FILE: Path = Path("./example/station_map.png")
LOG_LEVEL: str = "DEBUG"            # 便于观察详细日志


###############################################################################
# Helper functions
###############################################################################
def _detect_delimiter(sample_line: str) -> Optional[str]:
    """Return ',', '\\t' or None (whitespace) based on first non-comment line."""
    if "," in sample_line:
        return ","
    if "\t" in sample_line:
        return "\t"
    # 默认：任意空白
    return None


def read_station_table() -> Dict[str, Tuple[float, float, Optional[float]]]:
    """Parse station list → {station: (lat, lon, elev)}  (auto delimiter)."""
    mapping: Dict[str, Tuple[float, float, Optional[float]]] = {}
    with STATION_FILE.open() as fh:
        # 先读取第一行有效内容判断分隔符
        for line in fh:
            if line.strip() and not line.lstrip().startswith("#"):
                delimiter = _detect_delimiter(line)
                fh.seek(0)                 # 回到文件开头
                break
        else:
            logging.error("Station file is empty or only comments")
            return mapping

        if delimiter:                      # CSV 模式
            reader = csv.reader(fh, delimiter=delimiter)
            for parts in reader:
                if not parts or parts[0].startswith("#"):
                    continue
                _process_row(parts, mapping)
        else:                              # 任意空白
            for raw in fh:
                parts = raw.strip().split()
                if not parts or parts[0].startswith("#"):
                    continue
                _process_row(parts, mapping)
    return mapping


def _process_row(parts: List[str],
                 mapping: Dict[str, Tuple[float, float, Optional[float]]]) -> None:
    """Parse a single row and update mapping."""
    try:
        sta  = parts[STATION_COL]
        lat  = float(parts[LAT_COL])
        lon  = float(parts[LON_COL])
        elev = float(parts[ELEV_COL]) if ELEV_COL is not None else None
        mapping[sta] = (lat, lon, elev)
    except (IndexError, ValueError) as exc:
        logging.warning("Bad station line: %s (%s)", parts, exc)


def update_sac_headers(
    station_files: Dict[str, List[Path]],
    station_meta: Dict[str, Tuple[float, float, Optional[float]]],
) -> None:
    total = sum(len(v) for v in station_files.values())
    pbar  = tqdm(total=total, desc="Writing SAC headers", unit="file")
    write_elev = ELEV_COL is not None

    for sta, files in station_files.items():
        meta = station_meta.get(sta)
        if meta is None:
            logging.warning("Station %s not in list; skip", sta)
            pbar.update(len(files))
            continue

        lat, lon, elev = meta
        for p in files:
            try:
                sac = SACTrace.read(str(p))
                sac.stla, sac.stlo = lat, lon
                if write_elev and elev is not None:
                    sac.stel = elev
                # 直接覆盖写入
                sac.write(str(p), byteorder="little")
            except Exception as exc:       # noqa: BLE001
                logging.error("Failed to update %s: %s", p, exc)
            finally:
                pbar.update(1)
    pbar.close()


def make_station_map(station_meta: Dict[str, Tuple[float, float, Optional[float]]]) -> None:
    lons, lats = zip(*[(v[1], v[0]) for v in station_meta.values()])
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(lons, lats, marker="^", s=25)
    for sta, (lat, lon, _e) in station_meta.items():
        ax.text(lon, lat, sta, fontsize=7)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Station distribution")
    fig.tight_layout()
    fig.savefig(MAP_FILE, dpi=300)
    plt.close(fig)
    logging.info("Station map saved → %s", MAP_FILE)


def verify_headers(
    station_files: Dict[str, List[Path]],
    station_meta: Dict[str, Tuple[float, float, Optional[float]]],
) -> None:
    """Cross-check first SAC header per station and print mismatches."""
    mismatches: List[Tuple[str, str]] = []
    for sta, files in tqdm(station_files.items(), desc="Verifying headers", unit="station"):
        meta = station_meta.get(sta)
        if meta is None:
            mismatches.append((sta, "missing_in_table"))
            continue
        lat_ref, lon_ref, elev_ref = meta
        try:
            sac = SACTrace.read(str(files[0]))
            issues = []
            if sac.stla is None or abs(sac.stla - lat_ref) > 1e-4:
                issues.append(f"lat {sac.stla} ≠ {lat_ref}")
            if sac.stlo is None or abs(sac.stlo - lon_ref) > 1e-4:
                issues.append(f"lon {sac.stlo} ≠ {lon_ref}")
            if (ELEV_COL is not None and elev_ref is not None
                    and (sac.stel is None or abs(sac.stel - elev_ref) > 0.1)):
                issues.append(f"elev {sac.stel} ≠ {elev_ref}")
            if issues:
                mismatches.append((sta, "; ".join(issues)))
        except Exception as exc:           # noqa: BLE001
            mismatches.append((sta, f"read_error: {exc}"))

    if mismatches:
        logging.warning("Found %d mismatching stations:\n%s",
                        len(mismatches),
                        "\n".join(f"  {s}: {reason}" for s, reason in mismatches))
    else:
        logging.info("All headers verified OK.")


###############################################################################
# Main
###############################################################################
def main() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info("Scanning files with SeisArray…")
    seis = SeisArray(array_dir=str(ARRAY_DIR), pattern=PATTERN)
    seis.match(threads=THREADS)
    all_files = seis.files
    if not all_files:
        logging.error("No files matched. Check ARRAY_DIR/PATTERN.")
        sys.exit(1)
    logging.info("Matched %d files", len(all_files))

    # Build {station: [Path,…]}
    station_files: Dict[str, List[Path]] = {}
    for f in all_files:
        station_files.setdefault(f["station"], []).append(Path(f["path"]))

    station_meta = read_station_table()
    logging.info("Loaded metadata for %d stations", len(station_meta))

    update_sac_headers(station_files, station_meta)
    make_station_map(station_meta)
    verify_headers(station_files, station_meta)

    # Optional: daily counts sample
    dates = [Path(f["path"]).stem.split(".")[-2]
             for f in all_files if "JJJ" not in PATTERN]
    if dates:
        logging.debug("Sample daily counts: %s",
                      dict(Counter(dates).most_common(5)))

    logging.info("Done.")


if __name__ == "__main__":
    main()

