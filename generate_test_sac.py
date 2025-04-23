#!/usr/bin/env python3
"""
generate_test_sac.py
====================

Fabricate a miniature SAC data set for testing **seis_station_updater.py**.

Changes in this revision
------------------------
* Removed the `overwrite=` argument from `SACTrace.write()` for compatibility
  with older ObsPy versions (<1.4), and instead `unlink()` any existing file
  before writing.
"""
from __future__ import annotations

import random
from pathlib import Path
from typing import List, Tuple

import numpy as np
from obspy import Trace, UTCDateTime
from obspy.io.sac import SACTrace

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
ROOT: Path = Path("./example/data")
YEAR: int = 2024
NETWORK: str = "XX"
COMP: str = "BHZ"
DAY_RANGE: Tuple[int, int] = (1, 3)  # JJJ = 001–003
STATIONS: List[Tuple[str, float, float, float]] = [
    ("TST1", 34.50, -117.10, 800),
    ("TST2", 35.20, -118.00, 500),
    ("TST3", 36.00, -119.30, 250),
]
STATION_FILE: Path = Path("./example/test_stations.txt")
SEED: int = 42

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_sac(path: Path, starttime: UTCDateTime) -> None:
    """Write a 10‑second dummy SAC file with empty coordinate headers."""
    npts = 1000
    dt = 0.01
    data = np.random.randn(npts).astype(np.float32)
    trace = Trace(data=data)
    trace.stats.delta = dt
    trace.stats.starttime = starttime

    sac = SACTrace.from_obspy_trace(trace)
    sac.stla = None
    sac.stlo = None
    sac.stel = None

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()  # ensure fresh write for old ObsPy versions
    sac.write(str(path), byteorder="little")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    # 1. Generate SAC files
    print("Generating synthetic SAC files …")
    start_date = UTCDateTime(f"{YEAR}-01-01")
    count = 0
    for sta, _lat, _lon, _elev in STATIONS:
        for jjj in range(DAY_RANGE[0], DAY_RANGE[1] + 1):
            jday_date = start_date + (jjj - 1) * 86400
            jjj_str = f"{jjj:03d}"
            dir_path = ROOT / f"{YEAR}" / f"{NETWORK}.{sta}"
            fname = f"{NETWORK}.{sta}.{COMP}.{jjj_str}.SAC"
            full_path = dir_path / fname
            _make_sac(full_path, starttime=jday_date)
            count += 1
    print(f"Created {count} SAC files under {ROOT}/")

    # 2. Write station list CSV
    print("Writing station metadata list …")
    STATION_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATION_FILE.write_text(
        "\n".join(f"{s},{lat},{lon},{elev}" for s, lat, lon, elev in STATIONS) + "\n"
    )
    print(f"Station list → {STATION_FILE}")

    print("Done. You can now run seis_station_updater.py to populate headers.")


if __name__ == "__main__":
    main()
