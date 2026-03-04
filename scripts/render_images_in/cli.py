# scripts/render_images_in/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# =============================================================================
# Force headless backend
# =============================================================================
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# Force repo root into sys.path (STRONG VERSION)
# =============================================================================
THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[2]  # intraday-limitup-engine/

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# sanity check
if not (REPO_ROOT / "scripts" / "render_images_common").exists():
    raise RuntimeError(
        f"[IN CLI] Cannot locate project root properly. "
        f"Expected scripts/render_images_common under {REPO_ROOT}"
    )

# =============================================================================
# DEBUG DEFAULT ON
# =============================================================================
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

# =============================================================================
# IMPORTS (now guaranteed safe)
# =============================================================================
from scripts.render_images_in.sector_blocks.draw_mpl import draw_block_table
from scripts.render_images_in.sector_blocks.layout import get_layout
from scripts.render_images_common.header_mpl import get_market_time_info
from scripts.render_images_common.overview_mpl import render_overview_png
from scripts.render_images_common.sector_order import (
    normalize_sector_key,
    extract_overview_sector_order,
    reorder_keys_by_overview,
)
from markets.india.aggregator import aggregate as in_aggregate


# =============================================================================
# Utils
# =============================================================================
def _pct(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    return str(x).strip().lower() in ("1", "true", "yes", "y", "on")


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def load_payload(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("snapshot_all", "snapshot_main", "snapshot_open", "snapshot"):
        rows = payload.get(key) or []
        if isinstance(rows, list) and rows:
            return rows
    return []


def _payload_ymd(payload: Dict[str, Any]) -> str:
    return _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")


def _payload_slot(payload: Dict[str, Any]) -> str:
    return _safe_str(payload.get("slot") or "unknown")


# =============================================================================
# TIME NOTE
# =============================================================================
def build_in_time_note(payload: Dict[str, Any]) -> str:
    trade_ymd = _payload_ymd(payload)
    meta = payload.get("meta") or {}
    tmeta = meta.get("time") or {}

    hm = _safe_str(tmeta.get("market_finished_hm") or "")
    finished_at = _safe_str(tmeta.get("market_finished_at") or "")

    if not hm:
        try:
            _, _, _, hhmm = get_market_time_info(payload, market="IN")
            hm = _safe_str(hhmm)
        except Exception:
            hm = ""

    line1 = f"India Trading Day {trade_ymd}"
    if hm:
        return f"{line1}\nUpdated {trade_ymd} {hm}"
    return line1


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True)
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="us")
    ap.add_argument("--rows-per-box", type=int, default=6)
    ap.add_argument("--cap-pages", type=int, default=5)
    ap.add_argument("--no-overview", action="store_true")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)
    ap.add_argument("--lang", default="en")
    ap.add_argument("--no-debug", action="store_true")

    args = ap.parse_args()

    payload = load_payload(args.payload)
    universe0 = pick_universe(payload)

    if not universe0:
        raise RuntimeError("No usable snapshot in payload")

    ymd = _payload_ymd(payload)
    slot = _payload_slot(payload)

    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = REPO_ROOT / "media" / "images" / "in" / ymd / slot

    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[IN] payload={args.payload}")
    print(f"[IN] ymd={ymd} slot={slot}")
    print(f"[IN] repo_root={REPO_ROOT}")

    agg_payload = in_aggregate(payload)
    universe = pick_universe(agg_payload) or universe0

    time_note = build_in_time_note(agg_payload)

    # Overview
    if not args.no_overview:
        try:
            render_overview_png(
                agg_payload,
                outdir,
                width=1080,
                height=1920,
                page_size=int(args.overview_page_size),
                metric="mix",
            )
        except Exception as e:
            print(f"[IN] overview failed: {e}")

    print("\n✅ IN render finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
