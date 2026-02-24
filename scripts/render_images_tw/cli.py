# scripts/render_images_tw/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

os.environ.setdefault("MPLBACKEND", "Agg")

# 預設 debug 打開（你原本需求）
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_tw.pipeline import render_tw  # ✅ 絕對 import
from scripts.render_images_tw.tw_rows import print_open_limit_watchlist, print_sector_top_rows

# ✅ NEW: shared sector order + ordered list writer
from scripts.render_images_common.sector_order import (  # noqa: E402
    extract_overview_sector_order,
    write_list_txt_from_overview_order,
)


# =============================================================================
# IO helpers
# =============================================================================
def load_payload(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _payload_ymd(payload: Dict[str, Any]) -> str:
    return _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")


def _payload_slot(payload: Dict[str, Any]) -> str:
    return _safe_str(payload.get("slot") or "") or "unknown"


# =============================================================================
# list.txt generator (fallback; keep your original behavior)
# =============================================================================
def write_list_txt(
    outdir: Path,
    *,
    ext: str = "png",
    overview_prefix: str = "overview_sectors_",
    filename: str = "list.txt",
) -> Path:
    """
    Generate outdir/list.txt for render_video.py (concat order source).

    Order:
      1) overview pages: {overview_prefix}*_p*.{ext} (sorted)
         fallback: {overview_prefix}*.{ext} (sorted)
      2) other images in outdir: *.{ext} excluding those starting with overview_prefix (sorted)

    Writes RELATIVE paths (relative to outdir), one per line.
    """
    outdir = outdir.resolve()
    ext = (ext or "png").lstrip(".")
    overview_prefix = str(overview_prefix or "").strip() or "overview_sectors_"

    items: List[Path] = []

    paged = sorted(outdir.glob(f"{overview_prefix}*_p*.{ext}"), key=lambda p: p.name)
    if paged:
        items.extend(paged)
    else:
        any_overview = sorted(outdir.glob(f"{overview_prefix}*.{ext}"), key=lambda p: p.name)
        items.extend(any_overview)

    others = sorted(outdir.glob(f"*.{ext}"), key=lambda p: p.name)
    others = [p for p in others if not p.name.startswith(overview_prefix)]
    items.extend(others)

    seen = set()
    rel_lines: List[str] = []
    for p in items:
        pp = p.resolve()
        if pp in seen:
            continue
        seen.add(pp)
        rel_lines.append(pp.relative_to(outdir).as_posix())

    list_path = outdir / filename
    list_path.write_text("\n".join(rel_lines) + ("\n" if rel_lines else ""), encoding="utf-8")
    return list_path


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--payload", required=True)
    ap.add_argument("--outdir", default=None)

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="tw")
    ap.add_argument("--rows-per-box", type=int, default=7)
    ap.add_argument("--max-sectors", type=int, default=20)
    ap.add_argument("--cap-pages", type=int, default=5)

    ap.add_argument("--no-overview", action="store_true")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)

    ap.add_argument("--no-debug", action="store_true")

    ap.add_argument("--debug-rows", type=int, default=0)
    ap.add_argument("--debug-sector", default=None)
    ap.add_argument("--debug-only", action="store_true")

    args = ap.parse_args()

    if args.no_debug:
        os.environ["OVERVIEW_DEBUG_FOOTER"] = "0"
        os.environ["OVERVIEW_DEBUG_FONTS"] = "0"
        os.environ["OVERVIEW_DEBUG"] = "0"

    payload = load_payload(args.payload)

    if int(args.debug_rows or 0) > 0:
        print_open_limit_watchlist(payload, n=int(args.debug_rows))

    if args.debug_sector:
        print_sector_top_rows(payload, sector=str(args.debug_sector), n=50)

    if args.debug_only:
        print("\n[DEBUG] --debug-only enabled: skip rendering.")
        return 0

    ymd = _payload_ymd(payload) or "unknown"
    slot = _payload_slot(payload)

    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = REPO_ROOT / "media" / "images" / "tw" / ymd / slot
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[TW] payload={args.payload}")
    print(f"[TW] ymd={ymd} slot={slot} outdir={outdir}")
    print(
        "[TW] debug="
        f"footer={os.getenv('OVERVIEW_DEBUG_FOOTER','0')} "
        f"fonts={os.getenv('OVERVIEW_DEBUG_FONTS','0')}"
    )

    # ✅ 直接固定關掉 gainbins（不浪費時間）
    render_tw(
        payload=payload,
        outdir=outdir,
        theme=str(args.theme),
        layout_name=str(args.layout),
        rows_per_box=int(args.rows_per_box),
        max_sectors=int(args.max_sectors),
        cap_pages=int(args.cap_pages),
        no_overview=bool(args.no_overview),
        overview_metric=str(args.overview_metric or "auto"),
        overview_page_size=int(args.overview_page_size),
        overview_gainbins=False,  # ✅ HARD OFF
    )

    # -------------------------------------------------------------------------
    # Write list.txt for video concat
    # - If overview exported sector order -> order sector pages accordingly
    # - Else fallback to old sorting
    # -------------------------------------------------------------------------
    try:
        keys = extract_overview_sector_order(payload)
        print("[TW][DEBUG] raw _overview_sector_order exists?:", isinstance(payload.get("_overview_sector_order"), list))
        print("[TW][DEBUG] raw overview order head:", (payload.get("_overview_sector_order", []) or [])[:20])
        if keys:
            met_eff = str(payload.get("_overview_metric_eff") or "").strip()
            print(f"[TW] overview sector order loaded: n={len(keys)}" + (f" metric={met_eff}" if met_eff else ""))
            print("[TW] normalized overview order head:", keys[:20])

            # ✅ NEW: list.txt ordered by overview sector order
            list_path = write_list_txt_from_overview_order(
                outdir=outdir,
                overview_prefix="overview_sectors_",
                sector_page_glob="tw_*_p*.png",  # TW sector pages filename pattern (adjust if yours differs)
                overview_sector_keys=keys,
                list_filename="list.txt",
            )
            print(f"[TW] wrote {list_path} (ordered by overview sector order)")
        else:
            list_path = write_list_txt(
                outdir,
                ext="png",
                overview_prefix="overview_sectors_",
                filename="list.txt",
            )
            print(f"[TW] wrote {list_path} (fallback)")
    except Exception as e:
        print(f"[TW] list.txt generation failed (continue): {e}")

    print("\n✅ TW render finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
