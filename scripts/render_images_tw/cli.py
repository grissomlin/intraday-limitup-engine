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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_tw.pipeline import render_tw  # ✅ 絕對 import
from scripts.render_images_tw.tw_rows import print_open_limit_watchlist, print_sector_top_rows


DEFAULT_ROOT_FOLDER = (
    os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip()
    or "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"
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
# list.txt generator (unified with JP/KR)
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
# Drive subfolder helpers (align with JP/KR)
# =============================================================================
def _first_ymd(payload: Dict[str, Any]) -> Optional[str]:
    for k in ("bar_date", "ymd", "ymd_effective", "date"):
        v = _safe_str(payload.get(k))
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            return v

    for k in ("asof", "slot"):
        v = _safe_str(payload.get(k))
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)
    return None


def _infer_run_tag(payload: Dict[str, Any]) -> str:
    s = _safe_str(payload.get("slot") or payload.get("asof") or "").lower()
    if "open" in s:
        return "open"
    if "midday" in s or "noon" in s:
        return "midday"
    if "close" in s:
        return "close"

    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return f"{hh:02d}{mm:02d}"
    return "run"


def make_drive_subfolder_name(payload: Dict[str, Any], market: str) -> str:
    ymd = _first_ymd(payload)
    tag = _infer_run_tag(payload)
    if ymd:
        return f"{market}_{ymd}_{tag}"
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{market}_{now}"


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

    # Upload flags (same semantics as JP/KR)
    ap.add_argument("--no-upload-drive", action="store_true")
    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default="TW")
    ap.add_argument("--drive-client-secret", default=None)
    ap.add_argument("--drive-token", default=None)

    # ✅ subfolder: explicit or auto
    ap.add_argument("--drive-subfolder", default=None)
    ap.add_argument("--drive-subfolder-auto", action="store_true", default=True)

    ap.add_argument("--drive-workers", type=int, default=16)
    ap.add_argument("--drive-no-concurrent", action="store_true")
    ap.add_argument("--drive-no-overwrite", action="store_true")
    ap.add_argument("--drive-quiet", action="store_true")

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
        print("\n[DEBUG] --debug-only enabled: skip rendering & uploading.")
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

    # Drive subfolder auto naming (align JP/KR)
    drive_subfolder: Optional[str] = None
    if args.drive_subfolder:
        drive_subfolder = str(args.drive_subfolder).strip() or None
    else:
        if bool(args.drive_subfolder_auto):
            drive_subfolder = make_drive_subfolder_name(payload, market=str(args.drive_market or "TW").upper())

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
        no_upload_drive=bool(args.no_upload_drive),
        drive_root_folder_id=str(args.drive_root_folder_id),
        drive_market=str(args.drive_market),
        drive_client_secret=args.drive_client_secret,
        drive_token=args.drive_token,
        drive_subfolder=drive_subfolder,
        drive_workers=int(args.drive_workers),
        drive_no_concurrent=bool(args.drive_no_concurrent),
        drive_no_overwrite=bool(args.drive_no_overwrite),
        drive_quiet=bool(args.drive_quiet),
    )

    # -------------------------------------------------------------------------
    # Write list.txt for video concat (unified with JP/KR)
    # -------------------------------------------------------------------------
    try:
        list_path = write_list_txt(
            outdir,
            ext="png",
            overview_prefix="overview_sectors_",
            filename="list.txt",
        )
        print(f"[TW] wrote {list_path}")
    except Exception as e:
        print(f"[TW] list.txt generation failed (continue): {e}")

    print("\n✅ TW render finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())