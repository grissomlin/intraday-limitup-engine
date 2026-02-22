# scripts/render_images_in/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

# ✅ VERY IMPORTANT: Force matplotlib headless backend (avoid tkinter warnings)
os.environ.setdefault("MPLBACKEND", "Agg")

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# IN sector pages
from scripts.render_images_in.sector_blocks.draw_mpl import draw_block_table  # noqa: E402
from scripts.render_images_in.sector_blocks.layout import get_layout  # noqa: E402

# ✅ common header/time helper
from scripts.render_images_common.header_mpl import get_market_time_info  # noqa: E402

# overview (common)
from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402

# ✅ Drive uploader (env-first / b64 supported by drive_uploader)
from scripts.utils.drive_uploader import (  # noqa: E402
    get_drive_service,
    ensure_folder,
    upload_dir,
)

DEFAULT_ROOT_FOLDER = (
    os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip()
    or "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"
)

MARKET = "IN"


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


def _s(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def load_payload(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Prefer full-market universe first, so peers can be computed correctly.
    Support raw payload and agg payload that still carries snapshots.
    """
    for k in ("snapshot_main", "snapshot_all", "snapshot_open", "snapshot"):
        rows = payload.get(k) or []
        if isinstance(rows, list) and rows:
            return rows
    rows = payload.get("universe") or []
    if isinstance(rows, list) and rows:
        return rows
    return []


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def clean_sector_name(s: Any) -> str:
    ss = _s(s) or "Unclassified"
    ss = re.sub(r"\s+", " ", ss).strip()
    if ss in ("-", "--", "—", "–", "－", ""):
        ss = "Unclassified"
    return ss


def _payload_cutoff_str(payload: Dict[str, Any]) -> str:
    """
    draw_mpl signature keeps cutoff for compat; this returns a simple string.
    """
    return _s(payload.get("cutoff") or payload.get("asof") or payload.get("slot") or "close") or "close"


# =============================================================================
# Drive subfolder helpers (same style as TH)
# =============================================================================
def _first_ymd(payload: Dict[str, Any]) -> Optional[str]:
    for k in ("bar_date", "ymd", "ymd_effective", "date"):
        v = str(payload.get(k) or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            return v

    for k in ("asof", "slot"):
        v = str(payload.get(k) or "").strip()
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)
    return None


def _infer_run_tag(payload: Dict[str, Any]) -> str:
    s = str(payload.get("slot") or payload.get("asof") or "").lower()
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
# Builders (IN: event = ret >= threshold)
# =============================================================================
def is_event_stock_in(r: Dict[str, Any], ret_th: float) -> bool:
    # India: use pure return threshold as "event"
    return _pct(r.get("ret")) >= float(ret_th)


def build_events_by_sector_in(
    universe: List[Dict[str, Any]],
    ret_th: float,
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        if not is_event_stock_in(r, ret_th):
            continue

        sector = clean_sector_name(r.get("sector"))
        sym = _s(r.get("symbol"))
        if not sym:
            continue

        name = _s(r.get("name") or sym)
        ret = _pct(r.get("ret"))

        # line1/line2 for draw_mpl
        line1 = f"{sym}  {name}"
        # keep line2 short; your draw_mpl will show Limit XX% near ret text (right side)
        line2 = ""  # optional: you can put "Prev strong / news" here later

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,

                # ✅ classify as "big" so draw_mpl uses the big bucket + draws ret on line2
                "limitup_status": "big",

                "line1": line1,
                "line2": line2,

                # fields used by your _limit_text_en helper:
                "limit_pct_effective": r.get("limit_pct_effective"),
                "limit_pct": r.get("limit_pct"),
                "limit_rate": r.get("limit_rate"),
                "band": r.get("band"),
            }
        )

    for k in out:
        out[k].sort(key=lambda x: -(x.get("ret") or 0.0))

    return out


def build_peers_by_sector_in(
    universe: List[Dict[str, Any]],
    events_by_sector: Dict[str, List[Dict[str, Any]]],
    *,
    ret_min: float,
    max_per_sector: int,
    ret_th_event: float,
) -> Dict[str, List[Dict[str, Any]]]:
    if not events_by_sector:
        return {}

    event_syms = {
        _s(rr.get("symbol"))
        for rows in events_by_sector.values()
        for rr in (rows or [])
        if _s(rr.get("symbol"))
    }
    sectors = set(events_by_sector.keys())
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        sym = _s(r.get("symbol"))
        if not sym or sym in event_syms:
            continue

        sector = clean_sector_name(r.get("sector"))
        if sector not in sectors:
            continue

        # exclude event stocks from peers
        if is_event_stock_in(r, float(ret_th_event)):
            continue

        ret = _pct(r.get("ret"))
        if ret < float(ret_min):
            continue

        name = _s(r.get("name") or sym)
        line1 = f"{sym}  {name}"
        line2 = ""

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "line1": line1,
                "line2": line2,

                # for limit label on the right side (optional but nice for peers too)
                "limit_pct_effective": r.get("limit_pct_effective"),
                "limit_pct": r.get("limit_pct"),
                "limit_rate": r.get("limit_rate"),
                "band": r.get("band"),
            }
        )

    for k in out:
        out[k].sort(key=lambda x: -(x.get("ret") or 0.0))
        out[k] = out[k][: int(max_per_sector)]

    return out


def _apply_in_overview_copy(payload: Dict[str, Any]) -> None:
    payload["overview_title"] = payload.get("overview_title") or "Sector event counts (Top)"
    payload["overview_footer"] = payload.get("overview_footer") or "IN snapshot"
    payload["overview_note"] = payload.get("overview_note") or (
        "India: per-symbol price bands differ (2/5/10/20/No Limit)."
    )


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True)
    ap.add_argument("--outdir", required=True)

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="in")
    ap.add_argument("--rows-per-box", type=int, default=6)

    # event thresholds
    ap.add_argument("--ret-th", type=float, default=0.10, help="event threshold (default 10%)")
    ap.add_argument("--peer-ret-min", type=float, default=0.05)
    ap.add_argument("--peer-max-per-sector", type=int, default=10)

    ap.add_argument("--no-overview", action="store_true")

    # ✅ Upload is DEFAULT ON
    ap.add_argument("--no-upload-drive", action="store_true", help="do not upload to Drive after rendering")

    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default=MARKET)  # ✅ IN by default
    ap.add_argument("--drive-client-secret", default=None)
    ap.add_argument("--drive-token", default=None)

    # ✅ subfolder
    ap.add_argument("--drive-subfolder", default=None)
    ap.add_argument("--drive-subfolder-auto", action="store_true", default=True)

    # ✅ upload tuning
    ap.add_argument("--drive-workers", type=int, default=16)
    ap.add_argument("--drive-no-concurrent", action="store_true")
    ap.add_argument("--drive-no-overwrite", action="store_true")
    ap.add_argument("--drive-quiet", action="store_true")

    # ✅ safety: prevent cross-market mis-upload by default
    ap.add_argument(
        "--allow-market-mismatch",
        action="store_true",
        help="ALLOW uploading into a different drive-market (dangerous; disables safety guard)",
    )

    # ✅ language for draw_mpl labels
    ap.add_argument("--lang", default="en")

    args = ap.parse_args()

    payload = load_payload(args.payload)
    universe = pick_universe(payload)
    if not universe:
        raise RuntimeError("No usable snapshot in payload (need snapshot_main/snapshot_all/...)")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    layout = get_layout(args.layout)
    cutoff = _payload_cutoff_str(payload)

    # ✅ FIX: get_market_time_info may return 2/3/... values; take last as time_note
    _ti = get_market_time_info(payload)
    if isinstance(_ti, (list, tuple)):
        time_note = str(_ti[-1]) if _ti else ""
    else:
        time_note = str(_ti or "")

    events = build_events_by_sector_in(universe, float(args.ret_th))
    peers = build_peers_by_sector_in(
        universe,
        events,
        ret_min=float(args.peer_ret_min),
        max_per_sector=int(args.peer_max_per_sector),
        ret_th_event=float(args.ret_th),
    )

    payload.setdefault("market", MARKET)
    _apply_in_overview_copy(payload)

    if not args.no_overview:
        render_overview_png(payload, outdir)

    width, height = 1080, 1920
    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1
    CAP_PAGES = 5

    for sector, E_total in (events or {}).items():
        P_total = peers.get(sector, [])

        E_show = E_total[: CAP_PAGES * rows_top]
        P_show = P_total

        E_pages = chunk(E_show, rows_top) if E_show else [[]]
        P_pages = chunk(P_show, rows_peer)

        total_pages = len(E_pages)
        if len(P_pages) > total_pages:
            total_pages += 1
        total_pages = min(CAP_PAGES, max(1, total_pages))

        for i in range(total_pages):
            top_rows = E_pages[i] if i < len(E_pages) else []
            peer_rows = P_pages[i] if i < len(P_pages) else []
            has_more_peers = (len(P_pages) > total_pages) and (i == total_pages - 1)

            safe_sector = re.sub(r"\s+", "_", sector.strip())
            safe_sector = re.sub(r"[^\w\-]+", "_", safe_sector)
            out_path = outdir / f"in_{safe_sector}_p{i+1}.png"

            draw_block_table(
                out_path=out_path,
                layout=layout,
                sector=sector,
                cutoff=cutoff,

                # kept for compat (IN doesn't use these)
                locked_cnt=0,
                touch_cnt=0,
                theme_cnt=0,

                limitup_rows=top_rows,
                peer_rows=peer_rows,

                page_idx=i + 1,
                page_total=total_pages,
                width=width,
                height=height,
                rows_per_page=rows_top,
                theme=args.theme,
                time_note=time_note,
                has_more_peers=has_more_peers,

                # ensure draw_mpl uses your desired language
                lang=str(args.lang),
                market=MARKET,
            )

    print("✅ IN render finished.")

    # -------------------------------------------------------------------------
    # Drive upload (DEFAULT ON) + SAFETY GUARD
    # -------------------------------------------------------------------------
    if not args.no_upload_drive:
        market_name = str(args.drive_market or MARKET).strip().upper()

        # ✅ HARD GUARD: prevent accidental cross-market overwrite
        if (market_name != MARKET) and (not args.allow_market_mismatch):
            print(f"\n🛑 SAFETY GUARD: drive-market={market_name} but this script is {MARKET}.")
            print("   Upload skipped to prevent cross-market overwrite.")
            print("   If you REALLY intend to upload there, re-run with --allow-market-mismatch")
            return 0

        print("\n🚀 Uploading PNGs to Google Drive...")

        svc = get_drive_service(
            client_secret_file=args.drive_client_secret,
            token_file=args.drive_token,
        )

        root_id = str(args.drive_root_folder_id).strip()
        market_folder_id = ensure_folder(svc, root_id, market_name)

        if args.drive_subfolder:
            subfolder = str(args.drive_subfolder).strip()
        else:
            subfolder = make_drive_subfolder_name(payload, market=market_name)

        print(f"📁 Target Drive folder: root/{market_name}/{subfolder}/")

        uploaded = upload_dir(
            svc,
            market_folder_id,
            outdir,
            pattern="*.png",
            recursive=False,
            overwrite=(not args.drive_no_overwrite),
            verbose=(not args.drive_quiet),
            concurrent=(not args.drive_no_concurrent),
            workers=int(args.drive_workers),
            subfolder_name=subfolder,
        )

        print(f"✅ Uploaded {uploaded} png(s)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
