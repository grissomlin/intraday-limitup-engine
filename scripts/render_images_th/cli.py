# scripts/render_images_th/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

# ✅ VERY IMPORTANT: Force matplotlib headless backend (avoid tkinter warnings)
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# ✅ DEFAULT DEBUG ON (overview + footer) — align JP/KR/TW
# - OVERVIEW_DEBUG_FOOTER: print footer layout/text positions/lines
# - OVERVIEW_DEBUG_FONTS : print i18n_font debug (incl. rcParams['font.sans-serif'] order)
# - OVERVIEW_DEBUG        : repo-level master switch (if exists)
# =============================================================================
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_th.sector_blocks.draw_mpl import (  # noqa: E402
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_th.sector_blocks.layout import get_layout  # noqa: E402

# ✅ Common overview
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

MARKET = "TH"


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


def _bool_any(r: Dict[str, Any], *keys: str) -> bool:
    for k in keys:
        if _bool(r.get(k)):
            return True
    return False


def load_payload(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Prefer full-market universe first, so peers can be computed correctly.
    Support both raw payload and agg payload that still carries snapshots.
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


# =============================================================================
# Payload ymd/slot helpers (JP/KR style)
# =============================================================================
def _payload_ymd(payload: Dict[str, Any]) -> Optional[str]:
    """
    Try to get YYYY-MM-DD from payload.
    Priority:
      - payload["ymd"] / ["ymd_effective"] / ["bar_date"] / ["date"]
      - parse date from payload["asof"] / ["slot"]
    """
    for k in ("ymd", "ymd_effective", "bar_date", "date"):
        v = str(payload.get(k) or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            return v

    for k in ("asof", "slot"):
        v = str(payload.get(k) or "").strip()
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)
    return None


def _payload_slot(payload: Dict[str, Any]) -> str:
    """
    Normalize slot to: open / midday / close / HHMM / run
    Priority: payload["slot"], then payload["asof"].
    """
    s = str(payload.get("slot") or payload.get("asof") or "").strip().lower()

    if "open" in s:
        return "open"
    if "midday" in s or "noon" in s:
        return "midday"
    if "close" in s:
        return "close"

    # also accept "HH:MM" inside asof/slot
    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return f"{hh:02d}{mm:02d}"

    return "run"


def _default_outdir_from_payload(payload: Dict[str, Any], market: str) -> Path:
    """
    media/images/{market_lower}/{ymd}/{slot}
    """
    ymd = _payload_ymd(payload) or datetime.utcnow().strftime("%Y-%m-%d")
    slot = _payload_slot(payload)
    return REPO_ROOT / "media" / "images" / market.lower() / ymd / slot


def write_list_txt(outdir: Path, market: str) -> Path:
    """
    Write list.txt for ffmpeg concat (or your pipeline).
    - Put overview images first
    - Then all other pngs in lexicographic order
    Format: each line is a relative path from outdir: "filename.png"
    """
    pngs = sorted([p for p in outdir.glob("*.png") if p.is_file()], key=lambda p: p.name)

    def _is_overview(p: Path) -> bool:
        n = p.name.lower()
        # be tolerant: "overview.png", "th_overview.png", etc.
        return ("overview" in n) and n.endswith(".png")

    overview = [p for p in pngs if _is_overview(p)]
    others = [p for p in pngs if p not in overview]
    ordered = overview + others

    list_path = outdir / "list.txt"
    # keep it simple: one filename per line (most pipelines accept this)
    list_path.write_text("\n".join([p.name for p in ordered]) + ("\n" if ordered else ""), encoding="utf-8")

    print(f"🧾 list.txt written: {list_path} (items={len(ordered)})")
    return list_path


# =============================================================================
# Drive subfolder helpers
# =============================================================================
def _first_ymd(payload: Dict[str, Any]) -> Optional[str]:
    # (kept for backward compat)
    return _payload_ymd(payload)


def _infer_run_tag(payload: Dict[str, Any]) -> str:
    # (kept for backward compat)
    return _payload_slot(payload)


def make_drive_subfolder_name(payload: Dict[str, Any], market: str) -> str:
    ymd = _first_ymd(payload)
    tag = _infer_run_tag(payload)
    if ymd:
        return f"{market}_{ymd}_{tag}"
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{market}_{now}"


# =============================================================================
# TH Status / Badge
# =============================================================================
def clean_sector_name(s: Any) -> str:
    ss = _s(s) or "Unclassified"
    ss = re.sub(r"\s+", " ", ss).strip()
    if ss in ("-", "--", "—", "–", "－", ""):
        ss = "Unclassified"
    return ss


def is_limitup_locked_th(r: Dict[str, Any]) -> bool:
    return _bool_any(
        r,
        "is_limitup_locked",
        "is_limitup30_locked",
        "is_limitup20_locked",
        "is_limitup10_locked",
    )


def is_limitup_touch_th(r: Dict[str, Any]) -> bool:
    return _bool_any(
        r,
        "is_limitup_touch",
        "is_limitup30_touch",
        "is_limitup20_touch",
        "is_limitup10_touch",
    )


def is_touch_only_th(r: Dict[str, Any]) -> bool:
    return is_limitup_touch_th(r) and (not is_limitup_locked_th(r))


def is_bigup10_th(r: Dict[str, Any]) -> bool:
    return _bool_any(r, "is_bigup10", "is_bigup", "is_bigmove10")


def is_event_stock_th(r: Dict[str, Any], ret_th: float) -> bool:
    """Event if limitup/touch OR ret >= threshold."""
    if is_limitup_locked_th(r) or is_limitup_touch_th(r):
        return True
    return _pct(r.get("ret")) >= float(ret_th)


# -----------------------------------------------------------------------------
# Thai labels (for line2 + badge)
# -----------------------------------------------------------------------------
def badge_text_from_ret_th(ret: float) -> str:
    if ret >= 0.20:
        return "พุ่งแรง"       # Surge
    return "แข็งแกร่ง"         # Strong


def yesterday_text_th(r: Dict[str, Any]) -> str:
    """
    Prefer upstream status_line2 if present.
    Fallback to streak fields.
    NOTE: return Thai by default.
    """
    sl2 = _s(r.get("status_line2"))
    if sl2:
        return sl2

    s30 = _int(r.get("streak30_prev", 0))
    s10 = _int(r.get("streak10_prev", 0))
    if s30 > 0:
        return f"เมื่อวานลิมิต {s30} วัน"
    if s10 > 0:
        return f"เมื่อวาน 10%+ {s10} วัน"
    return "เมื่อวาน: ไม่มี"


def get_new_listing_mark_th(r: Dict[str, Any]) -> str:
    is_new = _bool(r.get("is_new_listing", False))
    if not is_new:
        return ""
    new_date = _s(r.get("new_listing_date", ""))
    if new_date:
        return f"เข้าใหม่({new_date})"
    return "เข้าใหม่"


# =============================================================================
# Penny filter + Debug (CLI-level)
# =============================================================================
def _payload_penny_price_max(payload: Dict[str, Any], default: float) -> float:
    try:
        filt = payload.get("filters") or {}
        v = filt.get("th_penny_price_max")
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def is_penny_th(r: Dict[str, Any], *, price_max: float) -> bool:
    if "is_penny" in r:
        return _bool(r.get("is_penny"))
    close_px = _pct(r.get("close"))
    return (close_px > 0) and (close_px < float(price_max))


def filter_universe_penny(
    universe: List[Dict[str, Any]],
    *,
    price_max: float,
    enabled: bool,
    debug: bool,
) -> List[Dict[str, Any]]:
    if not enabled:
        if debug:
            print(f"[TH_PENNY_FILTER] disabled (price_max={price_max})")
        return universe

    kept: List[Dict[str, Any]] = []
    removed = 0
    for r in universe:
        if is_penny_th(r, price_max=price_max):
            removed += 1
            continue
        kept.append(r)

    if debug:
        print(
            f"[TH_PENNY_FILTER] enabled  price_max={price_max}  removed={removed}  kept={len(kept)}  total={len(universe)}"
        )
    return kept


def debug_touch_vs_10(
    universe: List[Dict[str, Any]],
    *,
    ret_th: float,
    max_rows: int = 120,
) -> None:
    touch_and_ge = []
    touch_but_lt = []

    for r in universe:
        touch = is_limitup_touch_th(r)
        if not touch:
            continue
        ret = _pct(r.get("ret"))
        if ret >= float(ret_th):
            touch_and_ge.append(r)
        else:
            touch_but_lt.append(r)

    print("[TH_TOUCH_VS_10_DEBUG]")
    print(f"  ret_th={float(ret_th):.4f}")
    print(f"  touch_and_ret_ge10 = {len(touch_and_ge)}")
    print(f"  touch_but_ret_lt10 = {len(touch_but_lt)}")

    if touch_but_lt:

        def _key(rr: Dict[str, Any]) -> Tuple[float, float]:
            return (_pct(rr.get("ret")), _pct(rr.get("ret_high")))

        touch_but_lt.sort(key=_key)
        print(f"  offenders (first {int(max_rows)}):")
        for rr in touch_but_lt[: int(max_rows)]:
            sym = _s(rr.get("symbol"))
            name = _s(rr.get("name") or sym)
            sec = clean_sector_name(rr.get("sector"))
            lc = _pct(rr.get("last_close"))
            hi = _pct(rr.get("high"))
            cl = _pct(rr.get("close"))
            ret_pct = _pct(rr.get("ret")) * 100.0
            rh_pct = _pct(rr.get("ret_high")) * 100.0
            print(
                f"   - {sym} | {sec} | {name} | lc={lc:.4f} hi={hi:.4f} close={cl:.4f} ret={ret_pct:.2f}% ret_high={rh_pct:.2f}%"
            )


# =============================================================================
# Builders (sector pages use events + peers)
# =============================================================================
def build_events_by_sector_th(
    universe: List[Dict[str, Any]],
    ret_th: float,
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        if not is_event_stock_th(r, ret_th):
            continue

        sector = clean_sector_name(r.get("sector"))
        sym = _s(r.get("symbol"))
        if not sym:
            continue

        name = _s(r.get("name") or sym)
        ret = _pct(r.get("ret"))

        is_locked = is_limitup_locked_th(r)
        is_touch = is_touch_only_th(r)

        if is_locked:
            badge = "ล็อกเพดาน"
            status = "locked"
        elif is_touch:
            badge = "แตะเพดาน"
            status = "touch"
        else:
            badge = badge_text_from_ret_th(ret)
            status = "surge"

        ytxt = yesterday_text_th(r)
        new_mark = get_new_listing_mark_th(r)

        line1 = f"{sym}  {name}"
        line2 = f"{badge} | {ytxt}" + (f" | {new_mark}" if new_mark else "")

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "badge_text": badge,
                "limitup_status": status,
                "line1": line1,
                "line2": line2,
                "is_limitup_locked": bool(is_locked),
                "is_limitup_touch": bool(is_touch),
                "is_touch_only": bool(is_touch),
                "is_bigup": bool(is_bigup10_th(r)),
                "is_new_listing": bool(_bool(r.get("is_new_listing", False))),
                "streak30_prev": _int(r.get("streak30_prev", 0)),
                "streak10_prev": _int(r.get("streak10_prev", 0)),
                "badge_kind": "surge"
                if (not is_locked and not is_touch and ret >= 0.20)
                else ("strong" if (not is_locked and not is_touch) else status),
            }
        )

    def _rank(st: str) -> int:
        if st == "locked":
            return 0
        if st == "touch":
            return 1
        return 2

    for k in out:
        out[k].sort(
            key=lambda x: (
                -int(x.get("is_new_listing", False)),
                _rank(str(x.get("limitup_status") or "")),
                -(x.get("ret") or 0.0),
            )
        )

    return out


def build_peers_by_sector_th(
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

        if is_event_stock_th(r, float(ret_th_event)):
            continue

        ret = _pct(r.get("ret"))
        if ret < float(ret_min):
            continue

        name = _s(r.get("name") or sym)
        ytxt = yesterday_text_th(r)
        new_mark = get_new_listing_mark_th(r)

        line1 = f"{sym}  {name}"
        line2 = f"แข็งแกร่ง | {ytxt}" + (f" | {new_mark}" if new_mark else "")

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "line1": line1,
                "line2": line2,
                "badge_text": "",
                "is_new_listing": bool(_bool(r.get("is_new_listing", False))),
            }
        )

    for k in out:
        out[k].sort(key=lambda x: (-int(x.get("is_new_listing", False)), -(x.get("ret") or 0.0)))
        out[k] = out[k][: int(max_per_sector)]

    return out


def _apply_th_overview_copy(payload: Dict[str, Any]) -> None:
    payload["overview_title"] = payload.get("overview_title") or "Sector event counts (Top)"
    payload["overview_footer"] = payload.get("overview_footer") or "TH snapshot"
    payload["overview_note"] = payload.get("overview_note") or (
        "Market rules differ; limit-up/touch/10%+ may appear together."
    )


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True)

    # ✅ outdir OPTIONAL (JP/KR style)
    ap.add_argument(
        "--outdir",
        default=None,
        help="output dir (optional). If omitted -> media/images/th/{ymd}/{slot}",
    )

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="th")
    ap.add_argument("--rows-per-box", type=int, default=6)

    # event thresholds
    ap.add_argument("--ret-th", type=float, default=0.10, help="event stock threshold (default 10%)")
    ap.add_argument("--peer-ret-min", type=float, default=0.05)
    ap.add_argument("--peer-max-per-sector", type=int, default=10)

    ap.add_argument("--no-overview", action="store_true")

    # ✅ Upload is DEFAULT ON
    ap.add_argument("--no-upload-drive", action="store_true", help="do not upload to Drive after rendering")

    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default=MARKET)
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

    # ✅ Debug default ON (user passes --no-debug to disable)
    ap.add_argument("--no-debug", action="store_true", help="disable debug prints/env (default: debug ON)")

    # ✅ Penny filter default ON
    ap.add_argument("--no-filter-penny", action="store_true", help="do not filter penny stocks in sector pages")
    ap.add_argument("--penny-price-max", type=float, default=0.15, help="penny threshold: close < this (THB)")

    args = ap.parse_args()

    # ✅ debug env control (JP/KR style)
    debug_on = (not bool(args.no_debug))
    if not debug_on:
        os.environ["OVERVIEW_DEBUG_FOOTER"] = "0"
        os.environ["OVERVIEW_DEBUG_FONTS"] = "0"
        os.environ["OVERVIEW_DEBUG"] = "0"

    penny_filter_on = (not bool(args.no_filter_penny))

    payload = load_payload(args.payload)
    universe = pick_universe(payload)
    if not universe:
        raise RuntimeError("No usable snapshot in payload (need snapshot_main/snapshot_all/...)")

    # ✅ default outdir
    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = _default_outdir_from_payload(payload, market=MARKET)

    outdir.mkdir(parents=True, exist_ok=True)

    ymd = _payload_ymd(payload)
    slot = _payload_slot(payload)

    print(f"[TH] payload={args.payload}")
    print(f"[TH] ymd={ymd or 'UNKNOWN'} slot={slot} outdir={outdir}")
    print(f"[TH] debug={'ON' if debug_on else 'OFF'} penny_filter={'ON' if penny_filter_on else 'OFF'}")

    # Penny threshold: CLI overrides, but show payload filters if available
    penny_price_max = float(args.penny_price_max)
    penny_from_payload = _payload_penny_price_max(payload, penny_price_max)

    # Apply penny filter ONLY to sector pages (events/peers).
    universe_for_pages = filter_universe_penny(
        universe,
        price_max=float(penny_price_max),
        enabled=bool(penny_filter_on),
        debug=bool(debug_on),
    )

    if debug_on and (penny_from_payload != penny_price_max):
        print(
            f"[TH_PENNY_FILTER] payload.th_penny_price_max={penny_from_payload}  cli.penny_price_max={penny_price_max}"
        )

    if debug_on:
        debug_touch_vs_10(universe_for_pages, ret_th=float(args.ret_th), max_rows=120)

    layout = get_layout(args.layout)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    events = build_events_by_sector_th(universe_for_pages, float(args.ret_th))
    peers = build_peers_by_sector_th(
        universe_for_pages,
        events,
        ret_min=float(args.peer_ret_min),
        max_per_sector=int(args.peer_max_per_sector),
        ret_th_event=float(args.ret_th),
    )

    payload.setdefault("market", MARKET)

    # ✅ CRITICAL: NEVER overwrite aggregator sector_summary
    _apply_th_overview_copy(payload)

    # ✅ overview first
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

        locked_total = sum(1 for r in E_total if r.get("limitup_status") == "locked")
        touch_total = sum(1 for r in E_total if r.get("limitup_status") == "touch")
        hit_total = len(E_total)

        locked_shown = sum(1 for r in E_show if r.get("limitup_status") == "locked")
        touch_shown = sum(1 for r in E_show if r.get("limitup_status") == "touch")
        hit_shown = len(E_show)

        for i in range(total_pages):
            limitup_rows = E_pages[i] if i < len(E_pages) else []
            peer_rows = P_pages[i] if i < len(P_pages) else []
            has_more_peers = (len(P_pages) > total_pages) and (i == total_pages - 1)

            safe_sector = re.sub(r"\s+", "_", sector.strip())
            safe_sector = re.sub(r"[^\w\-]+", "_", safe_sector)
            out_path = outdir / f"th_{safe_sector}_p{i+1}.png"

            draw_block_table(
                out_path=out_path,
                layout=layout,
                sector=sector,
                cutoff=cutoff,
                locked_cnt=int(locked_total),
                touch_cnt=int(touch_total),
                theme_cnt=int(hit_total),
                limitup_rows=limitup_rows,
                peer_rows=peer_rows,
                page_idx=i + 1,
                page_total=total_pages,
                width=width,
                height=height,
                rows_per_page=rows_top,
                theme=args.theme,
                time_note=time_note,
                has_more_peers=has_more_peers,
                hit_shown=hit_shown,
                hit_total=hit_total,
                touch_shown=touch_shown,
                touch_total=touch_total,
                locked_shown=locked_shown,
                locked_total=locked_total,
            )

    # ✅ list.txt (for video)
    write_list_txt(outdir, market=MARKET)

    print("✅ TH render finished.")

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