# scripts/render_images_th/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

# =============================================================================
# Headless backend (CRITICAL on CI)
# =============================================================================
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# DEFAULT DEBUG ON
# =============================================================================
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

from scripts.render_images_th.sector_blocks.draw_mpl import (  # noqa: E402
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_th.sector_blocks.layout import get_layout  # noqa: E402

# NOTE:
# Drive upload has been removed from this CLI.
# Keep env read for compatibility, but NO hard-coded fallback folder id.
DEFAULT_ROOT_FOLDER = os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip()

MARKET = "TH"


# =============================================================================
# Utils (keep your originals)
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
    for k in ("snapshot_main", "snapshot_all", "snapshot_open", "snapshot"):
        rows = payload.get(k) or []
        if isinstance(rows, list) and rows:
            return rows
    rows = payload.get("universe") or []
    if isinstance(rows, list) and rows:
        return rows
    return []


def _env_on(name: str, default: str = "1") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def filter_th_penny_universe(universe: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply TH penny filter for sector pages to match overview behavior.

    Strategy:
    - If TH_FILTER_PENNY is off -> return original
    - Prefer using payload field `is_penny` (set by TH aggregator)
    - Fallback: compute using close < TH_PENNY_PRICE_MAX
    """
    if not universe:
        return universe

    if not _env_on("TH_FILTER_PENNY", "1"):
        return universe

    penny_max = float(os.getenv("TH_PENNY_PRICE_MAX", "0.15"))
    out: List[Dict[str, Any]] = []
    kept = 0
    dropped = 0

    for r in universe:
        try:
            if isinstance(r, dict):
                if "is_penny" in r:
                    if _bool(r.get("is_penny")):
                        dropped += 1
                        continue
                else:
                    if _pct(r.get("close")) < penny_max:
                        dropped += 1
                        continue
        except Exception:
            # if something is weird, keep it (safer than dropping good rows)
            pass

        out.append(r)
        kept += 1

    if _env_on("TH_DEBUG_PENNY", "0"):
        print("[TH][PENNY_FILTER]")
        print("  TH_FILTER_PENNY =", True)
        print("  TH_PENNY_PRICE_MAX =", penny_max)
        print("  kept =", kept)
        print("  dropped =", dropped)

    return out


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i : i + n] for i in range(0, len(lst), n)]


# =============================================================================
# Payload ymd/slot helpers
# =============================================================================
def _payload_ymd(payload: Dict[str, Any]) -> Optional[str]:
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
    s = str(payload.get("slot") or payload.get("asof") or "").strip().lower()

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


def _default_outdir_from_payload(payload: Dict[str, Any], market: str) -> Path:
    ymd = _payload_ymd(payload) or datetime.utcnow().strftime("%Y-%m-%d")
    slot = _payload_slot(payload)
    return REPO_ROOT / "media" / "images" / market.lower() / ymd / slot


def write_list_txt(outdir: Path, market: str) -> Path:
    """
    Original behavior (filename sorted).
    Kept for fallback mode.
    """
    pngs = sorted([p for p in outdir.glob("*.png") if p.is_file()], key=lambda p: p.name)

    def _is_overview(p: Path) -> bool:
        n = p.name.lower()
        return ("overview" in n) and n.endswith(".png")

    overview = [p for p in pngs if _is_overview(p)]
    others = [p for p in pngs if p not in overview]
    ordered = overview + others

    list_path = outdir / "list.txt"
    list_path.write_text("\n".join([p.name for p in ordered]) + ("\n" if ordered else ""), encoding="utf-8")
    print(f"🧾 list.txt written: {list_path} (items={len(ordered)})")
    return list_path


def write_list_txt_ordered(outdir: Path, ordered_paths: List[Path]) -> Path:
    """
    ✅ NEW: write list.txt in the exact order you want (overview first + sector pages by overview order)
    """
    seen = set()
    final: List[Path] = []
    for p in ordered_paths:
        try:
            pp = Path(p)
            if pp.is_file():
                key = pp.name
                if key not in seen:
                    final.append(pp)
                    seen.add(key)
        except Exception:
            continue

    list_path = outdir / "list.txt"
    list_path.write_text("\n".join([p.name for p in final]) + ("\n" if final else ""), encoding="utf-8")
    print(f"🧾 list.txt written (ordered): {list_path} (items={len(final)})")
    return list_path


# =============================================================================
# TH Status / Badge (keep your originals)
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
    if is_limitup_locked_th(r) or is_limitup_touch_th(r):
        return True
    return _pct(r.get("ret")) >= float(ret_th)


def badge_text_from_ret_th(ret: float) -> str:
    if ret >= 0.20:
        return "พุ่งแรง"
    return "แข็งแกร่ง"


def yesterday_text_th(r: Dict[str, Any]) -> str:
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
# ✅ NEW: overview sector order helpers
# =============================================================================
def normalize_sector_key(s: Any) -> str:
    """
    Normalize sector names for matching overview order:
    - strip
    - collapse whitespace
    - lowercase
    """
    ss = _s(s)
    ss = re.sub(r"\s+", " ", ss).strip().lower()
    return ss


def _extract_overview_sector_order(payload: Dict[str, Any]) -> List[str]:
    raw = payload.get("_overview_sector_order", []) or []
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for x in raw:
        k = normalize_sector_key(x)
        if k:
            out.append(k)
    # uniq keep order
    seen = set()
    out2: List[str] = []
    for k in out:
        if k not in seen:
            out2.append(k)
            seen.add(k)
    return out2


# =============================================================================
# Builders (as your original)
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
        # ✅ keep your original behavior
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

    ap.add_argument(
        "--outdir",
        default=None,
        help="output dir (optional). If omitted -> media/images/th/{ymd}/{slot}",
    )

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="th")
    ap.add_argument("--rows-per-box", type=int, default=6)

    ap.add_argument("--ret-th", type=float, default=0.10, help="event stock threshold (default 10%)")
    ap.add_argument("--peer-ret-min", type=float, default=0.05)
    ap.add_argument("--peer-max-per-sector", type=int, default=10)

    ap.add_argument("--no-overview", action="store_true")

    # ✅ Drive upload 기능 완전移除（不再提供任何 drive 參數）
    ap.add_argument("--no-debug", action="store_true", help="disable debug prints/env (default: debug ON)")

    args = ap.parse_args()

    debug_on = (not bool(args.no_debug))
    if not debug_on:
        os.environ["OVERVIEW_DEBUG_FOOTER"] = "0"
        os.environ["OVERVIEW_DEBUG_FONTS"] = "0"
        os.environ["OVERVIEW_DEBUG"] = "0"

    payload = load_payload(args.payload)
    universe = pick_universe(payload)
    if not universe:
        raise RuntimeError("No usable snapshot in payload (need snapshot_main/snapshot_all/...)")

    # ✅ NEW: apply TH penny filter for sector pages (match overview)
    before_n = len(universe)
    universe = filter_th_penny_universe(universe)
    after_n = len(universe)
    if _env_on("TH_DEBUG_PENNY", "0"):
        print(f"[TH][PENNY_FILTER] universe: {before_n} -> {after_n}")

    # ✅ CRITICAL: create outdir FIRST so run_shorts won't fail with 'missing'
    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = _default_outdir_from_payload(payload, market=MARKET)
    outdir.mkdir(parents=True, exist_ok=True)

    ymd = _payload_ymd(payload)
    slot = _payload_slot(payload)

    print(f"[TH] payload={args.payload}")
    print(f"[TH] ymd={ymd or 'UNKNOWN'} slot={slot} outdir={outdir}")
    print(f"[TH] debug={'ON' if debug_on else 'OFF'}")

    layout = get_layout(args.layout)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    events = build_events_by_sector_th(universe, float(args.ret_th))
    peers = build_peers_by_sector_th(
        universe,
        events,
        ret_min=float(args.peer_ret_min),
        max_per_sector=int(args.peer_max_per_sector),
        ret_th_event=float(args.ret_th),
    )

    payload.setdefault("market", MARKET)
    _apply_th_overview_copy(payload)

    # We'll build an explicit render order list for list.txt
    ordered_for_list: List[Path] = []

    # ✅ overview import moved INSIDE main + protected
    overview_order_keys: List[str] = []
    if not args.no_overview:
        try:
            from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402

            overview_paths = render_overview_png(payload, outdir) or []
            # overview images should come first in list.txt
            ordered_for_list.extend([Path(p) for p in overview_paths if Path(p).is_file()])

            # ✅ NEW: read sector order exported by overview renderer (if present)
            print(
                "[TH][DEBUG] raw _overview_sector_order exists?:",
                isinstance(payload.get("_overview_sector_order"), list),
            )
            print("[TH][DEBUG] raw overview order head:", (payload.get("_overview_sector_order", []) or [])[:20])

            overview_order_keys = _extract_overview_sector_order(payload)
            if overview_order_keys:
                met_eff = str(payload.get("_overview_metric_eff") or "").strip()
                print(
                    f"[TH] overview sector order loaded: n={len(overview_order_keys)}"
                    + (f" metric={met_eff}" if met_eff else "")
                )
                print("[TH] normalized overview order head:", overview_order_keys[:20])
            else:
                print("[TH][WARN] overview order empty after normalization", flush=True)
        except Exception as e:
            print(f"[TH][WARN] overview skipped due to import/render error: {e}", flush=True)

    width, height = 1080, 1920
    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1
    CAP_PAGES = 5

    # =============================================================================
    # ✅ NEW: reorder sectors by overview order (then append remaining sectors)
    # =============================================================================
    sectors_events = list((events or {}).keys())

    # Map normalized key -> original sector name(s)
    key_to_sector: Dict[str, str] = {}
    for sec in sectors_events:
        k = normalize_sector_key(sec)
        # first wins (stable)
        if k and k not in key_to_sector:
            key_to_sector[k] = sec

    ordered_sectors: List[str] = []
    if overview_order_keys:
        # add sectors that appear in overview order
        for k in overview_order_keys:
            sec = key_to_sector.get(k)
            if sec and sec in (events or {}):
                ordered_sectors.append(sec)

        # append remaining sectors (keep original insertion order from dict)
        seen = set(normalize_sector_key(s) for s in ordered_sectors)
        for sec in sectors_events:
            k = normalize_sector_key(sec)
            if k not in seen:
                ordered_sectors.append(sec)
                seen.add(k)
    else:
        ordered_sectors = sectors_events  # fallback

    # =============================================================================
    # Render sector pages in the chosen order + record paths for list.txt
    # =============================================================================
    for sector in ordered_sectors:
        E_total = (events or {}).get(sector, []) or []
        P_total = peers.get(sector, []) or []

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

        safe_sector = re.sub(r"\s+", "_", sector.strip())
        safe_sector = re.sub(r"[^\w\-]+", "_", safe_sector)

        for i in range(total_pages):
            limitup_rows = E_pages[i] if i < len(E_pages) else []
            peer_rows = P_pages[i] if i < len(P_pages) else []
            has_more_peers = (len(P_pages) > total_pages) and (i == total_pages - 1)

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

            # ✅ record sector pages in the EXACT sequence we rendered
            if out_path.is_file():
                ordered_for_list.append(out_path)

    # =============================================================================
    # list.txt
    # =============================================================================
    if overview_order_keys:
        # ✅ keep the explicit order (overview + sector pages)
        write_list_txt_ordered(outdir, ordered_for_list)
    else:
        # fallback to old behavior
        write_list_txt(outdir, market=MARKET)

    print("✅ TH render finished. (Drive upload removed)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
