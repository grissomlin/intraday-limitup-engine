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
# IMPORTS (guaranteed safe)
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


def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    return str(x).strip().lower() in ("1", "true", "yes", "y", "on")


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _sanitize_filename(s: str) -> str:
    s = _safe_str(s)
    if not s:
        return "unknown"
    s = s.replace(" ", "_")
    s = re.sub(r"[^\w\u4e00-\u9fff\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def _norm_sector_name(s: str) -> str:
    ss = _safe_str(s or "").strip()
    if ss in ("", "—", "-", "--", "－", "–"):
        return "Unclassified"

    low = ss.lower()
    if low in ("error", "unknown", "n/a", "na", "none", "null"):
        return "Unclassified"

    return ss


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


def _payload_cutoff_str(payload: Dict[str, Any]) -> str:
    return _safe_str(payload.get("cutoff") or payload.get("asof") or payload.get("slot") or "close")


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i: i + n] for i in range(0, len(lst), n)]


# =============================================================================
# TIME NOTE
# =============================================================================
def build_in_time_note(payload: Dict[str, Any]) -> str:
    trade_ymd = _payload_ymd(payload)
    meta = payload.get("meta") or {}
    tmeta = meta.get("time") or {}
    if not isinstance(tmeta, dict):
        tmeta = {}

    hm = _safe_str(tmeta.get("market_finished_hm") or "")
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
# list.txt generator (unified)
# =============================================================================
def write_list_txt(
    outdir: Path,
    *,
    ext: str = "png",
    overview_prefix: str = "overview_sectors_",
    filename: str = "list.txt",
) -> Path:
    outdir = outdir.resolve()
    ext = (ext or "png").lstrip(".")
    overview_prefix = str(overview_prefix or "").strip() or "overview_sectors_"

    items: List[Path] = []
    paged = sorted(outdir.glob(f"{overview_prefix}*_p*.{ext}"), key=lambda p: p.name)
    if paged:
        items.extend(paged)
    else:
        items.extend(sorted(outdir.glob(f"{overview_prefix}*.{ext}"), key=lambda p: p.name))

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
# Builders (India)
# =============================================================================
def _touch_any_in(r: Dict[str, Any]) -> bool:
    if "is_limitup_touch_any" in r:
        return _bool(r.get("is_limitup_touch_any", False))
    return _bool(r.get("is_limitup_touch", False))


def _bombed_in(r: Dict[str, Any]) -> bool:
    if "is_limitup_opened" in r:
        return _bool(r.get("is_limitup_opened", False))
    return _touch_any_in(r) and (not _bool(r.get("is_limitup_locked", False)))


def _is_big10_in(
    r: Dict[str, Any],
    *,
    ret: Optional[float] = None,
    touch_any: Optional[bool] = None,
    is_locked: Optional[bool] = None
) -> bool:
    """
    ✅ Big 10%+ logic (robust):
    - Prefer aggregator flag is_surge_ge10 / is_bigup10 / is_bigmove10
    - Fallback to pure ret>=0.10
    - Must NOT be touch_any (otherwise it's band-touch category)
    - Must NOT be locked (locked is its own category)
    """
    if ret is None:
        ret = _pct(r.get("ret", 0.0))
    if touch_any is None:
        touch_any = _touch_any_in(r)
    if is_locked is None:
        is_locked = _bool(r.get("is_limitup_locked", False))

    flag10 = (
        _bool(r.get("is_surge_ge10", False))
        or _bool(r.get("is_bigup10", False))
        or _bool(r.get("is_bigmove10", False))
        or _bool(r.get("is_bigup", False))
    )
    return bool((ret >= 0.10 or flag10) and (not touch_any) and (not is_locked))


def _pick_band_pct(r: Dict[str, Any]) -> Optional[float]:
    """
    normalized band ratio, e.g. 0.05 / 0.10 / 0.20
    """
    for k in ("band_pct", "limit_rate_pct", "limit_rate", "limit_pct"):
        if k not in r:
            continue
        v = r.get(k)
        if v is None:
            continue
        try:
            fv = float(v)
            if fv <= 0:
                continue
            # pct form like 5 / 10 / 20 -> convert to ratio
            if fv > 1.5:
                fv = fv / 100.0
            return fv
        except Exception:
            continue
    return None


def _row_prev_status(r: Dict[str, Any]) -> str:
    return _safe_str(
        r.get("prev_status")
        or r.get("prev_limitup_status")
        or ""
    )


def _row_today_status(r: Dict[str, Any]) -> str:
    return _safe_str(
        r.get("today_status")
        or r.get("limitup_status")
        or ""
    )


def _row_streak_today(r: Dict[str, Any]) -> int:
    return _safe_int(r.get("streak_today"), _safe_int(r.get("streak"), 0))


def _row_streak_prev(r: Dict[str, Any]) -> int:
    return _safe_int(r.get("streak_prev"), 0)


def build_limitup_by_sector_in(universe: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        is_locked = _bool(r.get("is_limitup_locked", False))
        touch_any = _touch_any_in(r)
        touch_only = _bombed_in(r)
        ret = _pct(r.get("ret", 0.0))

        big10 = _is_big10_in(r, ret=ret, touch_any=touch_any, is_locked=is_locked)

        if not (is_locked or touch_only or big10):
            continue

        sector = _norm_sector_name(_safe_str(r.get("sector") or "Unclassified"))
        sym = _safe_str(r.get("symbol") or "")
        name = _safe_str(r.get("name") or sym)
        band_pct = _pick_band_pct(r)

        if big10:
            status = "big"
        elif touch_only:
            status = "touch"
        else:
            status = "hit"

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "line1": f"{sym}  {name}",
                "line2": "",

                # status / streak fields for top 2nd line
                "limitup_status": status,
                "today_status": _row_today_status(r) or status,
                "prev_status": _row_prev_status(r),
                "streak_today": _row_streak_today(r),
                "streak_prev": _row_streak_prev(r),

                # band fields for Limit X% pill
                "band_pct": band_pct,
                "limit_rate": (band_pct * 100.0) if band_pct is not None else None,
                "limit_rate_pct": (band_pct * 100.0) if band_pct is not None else None,

                # keep raw helpers
                "market_detail": r.get("market_detail"),
            }
        )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0) or 0.0), reverse=True)

    return out


def _get_prev_ret_pct(r: Dict[str, Any]) -> Optional[float]:
    """
    Return percent value for display, e.g. +5.23 not 0.0523
    """
    if "prev_ret_pct" in r and r.get("prev_ret_pct") is not None:
        try:
            return float(r.get("prev_ret_pct"))
        except Exception:
            pass

    for k in ("ret_prev", "ret_prev1", "prev_ret", "ret_1d", "ret_prev_session", "ret_prev_day"):
        if k in r and r.get(k) is not None:
            try:
                v = float(r.get(k))
                return v * 100.0 if abs(v) < 1.5 else v
            except Exception:
                continue
    return None


def build_peers_by_sector_in(universe: List[Dict[str, Any]], *, peer_ret_min: float = 0.0) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        is_locked = _bool(r.get("is_limitup_locked", False))
        touch_any = _touch_any_in(r)
        touch_only = _bombed_in(r)
        ret = _pct(r.get("ret", 0.0))
        big10 = _is_big10_in(r, ret=ret, touch_any=touch_any, is_locked=is_locked)

        # peers: exclude any display items & exclude touch_any
        if is_locked or touch_only or big10 or touch_any:
            continue

        if ret < float(peer_ret_min):
            continue

        sector = _norm_sector_name(_safe_str(r.get("sector") or "Unclassified"))
        sym = _safe_str(r.get("symbol") or "")
        name = _safe_str(r.get("name") or sym)
        band_pct = _pick_band_pct(r)
        prev_ret_pct = _get_prev_ret_pct(r)

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "line1": f"{sym}  {name}",
                "line2": "",

                # peer 2nd line fields
                "prev_ret_pct": prev_ret_pct,
                "prev_status": _row_prev_status(r),
                "streak_prev": _row_streak_prev(r),

                # optional keep today too
                "today_status": _row_today_status(r),
                "streak_today": _row_streak_today(r),

                # band fields for Limit X% pill
                "band_pct": band_pct,
                "limit_rate": (band_pct * 100.0) if band_pct is not None else None,
                "limit_rate_pct": (band_pct * 100.0) if band_pct is not None else None,

                "market_detail": r.get("market_detail"),
            }
        )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0) or 0.0), reverse=True)

    return out


def count_hit_touch_big(rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    hit = 0
    touch = 0
    big = 0
    for r in rows:
        s = _safe_str(r.get("limitup_status") or "").lower()
        if s in ("touch", "bomb"):
            touch += 1
        elif s == "big":
            big += 1
        else:
            hit += 1
    return hit, touch, big


def _norm_overview_metric_arg(s: str) -> str:
    v = (s or "").strip().lower()
    if not v or v == "auto":
        return "auto"
    if v in ("all", "bigmove10+locked+touched"):
        return "mix"
    if v == "locked_plus_touched":
        return "locked+touched"
    return v


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

    # ✅ peers filter gate
    ap.add_argument("--peer-ret-min", type=float, default=0.0)

    args = ap.parse_args()

    if args.no_debug:
        os.environ["OVERVIEW_DEBUG_FOOTER"] = "0"
        os.environ["OVERVIEW_DEBUG_FONTS"] = "0"
        os.environ["OVERVIEW_DEBUG"] = "0"

    payload = load_payload(args.payload)
    universe0 = pick_universe(payload)
    if not universe0:
        raise RuntimeError("No usable snapshot in payload")

    ymd = _payload_ymd(payload) or "unknown"
    slot = _payload_slot(payload)

    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = REPO_ROOT / "media" / "images" / "in" / ymd / slot
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[IN] payload={args.payload}")
    print(f"[IN] ymd={ymd} slot={slot} outdir={outdir}")
    print(f"[IN] repo_root={REPO_ROOT}")

    # aggregate
    agg_payload = in_aggregate(payload)
    universe = pick_universe(agg_payload) or universe0

    # DEBUG
    ret_ge10 = sum(1 for r in universe if _pct(r.get("ret", 0.0)) >= 0.10)
    ret_ge5 = sum(1 for r in universe if _pct(r.get("ret", 0.0)) >= 0.05)
    print(f"[IN][DEBUG] universe={len(universe)} ret>=5%={ret_ge5} ret>=10%={ret_ge10}")

    layout = get_layout(args.layout)
    cutoff = _payload_cutoff_str(agg_payload)
    time_note = build_in_time_note(agg_payload)

    # -------------------------------------------------------------------------
    # 0) Overview first + capture overview sector order
    # -------------------------------------------------------------------------
    overview_sector_keys: List[str] = []
    if not args.no_overview:
        try:
            om = _norm_overview_metric_arg(str(args.overview_metric))
            if om == "auto":
                agg_payload.setdefault("meta", {})
                agg_payload["meta"].setdefault("overview_metric", "mix")
                om = "mix"

            payload_for_overview = dict(agg_payload)
            payload_for_overview.setdefault("market", "IN")
            payload_for_overview.setdefault(
                "asof",
                payload_for_overview.get("asof") or payload_for_overview.get("slot") or ""
            )

            render_overview_png(
                payload_for_overview,
                outdir,
                width=1080,
                height=1920,
                page_size=int(args.overview_page_size),
                metric=om,
            )
            overview_sector_keys = extract_overview_sector_order(payload_for_overview)

            print("[IN] overview done. sector_order_n=", len(overview_sector_keys))
        except Exception as e:
            print(f"[IN] overview failed (continue): {e}")

    # -------------------------------------------------------------------------
    # 1) Sector pages
    # -------------------------------------------------------------------------
    sector_total_map: Dict[str, int] = {}
    for r in universe:
        s = _norm_sector_name(_safe_str(r.get("sector") or "Unclassified"))
        sector_total_map[s] = sector_total_map.get(s, 0) + 1

    limitup = build_limitup_by_sector_in(universe)
    peers = build_peers_by_sector_in(universe, peer_ret_min=float(args.peer_ret_min))

    print(f"[IN][DEBUG] sectors(limitup)={len(limitup)} sectors(peers)={len(peers)}")

    sectors_raw = sorted(set((limitup or {}).keys()) | set((peers or {}).keys()))
    if not sectors_raw:
        print("[IN][WARN] No sectors to render (limitup empty & peers empty). Only overview will exist.")
    else:
        norm_to_sector: Dict[str, str] = {}
        existing_norm_keys: List[str] = []
        for sec in sectors_raw:
            k = normalize_sector_key(sec)
            if not k:
                continue
            existing_norm_keys.append(k)
            norm_to_sector.setdefault(k, sec)

        if overview_sector_keys:
            ordered_norm = reorder_keys_by_overview(
                existing_keys=existing_norm_keys,
                overview_keys=overview_sector_keys
            )
        else:
            ordered_norm = existing_norm_keys

        ordered_sectors: List[str] = []
        seen = set()
        for k in ordered_norm:
            sec = norm_to_sector.get(k)
            if not sec or sec in seen:
                continue
            ordered_sectors.append(sec)
            seen.add(sec)

        width, height = 1080, 1920
        rows_top = max(1, int(args.rows_per_box))
        rows_peer = rows_top + 1
        CAP_PAGES = max(1, int(args.cap_pages))

        for sector in ordered_sectors:
            L_total = (limitup or {}).get(sector, []) or []
            P_total = (peers or {}).get(sector, []) or []

            sector_fn = _sanitize_filename(sector)

            # ----------------------------
            # CASE A: has top display items
            # ----------------------------
            if L_total:
                max_limitup_show = CAP_PAGES * rows_top
                L_show = L_total[:max_limitup_show]
                L_pages = chunk(L_show, rows_top) if L_show else [[]]
                P_pages = chunk(P_total, rows_peer) if P_total else [[]]

                limitup_pages = len(L_pages)
                peer_pages = len(P_pages)

                total_pages = max(1, limitup_pages, peer_pages)
                if total_pages > CAP_PAGES:
                    total_pages = CAP_PAGES

                hit_total, touch_total, big_total = count_hit_touch_big(L_total)
                hit_shown, touch_shown, big_shown = count_hit_touch_big(L_show)

                sector_all_total = int(sector_total_map.get(sector, 0) or 0)
                sector_shown_total = int(hit_total + touch_total + big_total)

                locked_cnt = hit_total
                touch_cnt = touch_total
                theme_cnt = 0

                for i in range(total_pages):
                    limitup_rows = L_pages[i] if i < limitup_pages else []
                    peer_rows = P_pages[i] if i < peer_pages else []
                    has_more_peers = (peer_pages > total_pages) and (i == total_pages - 1)

                    out_path = outdir / f"in_{sector_fn}_p{i+1}.png"
                    draw_block_table(
                        out_path=out_path,
                        layout=layout,
                        sector=sector,
                        cutoff=cutoff,
                        locked_cnt=locked_cnt,
                        touch_cnt=touch_cnt,
                        theme_cnt=theme_cnt,
                        hit_shown=hit_shown,
                        hit_total=hit_total,
                        touch_shown=touch_shown,
                        touch_total=touch_total,
                        big_shown=big_shown,
                        big_total=big_total,
                        sector_shown_total=sector_shown_total,
                        sector_all_total=sector_all_total,
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
                        lang=str(args.lang or "en"),
                        market="IN",
                    )
                    print(f"[IN] wrote {out_path.name}")

            # ----------------------------
            # CASE B: peers-only sector
            # ----------------------------
            elif P_total:
                page_pack = rows_top + rows_peer
                total_pages = max(1, (len(P_total) + page_pack - 1) // page_pack)
                total_pages = min(total_pages, CAP_PAGES)

                hit_total = touch_total = big_total = 0
                hit_shown = touch_shown = big_shown = 0

                sector_all_total = int(sector_total_map.get(sector, 0) or 0)
                sector_shown_total = 0
                locked_cnt = touch_cnt = theme_cnt = 0

                for i in range(total_pages):
                    start = i * page_pack
                    top_rows = P_total[start: start + rows_top]
                    bot_rows = P_total[start + rows_top: start + rows_top + rows_peer]
                    has_more_peers = (start + page_pack) < len(P_total) and (i == total_pages - 1)

                    out_path = outdir / f"in_{sector_fn}_p{i+1}.png"
                    draw_block_table(
                        out_path=out_path,
                        layout=layout,
                        sector=sector,
                        cutoff=cutoff,
                        locked_cnt=locked_cnt,
                        touch_cnt=touch_cnt,
                        theme_cnt=theme_cnt,
                        hit_shown=hit_shown,
                        hit_total=hit_total,
                        touch_shown=touch_shown,
                        touch_total=touch_total,
                        big_shown=big_shown,
                        big_total=big_total,
                        sector_shown_total=sector_shown_total,
                        sector_all_total=sector_all_total,
                        limitup_rows=top_rows,
                        peer_rows=bot_rows,
                        page_idx=i + 1,
                        page_total=total_pages,
                        width=width,
                        height=height,
                        rows_per_page=rows_top,
                        theme=args.theme,
                        time_note=time_note,
                        has_more_peers=has_more_peers,
                        lang=str(args.lang or "en"),
                        market="IN",
                        top_box_title="Top movers (<10%)",
                        bot_box_title="Peers (same sector)",
                    )
                    print(f"[IN] wrote {out_path.name}")

            else:
                continue

    # -------------------------------------------------------------------------
    # 2) list.txt
    # -------------------------------------------------------------------------
    try:
        list_path = write_list_txt(
            outdir,
            ext="png",
            overview_prefix="overview_sectors_",
            filename="list.txt",
        )
        print(f"[IN] wrote {list_path}")
    except Exception as e:
        print(f"[IN] list.txt generation failed (continue): {e}")

    print("\n✅ IN render finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
