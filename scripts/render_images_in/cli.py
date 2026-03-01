# scripts/render_images_in/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ✅ headless backend
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# ✅ DEFAULT DEBUG ON (overview + footer) — align CN/JP/KR/TW
# =============================================================================
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

# Optional: try disable gainbins
os.environ.setdefault("OVERVIEW_GAINBINS", "0")
os.environ.setdefault("OVERVIEW_ENABLE_GAINBINS", "0")
os.environ.setdefault("OVERVIEW_DISABLE_GAINBINS", "1")
os.environ.setdefault("OVERVIEW_NO_GAINBINS", "1")

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

# ✅ shared ordering helpers
from scripts.render_images_common.sector_order import (  # noqa: E402
    normalize_sector_key,
    extract_overview_sector_order,
    reorder_keys_by_overview,
)

# aggregator (no re-download)
from markets.india.aggregator import aggregate as in_aggregate  # noqa: E402


# =============================================================================
# Small utils
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
    s = str(x).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


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
    s = _safe_str(payload.get("slot") or "")
    return s or "unknown"


def _payload_cutoff_str(payload: Dict[str, Any]) -> str:
    return _safe_str(payload.get("cutoff") or payload.get("asof") or payload.get("slot") or "close")


def _sanitize_filename(s: str) -> str:
    s = _safe_str(s)
    if not s:
        return "unknown"
    s = s.replace(" ", "_")
    s = re.sub(r"[^\w\u4e00-\u9fff\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def _norm_sector_name(s: str) -> str:
    ss = _safe_str(s or "")
    if ss.strip() in ("", "—", "-", "--", "－", "–"):
        return "Unclassified"
    return ss


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i : i + n] for i in range(0, len(lst), n)]


# =============================================================================
# Time note builder (IN one-line)
# India Trading Day YYYY-MM-DD  Updated YYYY-MM-DD HH:MM
# =============================================================================
def _split_ymd_from_dt(s: str) -> str:
    s = _safe_str(s)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return ""


def build_in_time_note(payload: Dict[str, Any]) -> str:
    trade_ymd = _payload_ymd(payload)

    meta = payload.get("meta") or {}
    tmeta = meta.get("time") or {}
    if not isinstance(tmeta, dict):
        tmeta = {}

    hm = _safe_str(tmeta.get("market_finished_hm") or "")
    finished_at = _safe_str(tmeta.get("market_finished_at") or "")
    update_ymd = _split_ymd_from_dt(finished_at) or trade_ymd

    if not hm:
        _, _, _, hhmm = get_market_time_info(payload, market="IN")
        hm = _safe_str(hhmm)

    if hm:
        return f"India Trading Day {trade_ymd}  Updated {update_ymd} {hm}"
    return f"India Trading Day {trade_ymd}"


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
        single_or_any = sorted(outdir.glob(f"{overview_prefix}*.{ext}"), key=lambda p: p.name)
        items.extend(single_or_any)

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
# - Use snapshot_main (full universe) and flags computed by aggregator
# - status:
#     hit   = locked
#     touch = touched_only (touch but not locked)
#     big   = >=10% and NOT touch_any
# =============================================================================
def _touch_any_in(r: Dict[str, Any]) -> bool:
    if "is_limitup_touch_any" in r:
        return _bool(r.get("is_limitup_touch_any", False))
    return _bool(r.get("is_limitup_touch", False))


def _bombed_in(r: Dict[str, Any]) -> bool:
    # India: "touch only" behaves like CN bombed/touch-only
    if "is_limitup_opened" in r:
        return _bool(r.get("is_limitup_opened", False))
    return _touch_any_in(r) and (not _bool(r.get("is_limitup_locked", False)))


def build_limitup_by_sector_in(universe: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        is_locked = _bool(r.get("is_limitup_locked", False))
        touch_any = _touch_any_in(r)
        touch_only = _bombed_in(r)

        ret = _pct(r.get("ret", 0.0))
        big10 = bool(_bool(r.get("is_surge_ge10", False)) and (not touch_any))

        if not (is_locked or touch_only or big10):
            continue

        sector = _norm_sector_name(_safe_str(r.get("sector") or "Unclassified"))
        sym = _safe_str(r.get("symbol") or "")
        name = _safe_str(r.get("name") or sym)

        # limit rate (ratio) for pill: from aggregator (limit_rate) or band_pct
        limit_rate = r.get("limit_rate", None)
        if limit_rate is None:
            limit_rate = r.get("band_pct", None)

        badge_text = ""
        status = "hit"
        if big10:
            badge_text = "10%+"
            status = "big"
            line2 = "Big Move 10%+"
        elif touch_only:
            badge_text = "Touch"
            status = "touch"
            line2 = "Touched upper band (not locked)"
        else:
            badge_text = "Locked"
            status = "hit"
            line2 = "Locked at upper band"

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "badge_text": badge_text,
                "line1": f"{sym}  {name}",
                "line2": line2,
                "limitup_status": status,  # hit/touch/big
                "limit_rate": limit_rate,  # ✅ for pill display
            }
        )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0) or 0.0), reverse=True)
    return out


def build_peers_by_sector_in(universe: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        is_locked = _bool(r.get("is_limitup_locked", False))
        touch_any = _touch_any_in(r)
        touch_only = _bombed_in(r)
        ret = _pct(r.get("ret", 0.0))
        big10 = bool(_bool(r.get("is_surge_ge10", False)) and (not touch_any))

        # peers: exclude locked/touch-only/big10 and also exclude touch_any
        if is_locked or touch_only or big10 or touch_any:
            continue

        sector = _norm_sector_name(_safe_str(r.get("sector") or "Unclassified"))

        sym = _safe_str(r.get("symbol") or "")
        name = _safe_str(r.get("name") or sym)

        limit_rate = r.get("limit_rate", None)
        if limit_rate is None:
            limit_rate = r.get("band_pct", None)

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "line1": f"{sym}  {name}",
                "line2": "",
                "limit_rate": limit_rate,
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

    ap.add_argument("--no-overview", action="store_true", help="disable overview page")
    ap.add_argument("--overview-metric", default="auto", help="auto/locked/touched/bigmove10/mix/locked+touched")
    ap.add_argument("--overview-page-size", type=int, default=15)

    # i18n: keep simple
    ap.add_argument("--lang", default="en", help="en/zh_hans/.. (i18n keys if available)")

    # ✅ DEBUG: default ON, allow opt-out
    ap.add_argument("--no-debug", action="store_true", help="overview/footer debug off")

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
    print(
        "[IN] debug="
        f"footer={os.getenv('OVERVIEW_DEBUG_FOOTER','0')} "
        f"fonts={os.getenv('OVERVIEW_DEBUG_FONTS','0')}"
    )

    # aggregate inside CLI (no re-download)
    agg_payload = in_aggregate(payload)
    universe = pick_universe(agg_payload) or universe0

    om = _norm_overview_metric_arg(str(args.overview_metric))
    if om == "auto":
        agg_payload.setdefault("meta", {})
        agg_payload["meta"].setdefault("overview_metric", "mix")

    layout = get_layout(args.layout)
    cutoff = _payload_cutoff_str(agg_payload)

    # time_note
    time_note = build_in_time_note(agg_payload)

    # -------------------------------------------------------------------------
    # 0) Overview first + capture overview sector order
    # -------------------------------------------------------------------------
    overview_sector_keys: List[str] = []
    if not args.no_overview:
        try:
            payload_for_overview = dict(agg_payload)
            payload_for_overview.setdefault("market", "IN")
            payload_for_overview.setdefault("asof", payload_for_overview.get("asof") or payload_for_overview.get("slot") or "")

            render_overview_png(
                payload_for_overview,
                outdir,
                width=1080,
                height=1920,
                page_size=int(args.overview_page_size),
                metric=om,
            )
            overview_sector_keys = extract_overview_sector_order(payload_for_overview)

            print(
                "[IN][DEBUG] raw _overview_sector_order exists?:",
                isinstance(payload_for_overview.get("_overview_sector_order"), list),
            )
            print(
                "[IN][DEBUG] raw overview order head:",
                (payload_for_overview.get("_overview_sector_order", []) or [])[:20],
            )
            if overview_sector_keys:
                met_eff = str(payload_for_overview.get("_overview_metric_eff") or "").strip()
                print(
                    f"[IN] overview sector order loaded: n={len(overview_sector_keys)}"
                    + (f" metric={met_eff}" if met_eff else "")
                )
                print("[IN] normalized overview order head:", overview_sector_keys[:20])
            else:
                print("[IN][WARN] overview order empty after normalization", flush=True)

        except Exception as e:
            print(f"[IN] overview failed (continue): {e}")

        for p in outdir.glob("overview_gainbins*.png"):
            try:
                p.unlink()
                print(f"[IN] removed gainbins page: {p.name}")
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # 1) Sector pages (order uses overview exported order)
    # -------------------------------------------------------------------------
    sector_total_map: Dict[str, int] = {}
    for r in universe:
        s = _norm_sector_name(_safe_str(r.get("sector") or "Unclassified"))
        sector_total_map[s] = sector_total_map.get(s, 0) + 1

    limitup = build_limitup_by_sector_in(universe)
    peers = build_peers_by_sector_in(universe)

    width, height = 1080, 1920
    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1
    CAP_PAGES = max(1, int(args.cap_pages))

    sectors_raw = list((limitup or {}).keys())
    norm_to_sector: Dict[str, str] = {}
    existing_norm_keys: List[str] = []
    for sec in sectors_raw:
        k = normalize_sector_key(sec)
        if not k:
            continue
        existing_norm_keys.append(k)
        if k not in norm_to_sector:
            norm_to_sector[k] = sec

    if overview_sector_keys:
        ordered_norm = reorder_keys_by_overview(existing_keys=existing_norm_keys, overview_keys=overview_sector_keys)
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

    for sector in ordered_sectors:
        L_total = (limitup or {}).get(sector, []) or []
        P = (peers or {}).get(sector, []) or []

        max_limitup_show = CAP_PAGES * rows_top
        L_show = L_total[:max_limitup_show]

        L_pages = chunk(L_show, rows_top) if L_show else [[]]
        P_pages = chunk(P, rows_peer)

        limitup_pages = len(L_pages)
        peer_pages = len(P_pages)

        total_pages = limitup_pages
        if peer_pages > limitup_pages:
            total_pages = limitup_pages + 1
        if total_pages > CAP_PAGES:
            total_pages = CAP_PAGES

        hit_total, touch_total, big_total = count_hit_touch_big(L_total)
        hit_shown, touch_shown, big_shown = count_hit_touch_big(L_show)

        sector_all_total = int(sector_total_map.get(sector, 0) or 0)
        sector_shown_total = int(hit_total + touch_total + big_total)

        # backward-compat params
        locked_cnt = hit_total
        touch_cnt = touch_total
        theme_cnt = 0

        sector_fn = _sanitize_filename(sector)

        for i in range(total_pages):
            limitup_rows = L_pages[i] if i < len(L_pages) else []
            peer_rows = P_pages[i] if i < len(P_pages) else []

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
            print(f"[IN] wrote {out_path}")

    # list.txt
    try:
        list_path = write_list_txt(outdir, ext="png", overview_prefix="overview_sectors_", filename="list.txt")
        print(f"[IN] wrote {list_path}")
    except Exception as e:
        print(f"[IN] list.txt generation failed (continue): {e}")

    print("\n✅ IN render finished. (Drive upload removed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
