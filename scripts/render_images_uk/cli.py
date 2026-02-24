# scripts/render_images_uk/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

# ✅ headless backend
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# ✅ DEFAULT DEBUG ON (overview + footer) — align JP/KR/TW/TH
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

from scripts.render_images_uk.sector_blocks.draw_mpl import (
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_uk.sector_blocks.layout import get_layout

# ✅ shared ordering helpers (NO local duplicate funcs)
from scripts.render_images_common.sector_order import (
    normalize_sector_key,
    extract_overview_sector_order,
    reorder_keys_by_overview,
    write_list_txt_ordered,
)

MARKET = "UK"


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


def load_payload(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    UK snapshot fallback order (open_limit market): prefer snapshot_open.
    """
    for key in ("snapshot_open", "snapshot_main", "snapshot_all", "snapshot_emerging", "snapshot"):
        rows = payload.get(key) or []
        if isinstance(rows, list) and rows:
            return rows
    return []


def safe_filename(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return "Unknown"
    bad = ["\\", "/", ":", "*", "?", '"', "<", ">", "|"]
    for ch in bad:
        s = s.replace(ch, "_")
    s = s.replace(" ", "_")
    return s[:120]


def _ellipsize(s: str, max_len: int) -> str:
    s = (s or "").strip()
    if max_len <= 0:
        return ""
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def short_company_name(name: str, max_len: int = 28) -> str:
    # UK names are usually shorter; keep mild trim only
    s = (name or "").strip()
    if not s:
        return s
    tails = [" PLC", " plc", " LIMITED", " Limited", " LTD", " Ltd"]
    for t in tails:
        if s.endswith(t):
            s = s[: -len(t)].rstrip()
            break
    return _ellipsize(s, max_len=max_len)


def prev_text(streak_prev: int) -> str:
    # UK 沒漲停連板概念，先沿用 US 的 prev streak 格式
    if streak_prev and streak_prev > 0:
        return f"Prev >=10% for {streak_prev} day(s)"
    return "Prev < 10%"


def mover_badge(ret_pct: float) -> str:
    if ret_pct >= 100.0:
        return "MOON"
    if ret_pct >= 30.0:
        return "SURGE"
    return "MOVER"


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i : i + n] for i in range(0, len(lst), n)]


# =============================================================================
# Payload ymd/slot helpers (JP/KR style)
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


# =============================================================================
# Builders (UK open_limit style)
# =============================================================================
def build_bigmove_by_sector(universe: List[Dict[str, Any]], ret_th: float):
    """
    Top box:
    - big: close ret >= ret_th
    - touch: touched_only=True but close < ret_th   (if your aggregator sets it)
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        ret = _pct(r.get("ret", 0))
        touch_ret = _pct(r.get("touch_ret", 0))
        touched_only = _bool(r.get("touched_only", False))

        is_big = ret >= ret_th
        is_touch = touched_only and (ret < ret_th)

        if not (is_big or is_touch):
            continue

        sector = str(r.get("sector") or "Unknown").strip() or "Unknown"
        sym = str(r.get("symbol") or "").strip()

        raw_name = str(r.get("name") or r.get("company_name") or sym).strip() or sym
        name = short_company_name(raw_name, max_len=28)

        streak_prev = _int(r.get("streak_prev", 0))
        ptxt = prev_text(streak_prev)

        if is_touch:
            line2 = f"Touched {ret_th*100:.0f}% intraday, then pulled back | {ptxt}"
            status = "touch"
        else:
            line2 = f"Close >= {ret_th*100:.0f}% | {ptxt}"
            status = "big"

        ret_pct = ret * 100.0

        out.setdefault(sector, []).append(
            dict(
                symbol=sym,
                name=name,
                sector=sector,
                ret=ret,
                ret_pct=ret_pct,
                touch_ret=touch_ret,
                touched_only=bool(is_touch),
                line1=f"{sym}  {name}",
                line2=line2,
                limitup_status=status,
                badge_text=mover_badge(ret_pct) if not is_touch else "TOUCHED",
                streak_prev=streak_prev,
            )
        )

    for k, rows in out.items():
        big_rows = [x for x in rows if not x.get("touched_only")]
        touch_rows = [x for x in rows if x.get("touched_only")]

        big_rows.sort(key=lambda x: float(x.get("ret", 0.0)), reverse=True)
        touch_rows.sort(key=lambda x: float(x.get("touch_ret", 0.0)), reverse=True)
        out[k] = big_rows + touch_rows

    return out


def build_peers_by_sector(universe: List[Dict[str, Any]], ret_th: float):
    """
    Bottom peers: not big, not touched.
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        ret = _pct(r.get("ret", 0))
        touched_only = _bool(r.get("touched_only", False))
        if (ret >= ret_th) or (touched_only and ret < ret_th):
            continue

        sector = str(r.get("sector") or "Unknown").strip() or "Unknown"
        sym = str(r.get("symbol") or "").strip()

        raw_name = str(r.get("name") or r.get("company_name") or sym).strip() or sym
        name = short_company_name(raw_name, max_len=28)

        streak_prev = _int(r.get("streak_prev", 0))
        out.setdefault(sector, []).append(
            dict(
                symbol=sym,
                name=name,
                sector=sector,
                ret=ret,
                line1=f"{sym}  {name}",
                line2=prev_text(streak_prev),
                streak_prev=streak_prev,
            )
        )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0)), reverse=True)

    return out


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--payload", required=True)

    ap.add_argument(
        "--outdir",
        default=None,
        help="output dir (optional). If omitted -> media/images/uk/{ymd}/{slot}",
    )

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="uk")

    ap.add_argument("--rows-per-box", type=int, default=6)
    ap.add_argument("--ret-th", type=float, default=0.10)

    # Overview
    ap.add_argument("--no-overview", action="store_true")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)

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
        raise RuntimeError("No usable snapshot in payload")

    outdir = Path(args.outdir) if args.outdir else _default_outdir_from_payload(payload, market=MARKET)
    outdir.mkdir(parents=True, exist_ok=True)

    ymd = _payload_ymd(payload)
    slot = _payload_slot(payload)

    print(f"[UK] payload={args.payload}")
    print(f"[UK] ymd={ymd or 'UNKNOWN'} slot={slot} outdir={outdir}")
    print(f"[UK] debug={'ON' if debug_on else 'OFF'}")

    layout = get_layout(args.layout)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    ordered_for_list: List[Path] = []

    # -------------------------------------------------------------------------
    # 1) Overview (first) + capture sector order
    # -------------------------------------------------------------------------
    overview_order_keys: List[str] = []
    if not args.no_overview:
        payload.setdefault("market", MARKET)
        payload.setdefault("asof", payload.get("asof") or payload.get("slot") or "")

        try:
            from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402

            overview_paths = render_overview_png(
                payload=payload,
                out_dir=outdir,
                width=1080,
                height=1920,
                page_size=int(args.overview_page_size),
                metric=str(args.overview_metric),
            ) or []

            for p in overview_paths:
                pp = Path(p)
                if pp.is_file():
                    ordered_for_list.append(pp)

            print(
                "[UK][DEBUG] raw _overview_sector_order exists?:",
                isinstance(payload.get("_overview_sector_order"), list),
            )
            print("[UK][DEBUG] raw overview order head:", (payload.get("_overview_sector_order", []) or [])[:20])

            overview_order_keys = extract_overview_sector_order(payload)
            if overview_order_keys:
                met_eff = str(payload.get("_overview_metric_eff") or "").strip()
                print(
                    f"[UK] overview sector order loaded: n={len(overview_order_keys)}"
                    + (f" metric={met_eff}" if met_eff else "")
                )
                print("[UK] normalized overview order head:", overview_order_keys[:20])
            else:
                print("[UK][WARN] overview order empty after normalization", flush=True)

        except Exception as e:
            print(f"[UK][WARN] overview skipped due to import/render error: {e}", flush=True)

    # -------------------------------------------------------------------------
    # 2) Sector pages
    # -------------------------------------------------------------------------
    movers = build_bigmove_by_sector(universe, float(args.ret_th))
    peers = build_peers_by_sector(universe, float(args.ret_th))

    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1

    movers_sectors = list((movers or {}).keys())

    # normalize -> original sector name
    norm_to_sector: Dict[str, str] = {}
    existing_norm_keys: List[str] = []
    for sec in movers_sectors:
        k = normalize_sector_key(sec)
        if not k:
            continue
        existing_norm_keys.append(k)
        if k not in norm_to_sector:
            norm_to_sector[k] = sec

    if overview_order_keys:
        ordered_norm_keys = reorder_keys_by_overview(existing_keys=existing_norm_keys, overview_keys=overview_order_keys)
    else:
        ordered_norm_keys = existing_norm_keys

    for k in ordered_norm_keys:
        sector = norm_to_sector.get(k)
        if not sector:
            continue

        L_total = (movers or {}).get(sector, []) or []
        P_all = peers.get(sector, []) or []

        big_total = len([x for x in L_total if not x.get("touched_only")])
        touch_total = len([x for x in L_total if x.get("touched_only")])

        top_pages = chunk(L_total, rows_top)
        peer_pages_all = chunk(P_all, rows_peer)

        peer_cap = len(top_pages) + 1
        peer_pages = peer_pages_all[:peer_cap]

        total_pages = max(len(top_pages), len(peer_pages))
        if total_pages <= 0:
            continue

        sector_shown_total = big_total + touch_total
        sector_all_total = sector_shown_total + len(P_all)

        safe_sector = safe_filename(sector)

        for i in range(total_pages):
            mover_rows = top_pages[i] if i < len(top_pages) else []
            peer_rows = peer_pages[i] if i < len(peer_pages) else []

            shown_n = min(len(L_total), (i + 1) * rows_top)
            shown_slice = L_total[:shown_n]

            big_shown = sum(1 for x in shown_slice if not x.get("touched_only"))
            touch_shown = sum(1 for x in shown_slice if x.get("touched_only"))

            has_more_peers = (i == total_pages - 1) and (len(peer_pages_all) > len(peer_pages))

            fname = f"uk_{safe_sector}_p{i+1}.png"
            out_path = outdir / fname

            draw_block_table(
                out_path=out_path,
                layout=layout,
                sector=sector,
                cutoff=cutoff,
                locked_cnt=0,
                touch_cnt=touch_total,
                theme_cnt=big_total,
                hit_shown=big_shown,
                hit_total=big_total,
                touch_shown=touch_shown,
                touch_total=touch_total,
                sector_shown_total=sector_shown_total,
                sector_all_total=sector_all_total,
                limitup_rows=mover_rows,
                peer_rows=peer_rows,
                page_idx=i + 1,
                page_total=total_pages,
                width=1080,
                height=1920,
                rows_per_page=rows_top,
                theme=args.theme,
                time_note=time_note,
                has_more_peers=has_more_peers,
            )

            if out_path.is_file():
                ordered_for_list.append(out_path)

    # -------------------------------------------------------------------------
    # 3) list.txt
    # -------------------------------------------------------------------------
    if overview_order_keys:
        write_list_txt_ordered(outdir, ordered_for_list, filename="list.txt")
    else:
        write_list_txt(outdir, market=MARKET)

    print("\n✅ UK render finished (Drive upload removed).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
