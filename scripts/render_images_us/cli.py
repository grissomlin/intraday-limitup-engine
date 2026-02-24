# scripts/render_images_us/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# ✅ VERY IMPORTANT: Force matplotlib headless backend (avoid tkinter warnings)
os.environ.setdefault("MPLBACKEND", "Agg")

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_us.sector_blocks.draw_mpl import (  # noqa: E402
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_us.sector_blocks.layout import get_layout  # noqa: E402
from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402

# ✅ Footer debug helpers (print what overview/footer will output)
from scripts.render_images_common.overview.footer import (  # noqa: E402
    build_footer_center_lines,
    build_footer_right_text,
)

MARKET = "US"


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


def _s(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def load_payload(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Payload snapshot fallback order.
    """
    for key in ("snapshot_all", "snapshot_open", "snapshot_main", "snapshot"):
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


def short_company_name(name: str, max_len: int = 26) -> str:
    """
    Clean US long suffixes.
    """
    s = (name or "").strip()
    if not s:
        return s

    tails = [
        " Class A Common Stock",
        " Class B Common Stock",
        " Ordinary Shares",
        " Common Stock",
        " Ordinary Share",
        " Inc.",
        " Ltd.",
        " Limited",
        " Corporation",
    ]
    for t in tails:
        if s.endswith(t):
            s = s[: -len(t)].rstrip()
            break

    return _ellipsize(s, max_len=max_len)


def prev_text(streak_prev: int) -> str:
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
# Payload helpers (JP-style)
# =============================================================================
def _payload_ymd(payload: Dict[str, Any]) -> str:
    """
    Best-effort YYYY-MM-DD for folder naming.
    Priority similar to JP:
      ymd / ymd_effective / bar_date / date
    fallback: extract from asof/slot strings
    """
    for k in ("ymd", "ymd_effective", "bar_date", "date"):
        v = _s(payload.get(k))
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            return v

    for k in ("asof", "slot"):
        v = _s(payload.get(k))
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)

    return ""


def _payload_slot(payload: Dict[str, Any]) -> str:
    """
    Normalize slot to: open / midday / close / HHMM / run
    Priority: payload.slot -> payload.asof -> payload.meta.time
    """
    s = _s(payload.get("slot") or payload.get("asof") or "").lower()

    # common labels
    if "open" in s:
        return "open"
    if "midday" in s or "noon" in s:
        return "midday"
    if "close" in s:
        return "close"

    # HH:MM in strings
    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return f"{hh:02d}{mm:02d}"

    # try meta.time (optional)
    meta = payload.get("meta") or {}
    if isinstance(meta, dict):
        mt = meta.get("time") or {}
        if isinstance(mt, dict):
            s2 = _s(mt.get("slot") or mt.get("session") or "").lower()
            if s2 in ("open", "midday", "close"):
                return s2

    return "run"


# =============================================================================
# Outdir + list.txt (JP-style)
# =============================================================================
def default_outdir_from_payload(payload: Dict[str, Any], market: str) -> Path:
    """
    Default output path:
      media/images/us/{ymd}/{slot}
    If ymd missing -> media/images/us/unknown/{slot}
    """
    ymd = _payload_ymd(payload) or "unknown"
    slot = _payload_slot(payload) or "run"
    return REPO_ROOT / "media" / "images" / market.lower() / ymd / slot


def write_list_txt(outdir: Path) -> int:
    """
    Write list.txt for video stitching.
    Ordering rule (JP-style):
      1) overview*.png first (alphabetical)
      2) then all other *.png alphabetical
    Each line is a filename (relative), one per line.
    """
    pngs = sorted([p for p in outdir.glob("*.png") if p.is_file()], key=lambda p: p.name.lower())

    overview = [p for p in pngs if p.name.lower().startswith("overview")]
    others = [p for p in pngs if p not in overview]

    ordered = overview + others
    lines = [p.name for p in ordered]

    (outdir / "list.txt").write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return len(lines)


# =============================================================================
# Builders
# =============================================================================
def build_limitup_by_sector(universe: List[Dict[str, Any]], ret_th: float):
    """
    Top box:
    - big: close ret >= ret_th
    - touch: touched_only=True but close < ret_th
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

        sector = _s(r.get("sector") or "Unknown") or "Unknown"
        sym = _s(r.get("symbol") or "")

        raw_name = _s(r.get("name") or r.get("company_name") or sym) or sym
        name = short_company_name(raw_name, max_len=26)

        streak_prev = _int(r.get("streak_prev", 0))
        ptxt = prev_text(streak_prev)

        if is_touch:
            line2 = f"Touched 10% intraday, then pulled back | {ptxt}"
            status = "touch"
        else:
            line2 = f"Close >=10% | {ptxt}"
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

    # Sort: big first, then touch
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

        sector = _s(r.get("sector") or "Unknown") or "Unknown"
        sym = _s(r.get("symbol") or "")

        raw_name = _s(r.get("name") or r.get("company_name") or sym) or sym
        name = short_company_name(raw_name, max_len=26)

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

    # ✅ outdir optional (JP-style)
    ap.add_argument("--outdir", default=None, help="Output dir. If omitted, auto: media/images/us/{ymd}/{slot}")

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="us")

    ap.add_argument("--rows-per-box", type=int, default=6)
    ap.add_argument("--ret-th", type=float, default=0.10)

    # Overview
    ap.add_argument("--no-overview", action="store_true")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)

    # ✅ Debug default ON (user can disable)
    ap.add_argument("--no-debug", action="store_true", help="Disable overview/footer/font debug env (default: ON)")

    args = ap.parse_args()

    # ✅ debug default ON
    debug_on = (not bool(args.no_debug))
    if debug_on:
        os.environ.setdefault("OVERVIEW_DEBUG", "1")
        os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
        os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
    else:
        # explicitly off (avoid inheriting from CI env)
        os.environ["OVERVIEW_DEBUG"] = "0"
        os.environ["OVERVIEW_DEBUG_FOOTER"] = "0"
        os.environ["OVERVIEW_DEBUG_FONTS"] = "0"

    payload = load_payload(args.payload)
    universe = pick_universe(payload)
    if not universe:
        raise RuntimeError("No usable snapshot in payload")

    # outdir resolution (JP-style)
    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = default_outdir_from_payload(payload, market=MARKET)
    outdir.mkdir(parents=True, exist_ok=True)

    # Optional fast debug snapshot + footer preview
    if debug_on:
        print("\n==============================")
        print("[US CLI] OVERVIEW DEBUG SNAPSHOT")
        print("==============================")
        print(f"payload.market={payload.get('market')} slot={payload.get('slot')} asof={payload.get('asof')}")
        print(f"ymd={payload.get('ymd')} ymd_effective={payload.get('ymd_effective')} generated_at={payload.get('generated_at')}")
        stats = payload.get("stats") or {}
        filters = payload.get("filters") or {}
        print(f"stats.snapshot_main_count={stats.get('snapshot_main_count')} snapshot_open_count={stats.get('snapshot_open_count')}")
        print(f"stats.open_limit_watchlist_count={stats.get('open_limit_watchlist_count')}")
        us_sync = (filters.get("us_sync") or {}) if isinstance(filters, dict) else {}
        if isinstance(us_sync, dict):
            print(f"filters.us_sync.total={us_sync.get('total')} success={us_sync.get('success')} failed={us_sync.get('failed')}")
        ss = payload.get("sector_summary") or []
        if isinstance(ss, list):
            ss_sum = 0
            for r in ss:
                if isinstance(r, dict):
                    ss_sum += int(r.get("bigmove10_cnt") or 0)
            print(f"sector_summary rows={len(ss)} sum(bigmove10_cnt)={ss_sum}")

        print("\n[US CLI] footer preview (metric=bigmove10)")
        l1, l2, l3, l4 = build_footer_center_lines(payload, metric="bigmove10")
        rtxt = build_footer_right_text(payload)
        print("CENTER:")
        print("  L1:", l1)
        print("  L2:", l2)
        print("  L3:", l3)
        print("  L4:", l4)
        print("RIGHT :", rtxt)
        print("==============================\n")

    layout = get_layout(args.layout)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    # 1) Overview first
    if not args.no_overview:
        payload.setdefault("market", MARKET)
        payload.setdefault("asof", payload.get("asof") or payload.get("slot") or "")

        render_overview_png(
            payload=payload,
            out_dir=outdir,
            width=1080,
            height=1920,
            page_size=int(args.overview_page_size),
            metric=str(args.overview_metric),
        )

    # 2) Sector pages
    limitup = build_limitup_by_sector(universe, float(args.ret_th))
    peers = build_peers_by_sector(universe, float(args.ret_th))

    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1

    for sector, L_total in (limitup or {}).items():
        P_all = peers.get(sector, [])

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

        for i in range(total_pages):
            limitup_rows = top_pages[i] if i < len(top_pages) else []
            peer_rows = peer_pages[i] if i < len(peer_pages) else []

            shown_n = min(len(L_total), (i + 1) * rows_top)
            shown_slice = L_total[:shown_n]

            big_shown = sum(1 for x in shown_slice if not x.get("touched_only"))
            touch_shown = sum(1 for x in shown_slice if x.get("touched_only"))

            has_more_peers = (i == total_pages - 1) and (len(peer_pages_all) > len(peer_pages))

            fname = f"us_{safe_filename(sector)}_p{i+1}.png"

            draw_block_table(
                out_path=outdir / fname,
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
                limitup_rows=limitup_rows,
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

    # 3) list.txt (for video stitching)
    n_list = write_list_txt(outdir)
    print(f"🧾 list.txt written ({n_list} png(s)) -> {outdir / 'list.txt'}")

    print("\n✅ US render finished (Drive upload removed).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
