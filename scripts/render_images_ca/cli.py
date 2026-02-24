# scripts/render_images_ca/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

# ✅ VERY IMPORTANT: Force matplotlib headless backend (avoid tkinter warnings)
os.environ.setdefault("MPLBACKEND", "Agg")

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_ca.sector_blocks.draw_mpl import (  # noqa: E402
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_ca.sector_blocks.layout import get_layout  # noqa: E402
from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402

# ✅ Sector order helpers (shared)
from scripts.render_images_common.sector_order import (  # noqa: E402
    normalize_sector_key,
    extract_overview_sector_order,
    reorder_keys_by_overview,
)

MARKET = "CA"


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
    CA snapshot fallback order.
    Your CA close payload often has snapshot_main empty but snapshot_open non-empty.
    """
    for key in ("snapshot_open", "snapshot_all", "snapshot_main", "snapshot"):
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
    """
    CA names are usually not too long, but keep a mild trim.
    """
    s = (name or "").strip()
    if not s:
        return s
    tails = [" Inc.", " Corp.", " Corporation", " Ltd.", " Limited", " PLC", " plc"]
    for t in tails:
        if s.endswith(t):
            s = s[: -len(t)].rstrip()
            break
    return _ellipsize(s, max_len=max_len)


def prev_text(streak_prev: int, th_pct: int = 10) -> str:
    if streak_prev and streak_prev > 0:
        return f"Prev >={th_pct}% for {streak_prev} day(s)"
    return f"Prev < {th_pct}%"


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
    s = _s(payload.get("slot") or payload.get("asof") or "").lower()

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

    meta = payload.get("meta") or {}
    if isinstance(meta, dict):
        mt = meta.get("time") or {}
        if isinstance(mt, dict):
            s2 = _s(mt.get("slot") or mt.get("session") or "").lower()
            if s2 in ("open", "midday", "close"):
                return s2

    return "run"


def default_outdir_from_payload(payload: Dict[str, Any], market: str) -> Path:
    ymd = _payload_ymd(payload) or "unknown"
    slot = _payload_slot(payload) or "run"
    return REPO_ROOT / "media" / "images" / market.lower() / ymd / slot


# =============================================================================
# ✅ NEW: ordered list.txt writer (THIS is the fix)
# =============================================================================
def write_list_txt_ordered(
    outdir: Path,
    *,
    overview_paths: List[Path],
    sector_paths: List[Path],
    filename: str = "list.txt",
    ext: str = "png",
) -> int:
    """
    EXACT order:
      1) overview_paths (render_overview_png return order)
      2) sector_paths   (sector loop order + page order)
      3) remaining pngs in outdir not referenced (alphabetical), safety net
    """
    outdir = outdir.resolve()
    ext = (ext or "png").lstrip(".")

    items: List[Path] = []
    items.extend([Path(p).resolve() for p in (overview_paths or [])])
    items.extend([Path(p).resolve() for p in (sector_paths or [])])

    seen = set()
    lines: List[str] = []

    for p in items:
        if not p.exists():
            continue
        if p in seen:
            continue
        seen.add(p)
        lines.append(p.relative_to(outdir).as_posix())

    others = sorted(outdir.glob(f"*.{ext}"), key=lambda p: p.name.lower())
    for p in others:
        pp = p.resolve()
        if pp in seen:
            continue
        seen.add(pp)
        lines.append(pp.relative_to(outdir).as_posix())

    (outdir / filename).write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return len(lines)


# =============================================================================
# Builders (CA open_limit style, US-compatible semantics)
# =============================================================================
def build_limitup_by_sector(universe: List[Dict[str, Any]], ret_th: float):
    out: Dict[str, List[Dict[str, Any]]] = {}
    th_pct_int = int(round(ret_th * 100))

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
        name = short_company_name(raw_name, max_len=28)

        streak_prev = _int(r.get("streak_prev", 0))
        ptxt = prev_text(streak_prev, th_pct=th_pct_int)

        if is_touch:
            line2 = f"Touched {th_pct_int}% intraday, then pulled back | {ptxt}"
            status = "touch"
        else:
            line2 = f"Close >={th_pct_int}% | {ptxt}"
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
    out: Dict[str, List[Dict[str, Any]]] = {}
    th_pct_int = int(round(ret_th * 100))

    for r in universe:
        ret = _pct(r.get("ret", 0))
        touched_only = _bool(r.get("touched_only", False))

        if (ret >= ret_th) or (touched_only and ret < ret_th):
            continue

        sector = _s(r.get("sector") or "Unknown") or "Unknown"
        sym = _s(r.get("symbol") or "")

        raw_name = _s(r.get("name") or r.get("company_name") or sym) or sym
        name = short_company_name(raw_name, max_len=28)

        streak_prev = _int(r.get("streak_prev", 0))

        out.setdefault(sector, []).append(
            dict(
                symbol=sym,
                name=name,
                sector=sector,
                ret=ret,
                line1=f"{sym}  {name}",
                line2=prev_text(streak_prev, th_pct=th_pct_int),
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
    ap.add_argument("--outdir", default=None, help="Output dir. If omitted, auto: media/images/ca/{ymd}/{slot}")

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="ca")

    ap.add_argument("--rows-per-box", type=int, default=6)
    ap.add_argument("--ret-th", type=float, default=0.10)

    ap.add_argument("--no-overview", action="store_true")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)

    ap.add_argument("--no-debug", action="store_true", help="Disable overview/footer/font debug env (default: ON)")

    args = ap.parse_args()

    debug_on = (not bool(args.no_debug))
    if debug_on:
        os.environ.setdefault("OVERVIEW_DEBUG", "1")
        os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
        os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
    else:
        os.environ["OVERVIEW_DEBUG"] = "0"
        os.environ["OVERVIEW_DEBUG_FOOTER"] = "0"
        os.environ["OVERVIEW_DEBUG_FONTS"] = "0"

    payload = load_payload(args.payload)
    universe = pick_universe(payload)
    if not universe:
        raise RuntimeError("No usable snapshot in payload")

    outdir = Path(args.outdir) if args.outdir else default_outdir_from_payload(payload, market=MARKET)
    outdir.mkdir(parents=True, exist_ok=True)

    layout = get_layout(args.layout)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    overview_sector_keys: List[str] = []
    overview_paths: List[Path] = []
    sector_page_paths: List[Path] = []

    # -------------------------------------------------------------------------
    # 1) Overview first + capture paths + capture order keys
    # -------------------------------------------------------------------------
    if not args.no_overview:
        payload.setdefault("market", MARKET)
        payload.setdefault("asof", payload.get("asof") or payload.get("slot") or "")

        overview_paths = render_overview_png(
            payload=payload,
            out_dir=outdir,
            width=1080,
            height=1920,
            page_size=int(args.overview_page_size),
            metric=str(args.overview_metric),
        ) or []

        for p in overview_paths:
            print(f"✅ 已產生：{p}")

        overview_sector_keys = extract_overview_sector_order(payload)

        if debug_on:
            print("[CA][DEBUG] raw _overview_sector_order exists?:", isinstance(payload.get("_overview_sector_order"), list))
            print("[CA][DEBUG] raw overview order head:", (payload.get("_overview_sector_order", []) or [])[:20])
            if overview_sector_keys:
                met_eff = str(payload.get("_overview_metric_eff") or "").strip()
                print(f"[CA] overview sector order loaded: n={len(overview_sector_keys)}" + (f" metric={met_eff}" if met_eff else ""))
                print("[CA] normalized overview order head:", overview_sector_keys[:20])

    # -------------------------------------------------------------------------
    # 2) Sector pages (capture sector_page_paths in generated order)
    # -------------------------------------------------------------------------
    limitup = build_limitup_by_sector(universe, float(args.ret_th))
    peers = build_peers_by_sector(universe, float(args.ret_th))

    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1

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
    seen_secs = set()
    for k in ordered_norm:
        sec = norm_to_sector.get(k)
        if not sec or sec in seen_secs:
            continue
        ordered_sectors.append(sec)
        seen_secs.add(sec)

    for sector in ordered_sectors:
        L_total = (limitup or {}).get(sector, []) or []
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

        for i in range(total_pages):
            limitup_rows = top_pages[i] if i < len(top_pages) else []
            peer_rows = peer_pages[i] if i < len(peer_pages) else []

            shown_n = min(len(L_total), (i + 1) * rows_top)
            shown_slice = L_total[:shown_n]

            big_shown = sum(1 for x in shown_slice if not x.get("touched_only"))
            touch_shown = sum(1 for x in shown_slice if x.get("touched_only"))

            has_more_peers = (i == total_pages - 1) and (len(peer_pages_all) > len(peer_pages))

            fname = f"ca_{safe_filename(sector)}_p{i+1}.png"
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

            sector_page_paths.append(out_path)

    # -------------------------------------------------------------------------
    # 3) ✅ list.txt ordered (THIS is what you want)
    # -------------------------------------------------------------------------
    n_list = write_list_txt_ordered(
        outdir,
        overview_paths=[Path(p) for p in (overview_paths or [])],
        sector_paths=[Path(p) for p in (sector_page_paths or [])],
        filename="list.txt",
        ext="png",
    )
    print(f"🧾 list.txt written ({n_list} png(s)) -> {outdir / 'list.txt'}")

    if debug_on:
        try:
            head20 = (outdir / "list.txt").read_text(encoding="utf-8").splitlines()[:20]
            print("[CA][DEBUG] list.txt first 20 lines:\n" + "\n".join(head20))
        except Exception:
            pass

    print("\n✅ CA render finished (Drive upload removed).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
