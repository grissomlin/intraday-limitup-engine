# scripts/render_images_au/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

# ✅ VERY IMPORTANT: Force matplotlib headless backend (avoid tkinter warnings)
os.environ.setdefault("MPLBACKEND", "Agg")

import re  # noqa: E402
import sys  # noqa: E402
from datetime import datetime  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any, Dict, List, Tuple, Optional  # noqa: E402

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_au.sector_blocks.draw_mpl import (  # noqa: E402
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_au.sector_blocks.layout import get_layout  # noqa: E402
from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402


# =============================================================================
# Debug env switches (JP-style)
# =============================================================================
_DEBUG_ENV_KEYS = (
    "OVERVIEW_DEBUG",
    "OVERVIEW_DEBUG_FOOTER",
    "OVERVIEW_DEBUG_FONTS",
)


def _enable_debug_env() -> None:
    # default ON (JP-style), can be disabled by --no-debug
    for k in _DEBUG_ENV_KEYS:
        os.environ.setdefault(k, "1")


def _disable_debug_env() -> None:
    # force OFF
    for k in _DEBUG_ENV_KEYS:
        os.environ[k] = "0"


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
    AU snapshot fallback order (open movers market):
    prefer snapshot_open.
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
    bad = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
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
    AU names sometimes have LTD/PTY LTD.
    """
    s = (name or "").strip()
    if not s:
        return s
    tails = [
        " PTY LTD", " Pty Ltd",
        " PTY", " Pty",
        " LIMITED", " Limited",
        " LTD", " Ltd",
        " GROUP", " Group",
    ]
    for t in tails:
        if s.endswith(t):
            s = s[: -len(t)].rstrip()
            break
    return _ellipsize(s, max_len=max_len)


def prev_text(streak_prev: int, ret_th: float) -> str:
    if streak_prev and streak_prev > 0:
        return f"Prev >= {ret_th*100:.0f}% for {streak_prev} day(s)"
    return f"Prev < {ret_th*100:.0f}%"


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
# Payload ymd/slot helpers (JP-style)
# =============================================================================
def _payload_ymd(payload: Dict[str, Any]) -> str:
    """
    Prefer effective trading day; allow ymd for weekend runs.
    """
    for k in ("ymd_effective", "ymd", "bar_date", "date"):
        v = str(payload.get(k) or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            return v
    for k in ("asof", "slot"):
        v = str(payload.get(k) or "").strip()
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)
    return ""


def _payload_slot(payload: Dict[str, Any]) -> str:
    """
    Prefer payload.slot; fallback infer from asof.
    """
    slot = str(payload.get("slot") or "").strip().lower()
    if slot in {"open", "midday", "close"}:
        return slot
    asof = str(payload.get("asof") or "").strip().lower()
    if "midday" in asof or "noon" in asof:
        return "midday"
    if "close" in asof:
        return "close"
    if "open" in asof:
        return "open"
    return slot or "run"


def _default_outdir(payload: Dict[str, Any], market: str = "au") -> Path:
    """
    Default output dir: media/images/au/{ymd}/{slot}
    """
    ymd = _payload_ymd(payload) or datetime.utcnow().strftime("%Y-%m-%d")
    slot = _payload_slot(payload) or "run"
    return REPO_ROOT / "media" / "images" / market.lower() / ymd / slot


# =============================================================================
# list.txt writer (JP-style) + ordered writer (NEW)
# =============================================================================
def write_list_txt(outdir: Path) -> Path:
    """
    Original behavior:
      1) overview* first
      2) others by filename asc
    """
    outdir = Path(outdir)
    pngs = [p for p in outdir.glob("*.png") if p.is_file()]
    if not pngs:
        p = outdir / "list.txt"
        p.write_text("", encoding="utf-8")
        return p

    def _key(p: Path) -> Tuple[int, str]:
        name = p.name.lower()
        is_overview = 0 if "overview" in name else 1
        return (is_overview, p.name)

    pngs_sorted = sorted(pngs, key=_key)

    lines = [p.name for p in pngs_sorted]
    list_path = outdir / "list.txt"
    list_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return list_path


def write_list_txt_ordered(outdir: Path, ordered_paths: List[Path]) -> Path:
    """
    ✅ NEW: write list.txt in the exact order you want
    (overview first + sector pages by overview sector order)
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

    list_path = Path(outdir) / "list.txt"
    list_path.write_text("\n".join([p.name for p in final]) + ("\n" if final else ""), encoding="utf-8")
    return list_path


# =============================================================================
# Builders (AU open movers style) - same logic as US
# =============================================================================
def build_bigmove_by_sector(universe: List[Dict[str, Any]], ret_th: float):
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

        sector = str(r.get("sector") or "Unknown").strip() or "Unknown"
        sym = str(r.get("symbol") or "").strip()

        raw_name = str(r.get("name") or r.get("company_name") or sym).strip() or sym
        name = short_company_name(raw_name, max_len=28)

        streak_prev = _int(r.get("streak_prev", 0))
        ptxt = prev_text(streak_prev, ret_th)

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
                line2=prev_text(streak_prev, ret_th),
                streak_prev=streak_prev,
            )
        )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0)), reverse=True)

    return out


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
# Main (JP-style UX)
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--payload", required=True)

    # ✅ JP-style: outdir optional; default media/images/au/{ymd}/{slot}
    ap.add_argument("--outdir", default=None)

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="au")

    ap.add_argument("--rows-per-box", type=int, default=6)
    ap.add_argument("--ret-th", type=float, default=0.10)

    # Overview
    ap.add_argument("--no-overview", action="store_true")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)

    # ✅ JP-style: allow one-switch disable debug env
    ap.add_argument("--no-debug", action="store_true", help="Disable overview/footer/font debug env")

    # keep quiet flag for logs (not related to Drive anymore)
    ap.add_argument("--quiet", action="store_true", help="Less logs")

    args = ap.parse_args()

    # Debug env handling
    if args.no_debug:
        _disable_debug_env()
    else:
        _enable_debug_env()

    payload = load_payload(args.payload)
    universe = pick_universe(payload)
    if not universe:
        raise RuntimeError("No usable snapshot in payload")

    # Resolve outdir (JP-style default)
    outdir = Path(args.outdir) if args.outdir else _default_outdir(payload, market="au")
    outdir.mkdir(parents=True, exist_ok=True)

    layout = get_layout(args.layout)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    if not args.quiet:
        ymd = _payload_ymd(payload) or "unknown"
        slot = _payload_slot(payload) or "unknown"
        print(f"[AU] payload={args.payload}")
        print(f"[AU] ymd={ymd} slot={slot} outdir={outdir}")
        print(f"[AU] universe={len(universe)}")
        print(
            "[AU] debug="
            f"footer={os.getenv('OVERVIEW_DEBUG_FOOTER','0')} "
            f"fonts={os.getenv('OVERVIEW_DEBUG_FONTS','0')}"
        )

    # We'll build an explicit render order list for list.txt
    ordered_for_list: List[Path] = []

    # 1) Overview (first) + capture sector order
    overview_order_keys: List[str] = []
    if not args.no_overview:
        payload.setdefault("market", "AU")
        payload.setdefault("asof", payload.get("asof") or payload.get("slot") or "")

        try:
            overview_paths = render_overview_png(
                payload=payload,
                out_dir=outdir,
                width=1080,
                height=1920,
                page_size=int(args.overview_page_size),
                metric=str(args.overview_metric),
            ) or []

            # overview images should come first in list.txt
            ordered_for_list.extend([Path(p) for p in overview_paths if Path(p).is_file()])

            # debug
            print(
                "[AU][DEBUG] raw _overview_sector_order exists?:",
                isinstance(payload.get("_overview_sector_order"), list),
            )
            print("[AU][DEBUG] raw overview order head:", (payload.get("_overview_sector_order", []) or [])[:20])

            overview_order_keys = _extract_overview_sector_order(payload)
            if overview_order_keys:
                met_eff = str(payload.get("_overview_metric_eff") or "").strip()
                print(
                    f"[AU] overview sector order loaded: n={len(overview_order_keys)}"
                    + (f" metric={met_eff}" if met_eff else "")
                )
                print("[AU] normalized overview order head:", overview_order_keys[:20])
            else:
                print("[AU][WARN] overview order empty after normalization", flush=True)
        except Exception as e:
            print(f"[AU][WARN] overview skipped due to render error: {e}", flush=True)

    # 2) Sector pages
    movers = build_bigmove_by_sector(universe, float(args.ret_th))
    peers = build_peers_by_sector(universe, float(args.ret_th))

    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1

    # =============================================================================
    # ✅ NEW: reorder sectors by overview order (then append remaining sectors)
    # =============================================================================
    sectors_movers = list((movers or {}).keys())

    # Map normalized key -> original sector name(s)
    key_to_sector: Dict[str, str] = {}
    for sec in sectors_movers:
        k = normalize_sector_key(sec)
        # first wins (stable)
        if k and k not in key_to_sector:
            key_to_sector[k] = sec

    ordered_sectors: List[str] = []
    if overview_order_keys:
        # add sectors that appear in overview order
        for k in overview_order_keys:
            sec = key_to_sector.get(k)
            if sec and sec in (movers or {}):
                ordered_sectors.append(sec)

        # append remaining sectors (keep original insertion order from dict)
        seen = set(normalize_sector_key(s) for s in ordered_sectors)
        for sec in sectors_movers:
            k = normalize_sector_key(sec)
            if k not in seen:
                ordered_sectors.append(sec)
                seen.add(k)
    else:
        ordered_sectors = sectors_movers  # fallback

    for sector in ordered_sectors:
        L_total = (movers or {}).get(sector, []) or []
        P_all = peers.get(sector, []) or []

        big_total = len([x for x in L_total if not x.get("touched_only")])
        touch_total = len([x for x in L_total if x.get("touched_only")])

        top_pages = chunk(L_total, rows_top)
        peer_pages_all = chunk(P_all, rows_peer)

        # Keep one extra page for peers after last mover page
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

            fname = f"au_{safe_sector}_p{i+1}.png"
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

            # ✅ record sector pages in the EXACT sequence we rendered
            if out_path.is_file():
                ordered_for_list.append(out_path)

    # 3) list.txt
    if overview_order_keys:
        list_path = write_list_txt_ordered(outdir, ordered_for_list)
    else:
        list_path = write_list_txt(outdir)

    if not args.quiet:
        print(f"📝 Wrote list: {list_path}")
        print("\n✅ AU render finished. (Drive upload removed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
