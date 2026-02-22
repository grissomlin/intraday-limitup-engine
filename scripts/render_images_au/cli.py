# scripts/render_images_au/cli.py
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
from typing import Any, Dict, List, Optional, Tuple

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_au.sector_blocks.draw_mpl import (
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_au.sector_blocks.layout import get_layout
from scripts.render_images_common.overview_mpl import render_overview_png

# ✅ Drive uploader (env-first / b64 supported by drive_uploader)
from scripts.utils.drive_uploader import (
    get_drive_service,
    ensure_folder,
    upload_dir,
)

DEFAULT_ROOT_FOLDER = (
    os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip()
    or "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"
)

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
    # sometimes asof contains date
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
# list.txt writer (JP-style)
# =============================================================================
def write_list_txt(outdir: Path) -> Path:
    """
    Write list.txt for video stitching.
    Sorting rule:
      1) overview* first (any filename containing 'overview')
      2) then others by filename asc
    Each line is the filename (relative, no quotes).
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


# =============================================================================
# Drive subfolder helpers (keep)
# =============================================================================
def _first_ymd(payload: Dict[str, Any]) -> Optional[str]:
    """
    Prefer ymd for weekend runs; allow ymd_effective.
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


def _infer_run_tag(payload: Dict[str, Any]) -> str:
    s = str(payload.get("slot") or payload.get("asof") or "").lower()

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
    """
    Default folder: AU_2026-02-15_close
    Fallback: AU_20260215_121530
    """
    ymd = _first_ymd(payload)
    tag = _infer_run_tag(payload)

    if ymd:
        return f"{market}_{ymd}_{tag}"

    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{market}_{now}"


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

    # ✅ JP-style: Drive upload default ON
    ap.set_defaults(upload_drive=True)
    ap.add_argument("--upload-drive", dest="upload_drive", action="store_true", help="Enable Drive upload (default on)")
    ap.add_argument("--no-upload-drive", dest="upload_drive", action="store_false", help="Disable Drive upload")

    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default="AU")

    ap.add_argument("--drive-client-secret", default=None)
    ap.add_argument("--drive-token", default=None)

    # Subfolder strategy (default ON recommended)
    ap.set_defaults(drive_subfolder_auto=True)
    ap.add_argument("--drive-subfolder", default=None)
    ap.add_argument("--drive-subfolder-auto", dest="drive_subfolder_auto", action="store_true")
    ap.add_argument("--no-drive-subfolder-auto", dest="drive_subfolder_auto", action="store_false")

    # Upload tuning
    ap.add_argument("--drive-workers", type=int, default=8)
    ap.add_argument("--drive-no-concurrent", action="store_true")
    ap.add_argument("--drive-no-overwrite", action="store_true")
    ap.add_argument("--drive-quiet", action="store_true")

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

    # 1) Overview (first)
    if not args.no_overview:
        payload.setdefault("market", "AU")
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
    movers = build_bigmove_by_sector(universe, args.ret_th)
    peers = build_peers_by_sector(universe, args.ret_th)

    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1

    for sector, L_total in movers.items():
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
            mover_rows = top_pages[i] if i < len(top_pages) else []
            peer_rows = peer_pages[i] if i < len(peer_pages) else []

            shown_n = min(len(L_total), (i + 1) * rows_top)
            shown_slice = L_total[:shown_n]

            big_shown = sum(1 for x in shown_slice if not x.get("touched_only"))
            touch_shown = sum(1 for x in shown_slice if x.get("touched_only"))

            has_more_peers = (i == total_pages - 1) and (len(peer_pages_all) > len(peer_pages))

            fname = f"au_{safe_filename(sector)}_p{i+1}.png"

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

    # ✅ list.txt (JP-style)
    list_path = write_list_txt(outdir)
    if not args.drive_quiet:
        print(f"📝 Wrote list: {list_path}")

    # 3) Drive upload (default ON)
    if args.upload_drive:
        if not args.drive_quiet:
            print("\n🚀 Uploading PNGs to Google Drive...")

        svc = get_drive_service(
            client_secret_file=args.drive_client_secret,
            token_file=args.drive_token,
        )

        root_id = str(args.drive_root_folder_id).strip()
        market_name = str(args.drive_market or "AU").strip().upper()

        market_folder_id = ensure_folder(svc, root_id, market_name)

        subfolder: Optional[str] = None
        if args.drive_subfolder:
            subfolder = str(args.drive_subfolder).strip()
        elif args.drive_subfolder_auto:
            subfolder = make_drive_subfolder_name(payload, market=market_name)

        if not args.drive_quiet:
            if subfolder:
                print(f"📁 Target Drive folder: root/{market_name}/{subfolder}/")
            else:
                print(f"📁 Target Drive folder: root/{market_name}/ (no subfolder)")

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

        if not args.drive_quiet:
            print(f"✅ Uploaded {uploaded} png(s)")

    if not args.drive_quiet:
        print("\n✅ AU render finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())