# scripts/render_images_ca/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    """
    Best-effort YYYY-MM-DD for folder naming.
    Priority:
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


# =============================================================================
# Outdir + list.txt (JP-style)
# =============================================================================
def default_outdir_from_payload(payload: Dict[str, Any], market: str) -> Path:
    """
    Default output path:
      media/images/ca/{ymd}/{slot}
    If ymd missing -> media/images/ca/unknown/{slot}
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
# Drive subfolder helpers (use payload helpers)
# =============================================================================
def _infer_run_tag(payload: Dict[str, Any]) -> str:
    return _payload_slot(payload) or "run"


def make_drive_subfolder_name(payload: Dict[str, Any], market: str) -> str:
    """
    Default folder: CA_2026-02-15_close (keep ymd for weekend runs)
    Fallback: CA_20260215_054033
    """
    ymd = _payload_ymd(payload)
    tag = _infer_run_tag(payload)

    if ymd:
        return f"{market}_{ymd}_{tag}"

    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{market}_{now}"


# =============================================================================
# Builders (CA open_limit style, US-compatible semantics)
# =============================================================================
def build_limitup_by_sector(universe: List[Dict[str, Any]], ret_th: float):
    """
    Top box:
    - big: close ret >= ret_th
    - touch: touched_only=True but close < ret_th
    """
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
                # optional hints for renderer / future
                ret_th=float(ret_th),
                touch_th=float(ret_th),
                touch_tag=f"Touched {th_pct_int}%",
            )
        )

    # Sort: big first (by ret), then touch (by touch_ret)
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

    # ✅ outdir optional (JP-style)
    ap.add_argument("--outdir", default=None, help="Output dir. If omitted, auto: media/images/ca/{ymd}/{slot}")

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="ca")

    ap.add_argument("--rows-per-box", type=int, default=6)
    ap.add_argument("--ret-th", type=float, default=0.10)

    # Overview
    ap.add_argument("--no-overview", action="store_true")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)

    # ✅ Debug default ON (user can disable)
    ap.add_argument("--no-debug", action="store_true", help="Disable overview/footer/font debug env (default: ON)")

    # ✅ Drive upload DEFAULT ON
    ap.add_argument("--no-upload-drive", action="store_true", help="Do not upload to Drive after rendering")

    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default=MARKET)

    ap.add_argument("--drive-client-secret", default=None)
    ap.add_argument("--drive-token", default=None)

    # Subfolder strategy
    ap.add_argument("--drive-subfolder", default=None)
    ap.add_argument("--drive-subfolder-auto", action="store_true", default=True)

    # Upload tuning
    ap.add_argument("--drive-workers", type=int, default=8)
    ap.add_argument("--drive-no-concurrent", action="store_true")
    ap.add_argument("--drive-no-overwrite", action="store_true")
    ap.add_argument("--drive-quiet", action="store_true")

    # ✅ safety guard
    ap.add_argument(
        "--allow-market-mismatch",
        action="store_true",
        help="ALLOW uploading into a different drive-market (dangerous; disables safety guard)",
    )

    args = ap.parse_args()

    # ✅ debug default ON
    debug_on = (not bool(args.no_debug))
    if debug_on:
        os.environ.setdefault("OVERVIEW_DEBUG", "1")
        os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
        os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")

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

            fname = f"ca_{safe_filename(sector)}_p{i+1}.png"

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

    print("\n✅ CA render finished.")

    # -------------------------------------------------------------------------
    # Drive upload (DEFAULT ON) + SAFETY GUARD
    # -------------------------------------------------------------------------
    if not args.no_upload_drive:
        market_name = str(args.drive_market or MARKET).strip().upper()

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
            subfolder: Optional[str] = str(args.drive_subfolder).strip()
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