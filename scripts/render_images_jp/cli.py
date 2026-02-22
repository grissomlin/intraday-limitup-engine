# scripts/render_images_jp/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

# ✅ VERY IMPORTANT: Force matplotlib headless backend (avoid tkinter warnings)
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# ✅ DEFAULT DEBUG ON (overview + footer)
# - OVERVIEW_DEBUG_FOOTER: 印 footer 佈局/文字座標/行內容等
# - OVERVIEW_DEBUG_FONTS : 印 i18n_font debug（包含 rcParams['font.sans-serif'] order）
# - OVERVIEW_DEBUG        : 若 repo 內還有總開關也順便開
# =============================================================================
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_jp.sector_blocks.draw_mpl import (
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_jp.sector_blocks.layout import get_layout

from scripts.render_images_common.overview_mpl import render_overview_png

# ✅ Drive uploader (env-first / b64 supported by drive_uploader)  —— US規格
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


def _sanitize_filename(s: str) -> str:
    s = _safe_str(s)
    if not s:
        return "unknown"
    s = s.replace(" ", "_")
    s = re.sub(r"[^\w\u4e00-\u9fff\u3040-\u30ff\u31f0-\u31ff\u3400-\u4dbf\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def _normalize_market(m: str) -> str:
    m = (m or "").strip().upper()
    alias = {
        "JPX": "JP",
        "JPN": "JP",
        "JAPAN": "JP",
        "TSE": "JP",
        "TOSE": "JP",
        "TOKYO": "JP",
    }
    return alias.get(m, m or "JP")


def _market_from_payload(payload: Dict[str, Any]) -> str:
    m = _safe_str(payload.get("market") or "JP")
    return _normalize_market(m)


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i: i + n] for i in range(0, len(lst), n)]


# =============================================================================
# New: list.txt generator (unified)
# =============================================================================
def write_list_txt(
    outdir: Path,
    *,
    ext: str = "png",
    overview_prefix: str = "overview_sectors_",
    filename: str = "list.txt",
) -> Path:
    """
    Generate outdir/list.txt for render_video.py (concat order source).

    Order:
      1) overview pages: {overview_prefix}*_p*.{ext} (sorted)
         fallback: {overview_prefix}*.{ext} (sorted)
      2) other images in outdir: *.{ext} excluding those starting with overview_prefix (sorted)

    Writes RELATIVE paths (relative to outdir), one per line.
    """
    outdir = outdir.resolve()
    ext = (ext or "png").lstrip(".")
    overview_prefix = str(overview_prefix or "").strip() or "overview_sectors_"

    items: List[Path] = []

    # 1) overview paged
    paged = sorted(outdir.glob(f"{overview_prefix}*_p*.{ext}"), key=lambda p: p.name)
    if paged:
        items.extend(paged)
    else:
        single_or_any = sorted(outdir.glob(f"{overview_prefix}*.{ext}"), key=lambda p: p.name)
        items.extend(single_or_any)

    # 2) others (exclude overview_prefix)
    others = sorted(outdir.glob(f"*.{ext}"), key=lambda p: p.name)
    others = [p for p in others if not p.name.startswith(overview_prefix)]
    items.extend(others)

    # dedupe just in case
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
# Yesterday text (JP)
# =============================================================================
def yesterday_text_jp(streak_prev: int, prev_locked: bool, prev_touch: bool) -> str:
    if streak_prev and streak_prev > 0 and prev_locked:
        return f"前日: S高張り付き {streak_prev}連"
    if prev_touch:
        return "前日: 一時S高"
    return "前日: S高なし"


# =============================================================================
# Builders (JP mix)
# =============================================================================
def build_limitup_by_sector_jp(universe: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Top box (mix):
      - true limitup (locked): ストップ高（張り付き）
      - touched only: 一時ストップ高
      - surge >=10% but not true limitup: 急騰/急伸/上昇（from payload tag）
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        is_touch = _bool(r.get("is_limitup_touch", False))
        is_locked = _bool(r.get("is_limitup_locked", False))
        is_true = _bool(r.get("is_true_limitup", False))  # locked
        is_surge = _bool(r.get("is_surge_ge10", False))
        is_display = _bool(r.get("is_display_limitup", False))

        if not (is_display or is_touch or is_locked or is_surge):
            continue

        touch_only = bool(is_touch and (not is_true))
        surge_only = bool(is_surge and (not is_true) and (not is_touch))

        sector = _safe_str(r.get("sector") or "未分類")
        if sector.strip() in ("", "A-Share", "—", "-", "--", "－", "–"):
            sector = "未分類"

        sym = _safe_str(r.get("symbol") or "")
        name = _safe_str(r.get("name") or sym)

        ret = _pct(r.get("ret", 0.0))
        streak_raw = _int(r.get("streak", 0))
        streak_prev = _int(r.get("streak_prev", 0))

        prev_locked = _bool(r.get("prev_is_limitup_locked", False))
        prev_touch = _bool(r.get("prev_is_limitup_touch", False))

        if is_true:
            if prev_locked:
                streak_display = max(1, streak_raw if streak_raw > 0 else 1)
            else:
                streak_display = 1
        else:
            streak_display = 0

        ytxt = yesterday_text_jp(streak_prev, prev_locked, prev_touch)
        line1 = f"{sym}  {name}"

        if is_true:
            badge = "ストップ高"
            if streak_display > 1 and prev_locked:
                line2 = f"S高張り付き ｜ {streak_display}連 ｜ {ytxt}"
            else:
                line2 = f"S高張り付き ｜ {ytxt}"
            status = "hit"
        elif touch_only:
            badge = "一時S高"
            line2 = f"S高到達後に反落 ｜ {ytxt}"
            status = "touch"
        elif surge_only:
            badge = _safe_str(r.get("tag") or "急騰")
            line2 = f"10%+ 上昇 ｜ {ytxt}"
            status = "surge"
        else:
            badge = _safe_str(r.get("tag") or "上昇")
            line2 = ytxt
            status = "surge"

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "badge_text": badge,
                "streak": streak_display,
                "streak_prev": streak_prev,
                "prev_is_limitup_locked": prev_locked,
                "prev_is_limitup_touch": prev_touch,
                "line1": line1,
                "line2": line2,
                "market_detail": _safe_str(r.get("market_detail") or ""),
                "limitup_status": status,  # hit / touch / surge
                "is_true_limitup": bool(is_true),
                "is_surge_ge10": bool(is_surge),
            }
        )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0) or 0.0), reverse=True)

    return out


def build_peers_by_sector_jp(universe: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Bottom box: same-sector peers that are NOT in top-box display set
    (exclude true limitup / touch / surge>=10 / is_display_limitup)
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        is_touch = _bool(r.get("is_limitup_touch", False))
        is_true = _bool(r.get("is_true_limitup", False))
        is_surge = _bool(r.get("is_surge_ge10", False))
        is_display = _bool(r.get("is_display_limitup", False))

        if is_true or is_touch or is_surge or is_display:
            continue

        sector = _safe_str(r.get("sector") or "未分類")
        if sector.strip() in ("", "A-Share", "—", "-", "--", "－", "–"):
            sector = "未分類"

        sym = _safe_str(r.get("symbol") or "")
        name = _safe_str(r.get("name") or sym)
        ret = _pct(r.get("ret", 0.0))

        streak_prev = _int(r.get("streak_prev", 0))
        prev_locked = _bool(r.get("prev_is_limitup_locked", False))
        prev_touch = _bool(r.get("prev_is_limitup_touch", False))

        line2 = yesterday_text_jp(streak_prev, prev_locked, prev_touch)
        line1 = f"{sym}  {name}"

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "streak_prev": streak_prev,
                "prev_is_limitup_locked": prev_locked,
                "prev_is_limitup_touch": prev_touch,
                "line1": line1,
                "line2": line2,
                "market_detail": _safe_str(r.get("market_detail") or ""),
            }
        )

    def _peer_sort_key(x: Dict[str, Any]) -> Tuple[int, int, int, float]:
        sp = int(x.get("streak_prev", 0) or 0)
        pl = 1 if bool(x.get("prev_is_limitup_locked", False)) else 0
        pt = 1 if bool(x.get("prev_is_limitup_touch", False)) else 0
        rr = float(x.get("ret", 0.0) or 0.0)
        return (sp, pl, pt, rr)

    for k in out:
        out[k].sort(key=_peer_sort_key, reverse=True)

    return out


def count_hit_touch_surge(rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    hit = 0
    touch = 0
    surge = 0
    for r in rows:
        s = _safe_str(r.get("limitup_status") or "").lower()
        if s == "touch":
            touch += 1
        elif s == "surge":
            surge += 1
        else:
            hit += 1
    return hit, touch, surge


# =============================================================================
# Drive subfolder helpers (US規格)
# =============================================================================
def _first_ymd(payload: Dict[str, Any]) -> Optional[str]:
    for k in ("bar_date", "ymd", "ymd_effective", "date"):
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


def make_drive_subfolder_name(payload: Dict[str, Any], market: str) -> str:
    ymd = _first_ymd(payload)
    tag = _infer_run_tag(payload)
    if ymd:
        return f"{market}_{ymd}_{tag}"
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{market}_{now}"


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--payload", required=True)
    ap.add_argument("--outdir", default=None)

    ap.add_argument("--theme", default="dark")
    ap.add_argument("--layout", default="jp")
    ap.add_argument("--rows-per-box", type=int, default=6)
    ap.add_argument("--max-sectors", type=int, default=20)
    ap.add_argument("--cap-pages", type=int, default=5)

    # Overview
    ap.add_argument("--no-overview", action="store_true", help="overview を出力しない")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)

    # ✅ DEBUG: default ON, allow opt-out
    ap.add_argument("--no-debug", action="store_true", help="overview/footer debug を無効化")

    # ✅ Upload is DEFAULT ON (統一規格)
    ap.add_argument("--no-upload-drive", action="store_true", help="生成後不上傳 Drive")

    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default="JP")
    ap.add_argument("--drive-client-secret", default=None)
    ap.add_argument("--drive-token", default=None)

    # ✅ Subfolder default AUTO ON (recommended)
    ap.add_argument("--drive-subfolder", default=None)
    ap.add_argument("--drive-subfolder-auto", action="store_true", default=True)

    # ✅ Upload tuning: faster by default
    ap.add_argument("--drive-workers", type=int, default=16)
    ap.add_argument("--drive-no-concurrent", action="store_true")
    ap.add_argument("--drive-no-overwrite", action="store_true")
    ap.add_argument("--drive-quiet", action="store_true")

    args = ap.parse_args()

    # If user explicitly disables debug, override envs to 0
    if args.no_debug:
        os.environ["OVERVIEW_DEBUG_FOOTER"] = "0"
        os.environ["OVERVIEW_DEBUG_FONTS"] = "0"
        os.environ["OVERVIEW_DEBUG"] = "0"

    payload = load_payload(args.payload)
    universe = pick_universe(payload)
    if not universe:
        raise RuntimeError("Payload に snapshot が見つかりません")

    ymd = _payload_ymd(payload) or "unknown"
    slot = _payload_slot(payload)

    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = REPO_ROOT / "media" / "images" / "jp" / ymd / slot
    outdir.mkdir(parents=True, exist_ok=True)

    market = _market_from_payload(payload)
    layout = get_layout(args.layout)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    width, height = 1080, 1920
    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1
    CAP_PAGES = max(1, int(args.cap_pages))

    print(f"[JP] payload={args.payload}")
    print(f"[JP] ymd={ymd} slot={slot} outdir={outdir}")
    print(f"[JP] universe={len(universe)}")
    print(
        "[JP] debug="
        f"footer={os.getenv('OVERVIEW_DEBUG_FOOTER','0')} "
        f"fonts={os.getenv('OVERVIEW_DEBUG_FONTS','0')}"
    )

    # -------------------------------------------------------------------------
    # 0) Overview first
    # -------------------------------------------------------------------------
    if not args.no_overview:
        try:
            payload_for_overview = dict(payload)
            payload_for_overview["market"] = market
            payload_for_overview.setdefault(
                "asof",
                payload_for_overview.get("asof")
                or payload_for_overview.get("slot")
                or "",
            )

            overview_paths = render_overview_png(
                payload_for_overview,
                outdir,
                width=width,
                height=height,
                page_size=int(args.overview_page_size),
                metric=str(args.overview_metric or "auto"),
            )
            for p in overview_paths:
                print(f"[JP] wrote {p}")
        except Exception as e:
            print(f"[JP] overview failed: {e}")

    # -------------------------------------------------------------------------
    # 1) Sector pages (mix top + peers bottom)
    # -------------------------------------------------------------------------
    top_rows = build_limitup_by_sector_jp(universe)
    peers = build_peers_by_sector_jp(universe)
    print(f"[JP] sectors(top)={len(top_rows)} sectors(peers)={len(peers)}")

    if top_rows:
        sector_keys = list(top_rows.keys())
    else:

        def _sec_key(sec: str) -> float:
            rr = peers.get(sec, [])
            if not rr:
                return -1e9
            return max(float(x.get("ret", 0.0) or 0.0) for x in rr)

        sector_keys = sorted(peers.keys(), key=_sec_key, reverse=True)[
            : max(1, int(args.max_sectors))
        ]
        print(f"[JP] fallback: top rows empty; use peers top {len(sector_keys)} sectors")

    for sector in sector_keys:
        L_total = top_rows.get(sector, [])
        P_all = peers.get(sector, [])

        max_top_show = CAP_PAGES * rows_top
        L_show = L_total[:max_top_show]

        L_pages = chunk(L_show, rows_top) if L_show else [[]]
        P_pages_all = chunk(P_all, rows_peer)

        top_pages = len(L_pages)
        peer_pages = len(P_pages_all)

        total_pages = top_pages
        if peer_pages > top_pages:
            total_pages = top_pages + 1
        if total_pages > CAP_PAGES:
            total_pages = CAP_PAGES

        hit_total, touch_total, surge_total = count_hit_touch_surge(L_total)
        hit_shown, touch_shown, surge_shown = count_hit_touch_surge(L_show)

        # ✅ 業種内%（產業內強勢占比）：Top / (Top + Peers)
        denom_sector = max(1, (len(L_total) + len(P_all)))
        sector_share = float(len(L_total)) / float(denom_sector)

        sector_fn = _sanitize_filename(sector)

        for i in range(total_pages):
            limitup_rows = L_pages[i] if i < len(L_pages) else []
            peer_rows = P_pages_all[i] if i < len(P_pages_all) else []

            has_more_peers = (peer_pages > total_pages) and (i == total_pages - 1)

            out_path = outdir / f"jp_{sector_fn}_p{i+1}.png"

            draw_block_table(
                out_path=out_path,
                layout=layout,
                sector=sector,
                cutoff=cutoff,
                locked_cnt=hit_total,  # legacy name
                touch_cnt=touch_total,  # legacy name
                theme_cnt=surge_total,  # legacy name
                hit_shown=hit_shown,
                hit_total=hit_total,
                touch_shown=touch_shown,
                touch_total=touch_total,
                surge_shown=surge_shown,
                surge_total=surge_total,
                sector_share=sector_share,  # ✅ FIXED
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
            )
            print(f"[JP] wrote {out_path}")

    # -------------------------------------------------------------------------
    # 1.5) Write list.txt (unified)  ✅ NEW
    # -------------------------------------------------------------------------
    try:
        list_path = write_list_txt(
            outdir,
            ext="png",
            overview_prefix="overview_sectors_",
            filename="list.txt",
        )
        print(f"[JP] wrote {list_path}")
    except Exception as e:
        print(f"[JP] list.txt generation failed (continue): {e}")

    # -------------------------------------------------------------------------
    # 2) Drive upload (DEFAULT ON) —— US規格
    # -------------------------------------------------------------------------
    if not args.no_upload_drive:
        print("\n🚀 Uploading PNGs to Google Drive...")

        svc = get_drive_service(
            client_secret_file=args.drive_client_secret,
            token_file=args.drive_token,
        )

        root_id = str(args.drive_root_folder_id).strip()
        market_name = str(args.drive_market or "JP").strip().upper()

        market_folder_id = ensure_folder(svc, root_id, market_name)

        subfolder: Optional[str] = None
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

    print("\n✅ JP render finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())