# scripts/render_images_cn/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ✅ VERY IMPORTANT: Force matplotlib headless backend (avoid tkinter warnings)
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# ✅ DEFAULT DEBUG ON (overview + footer)  — align JP/KR/TW
# - OVERVIEW_DEBUG_FOOTER: 印 footer 佈局/文字座標/行內容等
# - OVERVIEW_DEBUG_FONTS : 印 i18n_font debug（包含 rcParams['font.sans-serif'] order）
# - OVERVIEW_DEBUG        : 若 repo 內還有總開關也順便開
# =============================================================================
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

# ✅ Optional: hint common overview to disable gainbins if it supports env flags
# (Even if not supported, we will delete gainbins PNGs after rendering.)
os.environ.setdefault("OVERVIEW_GAINBINS", "0")
os.environ.setdefault("OVERVIEW_ENABLE_GAINBINS", "0")
os.environ.setdefault("OVERVIEW_DISABLE_GAINBINS", "1")
os.environ.setdefault("OVERVIEW_NO_GAINBINS", "1")

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# CN sector pages
from scripts.render_images_cn.sector_blocks.draw_mpl import draw_block_table
from scripts.render_images_cn.sector_blocks.layout import get_layout

# ✅ common header/time helper
from scripts.render_images_common.header_mpl import get_market_time_info

# overview (common)
from scripts.render_images_common.overview_mpl import render_overview_png

# aggregator (no re-download, just compute from payload snapshot_main)
from markets.cn.aggregator import aggregate as cn_aggregate

# ✅ Drive uploader (env-first / b64 supported by drive_uploader) —— US規格
from scripts.utils.drive_uploader import (
    ensure_folder,
    get_drive_service,
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


def _payload_cutoff_str(payload: Dict[str, Any]) -> str:
    """
    IMPORTANT:
    draw_mpl.parse_cutoff expects a string, not the whole payload dict.
    """
    return _safe_str(payload.get("cutoff") or payload.get("asof") or payload.get("slot") or "close")


def _sanitize_filename(s: str) -> str:
    """
    Make a safe filename fragment.
    Keep CJK, letters, numbers, underscore, dash.
    Replace other chars with underscore.
    """
    s = _safe_str(s)
    if not s:
        return "unknown"
    s = s.replace(" ", "_")
    s = re.sub(r"[^\w\u4e00-\u9fff\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def _norm_sector_name(s: str) -> str:
    ss = _safe_str(s or "")
    if ss.strip() in ("", "A-Share", "—", "-", "--", "－", "–"):
        return "未分类"
    return ss


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i : i + n] for i in range(0, len(lst), n)]


# =============================================================================
# ✅ NEW: CN time_note builder (one-line)
# 中国交易日YYYY-MM-DD  更新 YYYY-MM-DD HH:MM
# - prefer meta.time.market_finished_at + market_finished_hm
# - fallback to get_market_time_info() hhmm
# =============================================================================
def _split_ymd_from_dt(s: str) -> str:
    s = _safe_str(s)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return ""


def build_cn_time_note(payload: Dict[str, Any]) -> str:
    trade_ymd = _payload_ymd(payload)

    meta = payload.get("meta") or {}
    tmeta = meta.get("time") or {}
    if not isinstance(tmeta, dict):
        tmeta = {}

    hm = _safe_str(tmeta.get("market_finished_hm") or "")
    finished_at = _safe_str(tmeta.get("market_finished_at") or "")
    update_ymd = _split_ymd_from_dt(finished_at) or trade_ymd

    # fallback if no hm in meta.time
    if not hm:
        _, _, _, hhmm = get_market_time_info(payload, market="CN")
        hm = _safe_str(hhmm)

    if hm:
        return f"中国交易日{trade_ymd}  更新 {update_ymd} {hm}"
    return f"中国交易日{trade_ymd}"


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
# Big move 10%+ (CN: only for 20% boards - ChiNext/STAR)
# =============================================================================
def _is_20_board_cn(r: Dict[str, Any]) -> bool:
    sym = _safe_str(r.get("symbol") or "").upper()
    md = _safe_str(r.get("market_detail") or "").lower()
    try:
        lr = float(r.get("limit_rate", 0.0) or 0.0)
    except Exception:
        lr = 0.0

    code = sym.split(".", 1)[0]
    if md in ("chinext", "star"):
        return True
    if code.startswith(("300", "301", "688", "689")):
        return True
    return abs(lr - 0.20) < 1e-9


def _touch_any_cn(r: Dict[str, Any]) -> bool:
    """
    snapshot_builder: is_limitup_touch = touch_any (含封板)
    aggregator: 可能會另外塞 is_limitup_touch_any
    """
    if "is_limitup_touch_any" in r:
        return _bool(r.get("is_limitup_touch_any", False))
    return _bool(r.get("is_limitup_touch", False))


def _bombed_cn(r: Dict[str, Any]) -> bool:
    """
    炸板（触及未封）優先用 snapshot_builder 產出的 touched_only，
    若沒有再用 touch_any & ~locked 推。
    """
    if "touched_only" in r:
        return _bool(r.get("touched_only", False))
    return _touch_any_cn(r) and (not _bool(r.get("is_limitup_locked", False)))


# =============================================================================
# Board tag (CN)
# =============================================================================
def board_tag_cn(r: Dict[str, Any]) -> str:
    """
    回傳：创 / 科 / 北 / 主 / 特
    """
    sym = _safe_str(r.get("symbol") or "").upper()
    md = _safe_str(r.get("market_detail") or "").lower()
    name = _safe_str(r.get("name") or "")

    if "ST" in name.upper():
        return "特"
    if _safe_str(r.get("market_tag") or "").upper() == "ST":
        return "特"

    if sym.endswith(".BJ"):
        return "北"

    if md == "chinext" or (sym.endswith(".SZ") and (sym.startswith("300") or sym.startswith("301"))):
        return "创"

    if md == "star" or (sym.endswith(".SS") and (sym.startswith("688") or sym.startswith("689"))):
        return "科"

    return "主"


def prevday_text_cn(streak_prev: int, prev_locked: bool, prev_touch: bool) -> str:
    if streak_prev and streak_prev > 0 and prev_locked:
        return f"前日涨停 {streak_prev} 连"
    if prev_touch:
        return "前日触及"
    return "前日未涨停"


# =============================================================================
# Builders
# =============================================================================
def build_limitup_by_sector_cn(universe: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        is_locked = _bool(r.get("is_limitup_locked", False))
        touch_any = _touch_any_cn(r)
        bombed = _bombed_cn(r)

        ret = _pct(r.get("ret", 0.0))
        big10 = bool(_is_20_board_cn(r) and (ret >= 0.10) and (not is_locked) and (not touch_any))

        if not (is_locked or bombed or big10):
            continue

        sector = _norm_sector_name(_safe_str(r.get("sector") or "未分类"))

        sym = _safe_str(r.get("symbol") or "")
        name = _safe_str(r.get("name") or sym)

        market_detail = _safe_str(r.get("market_detail") or "")
        try:
            limit_rate = float(r.get("limit_rate", 0.0) or 0.0)
        except Exception:
            limit_rate = 0.0

        streak_raw = _int(r.get("streak", 0))
        streak_prev = _int(r.get("streak_prev", 0))
        prev_locked = _bool(r.get("prev_was_limitup_locked", False))
        prev_touch = _bool(r.get("prev_was_limitup_touch", False))

        # display streak
        if is_locked:
            if prev_locked:
                streak_display = max(1, streak_raw if streak_raw > 0 else 1)
            else:
                streak_display = 1
        else:
            streak_display = 0

        ytxt = prevday_text_cn(streak_prev, prev_locked, prev_touch)
        line1 = f"{sym}  {name}"

        if big10:
            badge = "10%+"
            line2 = f"大涨10%+  {ytxt}"
            status = "big"
        elif bombed:
            badge = "炸板"
            line2 = f"炸板回落  {ytxt}"
            status = "bomb"
        else:
            if is_locked and streak_display > 1:
                badge = f"涨停{streak_display}连"
            else:
                badge = "涨停"

            if streak_display > 1 and prev_locked:
                line2 = f"涨停 {streak_display}连  {ytxt}"
            else:
                line2 = f"涨停  {ytxt}"
            status = "hit"

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
                "prev_was_limitup_locked": prev_locked,
                "prev_was_limitup_touch": prev_touch,
                "line1": line1,
                "line2": line2,
                "market_detail": market_detail,
                "limit_rate": limit_rate,
                "market_tag": board_tag_cn(r),
                "limitup_status": status,  # hit/bomb/big
            }
        )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0) or 0.0), reverse=True)
    return out


def build_peers_by_sector_cn(universe: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        is_locked = _bool(r.get("is_limitup_locked", False))
        touch_any = _touch_any_cn(r)
        bombed = _bombed_cn(r)
        ret = _pct(r.get("ret", 0.0))
        big10 = bool(_is_20_board_cn(r) and (ret >= 0.10) and (not is_locked) and (not touch_any))

        # peers：排除封板/炸板/10%+/以及任何摸過漲停價(touch_any)
        if is_locked or bombed or big10 or touch_any:
            continue

        sector = _norm_sector_name(_safe_str(r.get("sector") or "未分类"))

        sym = _safe_str(r.get("symbol") or "")
        name = _safe_str(r.get("name") or sym)

        market_detail = _safe_str(r.get("market_detail") or "")
        try:
            limit_rate = float(r.get("limit_rate", 0.0) or 0.0)
        except Exception:
            limit_rate = 0.0

        streak_prev = _int(r.get("streak_prev", 0))
        prev_locked = _bool(r.get("prev_was_limitup_locked", False))
        prev_touch = _bool(r.get("prev_was_limitup_touch", False))

        line2 = prevday_text_cn(streak_prev, prev_locked, prev_touch)
        line1 = f"{sym}  {name}"

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "streak_prev": streak_prev,
                "prev_was_limitup_locked": prev_locked,
                "prev_was_limitup_touch": prev_touch,
                "line1": line1,
                "line2": line2,
                "market_detail": market_detail,
                "limit_rate": limit_rate,
                "market_tag": board_tag_cn(r),
            }
        )

    def _peer_sort_key(x: Dict[str, Any]) -> Tuple[int, int, int, float]:
        sp = int(x.get("streak_prev", 0) or 0)
        pl = 1 if bool(x.get("prev_was_limitup_locked", False)) else 0
        pt = 1 if bool(x.get("prev_was_limitup_touch", False)) else 0
        rr = float(x.get("ret", 0.0) or 0.0)
        return (sp, pl, pt, rr)

    for k in out:
        out[k].sort(key=_peer_sort_key, reverse=True)
    return out


def count_hit_bomb_big(rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    hit = 0
    bomb = 0
    big = 0
    for r in rows:
        s = _safe_str(r.get("limitup_status") or "").lower()
        if s in ("bomb", "touch"):
            bomb += 1
        elif s == "big":
            big += 1
        else:
            hit += 1
    return hit, bomb, big


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

    # ✅ DEBUG: default ON, allow opt-out
    ap.add_argument("--no-debug", action="store_true", help="overview/footer debug を無効化")

    # ✅ Upload is DEFAULT ON (統一規格)
    ap.add_argument("--no-upload-drive", action="store_true", help="生成後不上傳 Drive")
    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default="CN")
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
        raise RuntimeError("No usable snapshot in payload")

    ymd = _payload_ymd(payload) or "unknown"
    slot = _payload_slot(payload)

    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = REPO_ROOT / "media" / "images" / "cn" / ymd / slot
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[CN] payload={args.payload}")
    print(f"[CN] ymd={ymd} slot={slot} outdir={outdir}")
    print(
        "[CN] debug="
        f"footer={os.getenv('OVERVIEW_DEBUG_FOOTER','0')} "
        f"fonts={os.getenv('OVERVIEW_DEBUG_FONTS','0')}"
    )

    # aggregate inside CLI (no re-download)
    agg_payload = cn_aggregate(payload)

    # ✅ CN default overview_metric:
    # - If user uses auto (or empty), force payload override = mix
    #   (render_overview_png(auto_metric) will respect meta.overview_metric)
    om = _norm_overview_metric_arg(str(args.overview_metric))
    if om == "auto":
        agg_payload.setdefault("meta", {})
        # IMPORTANT: use "overview_metric" key (metrics.payload_metric_override() reads this)
        agg_payload["meta"].setdefault("overview_metric", "mix")

    layout = get_layout(args.layout)
    cutoff = _payload_cutoff_str(payload)

    # -------------------------------------------------------------------------
    # ✅ time_note (CN one-line)
    # 中国交易日YYYY-MM-DD  更新 YYYY-MM-DD HH:MM
    # -------------------------------------------------------------------------
    time_note = build_cn_time_note(payload)

    # -------------------------------------------------------------------------
    # 0) Overview first
    # -------------------------------------------------------------------------
    if not args.no_overview:
        try:
            render_overview_png(
                agg_payload,
                outdir,
                width=1080,
                height=1920,
                page_size=int(args.overview_page_size),
                metric=om,  # normalized
            )
        except Exception as e:
            print(f"[CN] overview failed (continue): {e}")

        # ✅ Remove gainbins pages (CN 不需要，避免 list.txt/影片混進去)
        for p in outdir.glob("overview_gainbins*.png"):
            try:
                p.unlink()
                print(f"[CN] removed gainbins page: {p.name}")
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # 1) Sector pages
    # -------------------------------------------------------------------------
    sector_total_map: Dict[str, int] = {}
    for r in universe:
        s = _norm_sector_name(_safe_str(r.get("sector") or "未分类"))
        sector_total_map[s] = sector_total_map.get(s, 0) + 1

    limitup = build_limitup_by_sector_cn(universe)
    peers = build_peers_by_sector_cn(universe)

    width, height = 1080, 1920
    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1
    CAP_PAGES = max(1, int(args.cap_pages))

    for sector, L_total in limitup.items():
        P = peers.get(sector, [])

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

        hit_total, bomb_total, big_total = count_hit_bomb_big(L_total)
        hit_shown, bomb_shown, big_shown = count_hit_bomb_big(L_show)

        sector_all_total = int(sector_total_map.get(sector, 0) or 0)
        sector_shown_total = int(hit_total + bomb_total + big_total)

        # backward-compat params
        locked_cnt = 0
        touch_cnt = bomb_total
        theme_cnt = hit_total

        sector_fn = _sanitize_filename(sector)

        for i in range(total_pages):
            limitup_rows = L_pages[i] if i < len(L_pages) else []
            peer_rows = P_pages[i] if i < len(P_pages) else []

            has_more_peers = (peer_pages > total_pages) and (i == total_pages - 1)

            out_path = outdir / f"cn_{sector_fn}_p{i+1}.png"

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
                touch_shown=bomb_shown,
                touch_total=bomb_total,
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
                lang="zh_hans",
                market="CN",
            )
            print(f"[CN] wrote {out_path}")

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
        print(f"[CN] wrote {list_path}")
    except Exception as e:
        print(f"[CN] list.txt generation failed (continue): {e}")

    print("\n✅ CN render finished.")

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
        market_name = str(args.drive_market or "CN").strip().upper()

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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())