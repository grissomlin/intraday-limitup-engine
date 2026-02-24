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
# =============================================================================
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

import re  # noqa: E402
import sys  # noqa: E402
from datetime import datetime  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any, Dict, List, Tuple, Optional  # noqa: E402

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_jp.sector_blocks.draw_mpl import (  # noqa: E402
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_jp.sector_blocks.layout import get_layout  # noqa: E402

from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402

# ✅ shared ordering helpers
from scripts.render_images_common.sector_order import (  # noqa: E402
    normalize_sector_key,
    extract_overview_sector_order,
    reorder_keys_by_overview,
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
    return [lst[i : i + n] for i in range(0, len(lst), n)]


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
# ✅ NEW: robust overview sector order extractor (handles typo key)
# =============================================================================
def _extract_overview_order_any(payload: Dict[str, Any]) -> List[str]:
    """
    Prefer shared extract_overview_sector_order(), but also support repo typo keys:
      - _overview_sector_orde (missing 'r')
    Also check meta dict.
    """
    # 1) official helper
    try:
        order = extract_overview_sector_order(payload)
        if order:
            return order
    except Exception:
        pass

    # 2) direct keys (including typo)
    for k in ("_overview_sector_order", "_overview_sector_orde"):
        v = payload.get(k)
        if isinstance(v, list) and v:
            out: List[str] = []
            for x in v:
                s = _safe_str(x)
                if s and s not in out:
                    out.append(s)
            if out:
                return out

    # 3) meta keys
    meta = payload.get("meta")
    if isinstance(meta, dict):
        for k in ("_overview_sector_order", "_overview_sector_orde"):
            v = meta.get(k)
            if isinstance(v, list) and v:
                out2: List[str] = []
                for x in v:
                    s = _safe_str(x)
                    if s and s not in out2:
                        out2.append(s)
                if out2:
                    return out2

    return []


def _jp_sector_key(sec: str) -> str:
    """
    Unicode-safe sector key:
    - prefer normalize_sector_key() from shared module
    - if it returns empty (some implementations strip non-ascii), fallback to casefold
    """
    s = _safe_str(sec)
    if not s:
        return ""
    k = ""
    try:
        k = normalize_sector_key(s) or ""
    except Exception:
        k = ""
    if k:
        return k
    return s.strip().casefold()


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
                "limitup_status": status,
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
# Drive subfolder helpers (US規格)  (kept; upload is optional)
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

    # ✅ CHANGE: Drive upload is DEFAULT OFF.
    ap.add_argument("--upload-drive", action="store_true", help="生成後 Drive へアップロード (default: off)")

    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default="JP")
    ap.add_argument("--drive-client-secret", default=None)
    ap.add_argument("--drive-token", default=None)

    ap.add_argument("--drive-subfolder", default=None)
    ap.add_argument("--drive-subfolder-auto", action="store_true", default=True)

    ap.add_argument("--drive-workers", type=int, default=16)
    ap.add_argument("--drive-no-concurrent", action="store_true")
    ap.add_argument("--drive-no-overwrite", action="store_true")
    ap.add_argument("--drive-quiet", action="store_true")

    args = ap.parse_args()

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
    # 0) Overview first + capture overview sector order (robust)
    # -------------------------------------------------------------------------
    overview_sector_keys: List[str] = []
    if not args.no_overview:
        try:
            payload_for_overview = dict(payload)
            payload_for_overview["market"] = market
            payload_for_overview.setdefault(
                "asof",
                payload_for_overview.get("asof") or payload_for_overview.get("slot") or "",
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

            # ✅ IMPORTANT: try multiple keys (incl typo) to extract order
            overview_sector_keys = _extract_overview_order_any(payload_for_overview)

            # If still empty, try original payload too (some implementations mutate original)
            if not overview_sector_keys:
                overview_sector_keys = _extract_overview_order_any(payload)

            if os.getenv("OVERVIEW_DEBUG", "0") == "1":
                print(
                    "[JP][DEBUG] raw _overview_sector_order exists?:",
                    isinstance(payload_for_overview.get("_overview_sector_order"), list)
                    or isinstance(payload_for_overview.get("_overview_sector_orde"), list),
                )
                print(
                    "[JP][DEBUG] raw order head:",
                    (payload_for_overview.get("_overview_sector_order") or payload_for_overview.get("_overview_sector_orde") or [])[:20],
                )
                if overview_sector_keys:
                    met_eff = str(payload_for_overview.get("_overview_metric_eff") or "").strip()
                    print(
                        f"[JP] overview sector order loaded: n={len(overview_sector_keys)}"
                        + (f" metric={met_eff}" if met_eff else "")
                    )
                    print("[JP] overview order head:", overview_sector_keys[:20])
                else:
                    print("[JP][WARN] overview sector order NOT found (will fallback).", flush=True)

        except Exception as e:
            print(f"[JP] overview failed: {e}")

    # -------------------------------------------------------------------------
    # 1) Sector pages — ✅ sort prefer overview order
    # -------------------------------------------------------------------------
    top_rows = build_limitup_by_sector_jp(universe)
    peers = build_peers_by_sector_jp(universe)
    print(f"[JP] sectors(top)={len(top_rows)} sectors(peers)={len(peers)}")

    if top_rows:
        sector_keys_raw = list(top_rows.keys())
    else:

        def _sec_key(sec: str) -> float:
            rr = peers.get(sec, [])
            if not rr:
                return -1e9
            return max(float(x.get("ret", 0.0) or 0.0) for x in rr)

        sector_keys_raw = sorted(peers.keys(), key=_sec_key, reverse=True)[: max(1, int(args.max_sectors))]
        print(f"[JP] fallback: top rows empty; use peers top {len(sector_keys_raw)} sectors")

    # normalize keys (unicode-safe) and keep mapping to original sector
    norm_to_sector: Dict[str, str] = {}
    existing_norm_keys: List[str] = []
    for sec in sector_keys_raw:
        k = _jp_sector_key(sec)
        if not k:
            continue
        existing_norm_keys.append(k)
        if k not in norm_to_sector:
            norm_to_sector[k] = sec

    # normalize overview keys too (so reorder can match)
    overview_norm_keys = [_jp_sector_key(x) for x in (overview_sector_keys or []) if _jp_sector_key(x)]

    if overview_norm_keys:
        ordered_norm_keys = reorder_keys_by_overview(existing_keys=existing_norm_keys, overview_keys=overview_norm_keys)
    else:
        ordered_norm_keys = existing_norm_keys

    sector_keys: List[str] = []
    seen_sec = set()
    for k in ordered_norm_keys:
        sec = norm_to_sector.get(k)
        if not sec or sec in seen_sec:
            continue
        sector_keys.append(sec)
        seen_sec.add(sec)

    sector_keys = sector_keys[: max(1, int(args.max_sectors))]

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
                locked_cnt=hit_total,
                touch_cnt=touch_total,
                theme_cnt=surge_total,
                hit_shown=hit_shown,
                hit_total=hit_total,
                touch_shown=touch_shown,
                touch_total=touch_total,
                surge_shown=surge_shown,
                surge_total=surge_total,
                sector_share=sector_share,
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
    # 1.5) Write list.txt (unified)
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
    # 2) Drive upload (DEFAULT OFF) — best-effort
    # -------------------------------------------------------------------------
    if args.upload_drive:
        print("\n🚀 Uploading PNGs to Google Drive...", flush=True)
        try:
            from scripts.utils.drive_uploader import (  # type: ignore
                get_drive_service,
                ensure_folder,
                upload_dir,
            )

            svc = get_drive_service(
                client_secret_file=args.drive_client_secret,
                token_file=args.drive_token,
            )

            root_id = str(args.drive_root_folder_id).strip()
            market_name = str(args.drive_market or "JP").strip().upper()

            market_folder_id = ensure_folder(svc, root_id, market_name)

            if args.drive_subfolder:
                subfolder = str(args.drive_subfolder).strip()
            else:
                subfolder = make_drive_subfolder_name(payload, market=market_name)

            print(f"📁 Target Drive folder: root/{market_name}/{subfolder}/", flush=True)

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

            print(f"✅ Uploaded {uploaded} png(s)", flush=True)
        except Exception as e:
            print(f"[WARN] Drive upload failed (best-effort, continue): {e}", flush=True)
    else:
        print("\n[drive] upload skipped (default off).", flush=True)

    print("\n✅ JP render finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
