# scripts/render_images_kr/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

# ✅ VERY IMPORTANT: Force matplotlib headless backend (avoid tkinter warnings)
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# ✅ DEFAULT DEBUG ON (overview + footer)
# - OVERVIEW_DEBUG_FOOTER: footer 佈局/文字座標/行內容等
# - OVERVIEW_DEBUG_FONTS : i18n_font debug（包含 rcParams['font.sans-serif'] order）
# - OVERVIEW_DEBUG        : 若 repo 內還有總開關也順便開
# =============================================================================
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

import re  # noqa: E402
import sys  # noqa: E402
from datetime import datetime  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any, Dict, List, Optional, Tuple  # noqa: E402

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_kr.sector_blocks.draw_mpl import (  # noqa: E402
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_kr.sector_blocks.layout import get_layout  # noqa: E402

# ✅ Common overview (optional)
from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402

# ✅ shared ordering helpers (NO local duplicate funcs)
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
# Utils
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
    return str(x).strip().lower() in ("1", "true", "yes", "y", "on")


def _s(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _bool_any(r: Dict[str, Any], *keys: str) -> bool:
    for k in keys:
        if _bool(r.get(k)):
            return True
    return False


def load_payload(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    ✅ Use full-market universe first, so peers can be computed correctly.
    Support both raw payload and agg payload that still carries snapshots.
    """
    for k in ("snapshot_main", "snapshot_all", "snapshot_open", "snapshot"):
        rows = payload.get(k) or []
        if isinstance(rows, list) and rows:
            return rows
    rows = payload.get("universe") or []
    if isinstance(rows, list) and rows:
        return rows
    return []


def _payload_ymd(payload: Dict[str, Any]) -> str:
    return _s(payload.get("ymd_effective") or payload.get("ymd") or "")


def _payload_slot(payload: Dict[str, Any]) -> str:
    s = _s(payload.get("slot") or "")
    return s or "unknown"


def _sanitize_filename(s: str) -> str:
    s = _s(s)
    if not s:
        return "unknown"
    s = s.replace(" ", "_")
    s = re.sub(r"[^\w\u4e00-\u9fff\u3040-\u30ff\u31f0-\u31ff\u3400-\u4dbf\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i : i + n] for i in range(0, len(lst), n)]


# =============================================================================
# list.txt generator (unified, JP/US compatible)
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
# KR Status / Badge (Korean copy)
# =============================================================================
def clean_sector_name(s: Any) -> str:
    ss = _s(s) or "미분류"
    ss = re.sub(r"\s+", " ", ss).strip()
    if ss in ("-", "--", "—", "–", "－", ""):
        ss = "미분류"
    return ss


def is_limitup_locked_kr(r: Dict[str, Any]) -> bool:
    # ✅ support new/old keys
    return _bool_any(r, "is_limitup30_locked", "is_limitup_locked")


def is_limitup_touch_kr(r: Dict[str, Any]) -> bool:
    # ✅ support new/old keys
    return _bool_any(r, "is_limitup30_touch", "is_limitup_touch")


def is_touch_only_kr(r: Dict[str, Any]) -> bool:
    return is_limitup_touch_kr(r) and (not is_limitup_locked_kr(r))


def is_bigup10_kr(r: Dict[str, Any]) -> bool:
    # ✅ snapshot_builder key
    return _bool_any(r, "is_bigup10", "is_bigup")


def is_event_stock_kr(r: Dict[str, Any], ret_th: float) -> bool:
    """Event stock if limitup/touch or ret >= threshold."""
    if is_limitup_locked_kr(r) or is_limitup_touch_kr(r):
        return True
    return _pct(r.get("ret")) >= float(ret_th)


def badge_text_from_ret_kr(ret: float) -> str:
    # Only for non-limitup stocks (>=10% but not limitup/touch) or peers.
    if ret >= 0.20:
        return "급등"
    return "강세"


def yesterday_text_kr(r: Dict[str, Any]) -> str:
    """
    Yesterday status:
    - Prefer payload status_line2 (snapshot_builder)
    - Else use streak30_prev / streak10_prev
    - Fallback: no signal
    """
    sl2 = _s(r.get("status_line2"))
    if sl2:
        if sl2 in ("昨無", "어제없음", "어제 무", "없음"):
            return "어제 상한가/10%+ 없음"
        if sl2 == "昨漲停":
            return "어제 상한가"
        if sl2 == "昨觸及":
            return "어제 상한가 터치"
        return sl2

    s30 = _int(r.get("streak30_prev", 0))
    s10 = _int(r.get("streak10_prev", 0))
    if s30 > 0:
        return f"어제 상한가 {s30}연속"
    if s10 > 0:
        return f"어제 10%+ {s10}일"
    return "어제 상한가/10%+ 없음"


def get_new_listing_mark_kr(r: Dict[str, Any]) -> str:
    is_new = _bool(r.get("is_new_listing", False))
    if not is_new:
        return ""
    new_date = _s(r.get("new_listing_date", ""))
    if new_date:
        return f"신규상장({new_date})"
    return "신규상장"


# =============================================================================
# Builders
# =============================================================================
def build_events_by_sector_kr(
    universe: List[Dict[str, Any]], ret_th: float
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Top box (event stocks):
    - locked: 상한가
    - touch-only: 상한가 터치
    - else (>=ret_th): 급등/강세
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        if not is_event_stock_kr(r, ret_th):
            continue

        sector = clean_sector_name(r.get("sector"))
        sym = _s(r.get("symbol"))
        if not sym:
            continue

        name = _s(r.get("name") or sym)
        ret = _pct(r.get("ret"))

        is_locked = is_limitup_locked_kr(r)
        is_touch = is_touch_only_kr(r)

        if is_locked:
            badge = "상한가"
            status = "locked"
        elif is_touch:
            badge = "상한가 터치"
            status = "touch"
        else:
            badge = badge_text_from_ret_kr(ret)
            status = "surge"

        ytxt = yesterday_text_kr(r)
        new_mark = get_new_listing_mark_kr(r)

        line1 = f"{sym}  {name}"
        if new_mark:
            line2 = f"{badge} | {ytxt} | {new_mark}"
        else:
            line2 = f"{badge} | {ytxt}"

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "badge_text": badge,
                "limitup_status": status,  # locked / touch / surge
                "line1": line1,
                "line2": line2,
                "is_limitup_locked": bool(is_locked),
                "is_limitup_touch": bool(is_touch),
                "is_touch_only": bool(is_touch),
                "is_bigup": bool(is_bigup10_kr(r)),
                "is_new_listing": bool(_bool(r.get("is_new_listing", False))),
                "streak30_prev": _int(r.get("streak30_prev", 0)),
                "streak10_prev": _int(r.get("streak10_prev", 0)),
            }
        )

    def _rank(st: str) -> int:
        if st == "locked":
            return 0
        if st == "touch":
            return 1
        return 2

    for k in out:
        out[k].sort(
            key=lambda x: (
                -int(x.get("is_new_listing", False)),
                _rank(str(x.get("limitup_status") or "")),
                -(x.get("ret") or 0.0),
            )
        )

    return out


def build_peers_by_sector_kr(
    universe: List[Dict[str, Any]],
    events_by_sector: Dict[str, List[Dict[str, Any]]],
    *,
    ret_min: float,
    max_per_sector: int,
    ret_th_event: float,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Bottom box (peers):
    - only sectors that have event stocks
    - exclude event stocks (limitup/touch or ret >= ret_th_event)
    - include ret >= ret_min
    """
    if not events_by_sector:
        return {}

    event_syms = {
        _s(rr.get("symbol"))
        for rows in events_by_sector.values()
        for rr in (rows or [])
        if _s(rr.get("symbol"))
    }
    sectors = set(events_by_sector.keys())
    out: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe:
        sym = _s(r.get("symbol"))
        if not sym or sym in event_syms:
            continue

        sector = clean_sector_name(r.get("sector"))
        if sector not in sectors:
            continue

        if is_event_stock_kr(r, float(ret_th_event)):
            continue

        ret = _pct(r.get("ret"))
        if ret < float(ret_min):
            continue

        name = _s(r.get("name") or sym)
        ytxt = yesterday_text_kr(r)
        new_mark = get_new_listing_mark_kr(r)

        line1 = f"{sym}  {name}"
        if new_mark:
            line2 = f"강세 | {ytxt} | {new_mark}"
        else:
            line2 = f"강세 | {ytxt}"

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "line1": line1,
                "line2": line2,
                "badge_text": "",
                "is_new_listing": bool(_bool(r.get("is_new_listing", False))),
            }
        )

    for k in out:
        out[k].sort(key=lambda x: (-int(x.get("is_new_listing", False)), -(x.get("ret") or 0.0)))
        out[k] = out[k][: int(max_per_sector)]

    return out


# =============================================================================
# Overview payload builders
# =============================================================================
def _build_sector_summary_from_events(
    events_by_sector: Dict[str, List[Dict[str, Any]]]
) -> List[Dict[str, Any]]:
    """
    ⚠️ NOTE:
    This is an EVENTS-ONLY summary (sector_total == event count).
    It must NOT be used to drive overview breadth badges.
    Keep it only for debugging / legacy fallback.
    """
    rows: List[Dict[str, Any]] = []
    for sector, es in (events_by_sector or {}).items():
        locked_cnt = sum(1 for r in es if str(r.get("limitup_status")) == "locked")
        touched_cnt = sum(1 for r in es if str(r.get("limitup_status")) == "touch")
        bigmove10_cnt = sum(1 for r in es if str(r.get("limitup_status")) == "surge")
        total_cnt = len(es or [])
        rows.append(
            {
                "sector": sector,
                "locked_cnt": int(locked_cnt),
                "touched_cnt": int(touched_cnt),
                "bigmove10_cnt": int(bigmove10_cnt),
                "total_cnt": int(total_cnt),
            }
        )
    return rows


def _sector_summary_is_overview_ready(payload: Dict[str, Any]) -> bool:
    """
    ✅ Overview-ready sector_summary means:
    - list[dict]
    - has universe denominator: sector_total (or similar)
    - has *_pct fields
    """
    ss = payload.get("sector_summary")
    if not isinstance(ss, list) or not ss:
        return False
    r0 = ss[0]
    if not isinstance(r0, dict):
        return False

    has_den = ("sector_total" in r0) or ("sector_cnt" in r0) or ("total_cnt" in r0)
    has_pct = any(k in r0 for k in ("mix_pct", "locked_pct", "touched_pct", "bigmove10_pct"))
    return bool(has_den and has_pct)


def _apply_kr_overview_copy(payload: Dict[str, Any]) -> None:
    payload["overview_title"] = payload.get("overview_title") or "업종별 이벤트 종목 수 (Top)"
    payload["overview_footer"] = payload.get("overview_footer") or "KR 스냅샷"
    payload["overview_note"] = (
        payload.get("overview_note")
        or "시장 제도 차이로 상한가/터치/급등(10%+)가 함께 표시될 수 있습니다"
    )


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

    ap.add_argument("--ret-th", type=float, default=0.10, help="event stock threshold (default 10%)")
    ap.add_argument("--peer-ret-min", type=float, default=0.05)
    ap.add_argument("--peer-max-per-sector", type=int, default=10)

    # Overview
    ap.add_argument("--no-overview", action="store_true")
    ap.add_argument("--overview-metric", default="auto")
    ap.add_argument("--overview-page-size", type=int, default=15)

    ap.add_argument("--no-debug", action="store_true", help="overview/footer debug 비활성화")

    # ✅ CHANGE: Drive upload is DEFAULT OFF.
    ap.add_argument("--upload-drive", action="store_true", help="생성 후 Drive 업로드 (default: off)")
    ap.add_argument("--drive-root-folder-id", default=DEFAULT_ROOT_FOLDER)
    ap.add_argument("--drive-market", default="KR")
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
        raise RuntimeError("No usable snapshot in payload (need snapshot_main/snapshot_all/...)")

    ymd = _payload_ymd(payload) or "unknown"
    slot = _payload_slot(payload)

    if args.outdir:
        outdir = Path(args.outdir)
    else:
        outdir = REPO_ROOT / "media" / "images" / "kr" / ymd / slot
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[KR] payload={args.payload}")
    print(f"[KR] ymd={ymd} slot={slot} outdir={outdir}")
    print(f"[KR] universe={len(universe)}")
    print(
        "[KR] debug="
        f"footer={os.getenv('OVERVIEW_DEBUG_FOOTER','0')} "
        f"fonts={os.getenv('OVERVIEW_DEBUG_FONTS','0')}"
    )

    layout = get_layout(args.layout)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    events = build_events_by_sector_kr(universe, float(args.ret_th))
    peers = build_peers_by_sector_kr(
        universe,
        events,
        ret_min=float(args.peer_ret_min),
        max_per_sector=int(args.peer_max_per_sector),
        ret_th_event=float(args.ret_th),
    )

    payload.setdefault("market", "KR")
    payload.setdefault("asof", payload.get("asof") or payload.get("slot") or "")

    payload["sector_summary_events"] = _build_sector_summary_from_events(events)
    if not _sector_summary_is_overview_ready(payload):
        payload["sector_summary"] = payload["sector_summary_events"]

    _apply_kr_overview_copy(payload)

    # -------------------------------------------------------------------------
    # 0) Overview first + capture _overview_sector_order (preferred)
    # -------------------------------------------------------------------------
    overview_sector_keys: List[str] = []
    if not args.no_overview:
        try:
            payload_for_overview = dict(payload)

            overview_paths = render_overview_png(
                payload_for_overview,
                outdir,
                width=1080,
                height=1920,
                page_size=int(args.overview_page_size),
                metric=str(args.overview_metric or "auto"),
            )
            for p in (overview_paths or []):
                print(f"[KR] wrote {p}")

            # ✅ IMPORTANT: overview exports order into payload_for_overview
            overview_sector_keys = extract_overview_sector_order(payload_for_overview)

            print(
                "[KR][DEBUG] raw _overview_sector_order exists?:",
                isinstance(payload_for_overview.get("_overview_sector_order"), list),
            )
            print(
                "[KR][DEBUG] raw overview order head:",
                (payload_for_overview.get("_overview_sector_order", []) or [])[:20],
            )
            if overview_sector_keys:
                met_eff = str(payload_for_overview.get("_overview_metric_eff") or "").strip()
                print(
                    f"[KR] overview sector order loaded: n={len(overview_sector_keys)}"
                    + (f" metric={met_eff}" if met_eff else "")
                )
                print("[KR] normalized overview order head:", overview_sector_keys[:20])
            else:
                print("[KR][WARN] overview order empty after normalization", flush=True)

        except Exception as e:
            print(f"[KR] overview failed: {e}")

    # -------------------------------------------------------------------------
    # 1) Sector pages — ✅ order by overview keys first, then remaining
    # -------------------------------------------------------------------------
    width, height = 1080, 1920
    rows_top = max(1, int(args.rows_per_box))
    rows_peer = rows_top + 1
    CAP_PAGES = 5

    # Base sector list: sectors that have events (top box)
    sectors_raw: List[str] = list((events or {}).keys())
    if not sectors_raw:
        print("[KR] no event sectors; nothing to render.")
    else:
        # Build normalized map
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

        # Resolve back to sector names (dedup)
        sector_keys: List[str] = []
        seen = set()
        for k in ordered_norm:
            sec = norm_to_sector.get(k)
            if not sec or sec in seen:
                continue
            sector_keys.append(sec)
            seen.add(sec)

        for sector in sector_keys:
            E_total = (events or {}).get(sector, []) or []
            P_total = (peers or {}).get(sector, []) or []

            E_show = E_total[: CAP_PAGES * rows_top]
            E_pages = chunk(E_show, rows_top) if E_show else [[]]
            P_pages_all = chunk(P_total, rows_peer) if P_total else [[]]

            top_pages = len(E_pages)
            peer_pages = len(P_pages_all)

            total_pages = top_pages
            if peer_pages > top_pages:
                total_pages = top_pages + 1
            total_pages = min(CAP_PAGES, max(1, total_pages))

            locked_total = sum(1 for r in E_total if str(r.get("limitup_status")) == "locked")
            touch_total = sum(1 for r in E_total if str(r.get("limitup_status")) == "touch")
            hit_total = len(E_total)

            # "shown" counts use prefix of E_show per page
            sector_fn = _sanitize_filename(sector)

            for i in range(total_pages):
                limitup_rows = E_pages[i] if i < len(E_pages) else []
                peer_rows = P_pages_all[i] if i < len(P_pages_all) else []

                prefix_n = min(len(E_show), (i + 1) * rows_top)
                prefix_rows = E_show[:prefix_n]
                locked_shown_i = sum(1 for r in prefix_rows if str(r.get("limitup_status")) == "locked")
                touch_shown_i = sum(1 for r in prefix_rows if str(r.get("limitup_status")) == "touch")
                hit_shown_i = len(prefix_rows)

                has_more_peers = (peer_pages > total_pages) and (i == total_pages - 1)

                out_path = outdir / f"kr_{sector_fn}_p{i+1}.png"

                draw_block_table(
                    out_path=out_path,
                    layout=layout,
                    sector=sector,
                    cutoff=cutoff,
                    locked_cnt=int(locked_total),
                    touch_cnt=int(touch_total),
                    theme_cnt=int(hit_total),
                    hit_shown=int(hit_shown_i),
                    hit_total=int(hit_total),
                    touch_shown=int(touch_shown_i),
                    touch_total=int(touch_total),
                    locked_shown=int(locked_shown_i),
                    locked_total=int(locked_total),
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
                print(f"[KR] wrote {out_path}")

    print("✅ KR render finished.")

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
        print(f"[KR] wrote {list_path}")
    except Exception as e:
        print(f"[KR] list.txt generation failed (continue): {e}")

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
            market_name = str(args.drive_market or "KR").strip().upper()

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
            # ✅ Never fail the render due to Drive
            print(f"[WARN] Drive upload failed (best-effort, continue): {e}", flush=True)
    else:
        print("\n[drive] upload skipped (default off).", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
