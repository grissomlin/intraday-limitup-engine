# scripts/render_images_tw/tw_rows.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

from scripts.render_images_tw.utils_tw import pct, to_bool, to_int, safe_str, norm_sector


# =============================================================================
# Text helpers
# =============================================================================
def board_tag_tw(r: Dict[str, Any]) -> str:
    """
    只有興櫃/開放市場池才顯示〔興櫃〕
    market_detail 可能是 rotc/emerging/open_limit
    """
    md = safe_str(r.get("market_detail") or "").lower()
    return "〔興櫃〕" if md in ("rotc", "emerging", "open_limit") else ""


def line1_tw(r: Dict[str, Any]) -> str:
    sym = safe_str(r.get("symbol") or "")
    name = safe_str(r.get("name") or sym)
    tag = board_tag_tw(r)
    return f"{sym}  {name} {tag}".rstrip()


# =============================================================================
# Prev-day status (must show in BOTH top & peer boxes)
# =============================================================================
def _prev_locked(r: Dict[str, Any]) -> bool:
    return (
        to_bool(r.get("prev_is_limitup_locked", False))
        or to_bool(r.get("prev_was_limitup_locked", False))
        or to_bool(r.get("prev_was_locked", False))
    )


def _prev_touch(r: Dict[str, Any]) -> bool:
    return (
        to_bool(r.get("prev_is_limitup_touch", False))
        or to_bool(r.get("prev_was_limitup_touch", False))
        or to_bool(r.get("prev_was_touch", False))
    )


def _prev_surge10(r: Dict[str, Any]) -> bool:
    # 興櫃/開放式：前一交易日是否 >=10%（收盤 or 盤中）
    return to_bool(r.get("prev_is_surge10", False)) or to_bool(r.get("prev_is_surge10_touch", False))


def prev_status_tw(r: Dict[str, Any]) -> str:
    """
    回傳：
      - "前一交易日：X連漲停"
      - "前一交易日：有觸及漲停"
      - "前一交易日：無漲停"
      - (興櫃/開放式) "前一交易日：X連10%+"
      - (興櫃/開放式) "前一交易日：無10%+"
    """
    md = safe_str(r.get("market_detail") or "").lower()
    is_open_limit_pool = md in ("rotc", "emerging", "open_limit")

    if is_open_limit_pool:
        n_prev = to_int(r.get("surge_streak_prev", 0) or r.get("streak_prev", 0))
        if _prev_surge10(r):
            if n_prev > 0:
                return f"前一交易日：{n_prev}連10%+"
            return "前一交易日：10%+"
        return "前一交易日：無10%+"

    # standard board: limitup semantics
    n_prev = to_int(r.get("streak_prev", 0))
    if _prev_locked(r):
        if n_prev > 0:
            return f"前一交易日：{n_prev}連漲停"
        return "前一交易日：漲停"
    if _prev_touch(r):
        return "前一交易日：有觸及漲停"
    return "前一交易日：無漲停"


# =============================================================================
# Line2 (Top / Peer)
# =============================================================================
def line2_tw_peer(r: Dict[str, Any]) -> str:
    """
    ✅ 下框：只顯示「前一交易日狀態」
    """
    return prev_status_tw(r)


def line2_tw_top(r: Dict[str, Any]) -> str:
    """
    上框：第二行要顯示今日連板 + 前一交易日狀態
    - 主板：今日X連漲停
    - 興櫃：今日X連10%+
    """
    md = safe_str(r.get("market_detail") or "").lower()
    is_open_limit_pool = md in ("rotc", "emerging", "open_limit")

    if is_open_limit_pool:
        n = to_int(r.get("surge_streak", 0) or r.get("streak", 0))
        today_part = f"今日{n}連10%+" if n > 0 else "今日無連10%+"
        return f"{today_part} | {prev_status_tw(r)}"

    n = to_int(r.get("streak", 0))
    today_part = f"今日{n}連漲停" if n > 0 else "今日無連板"
    return f"{today_part} | {prev_status_tw(r)}"


def streak_badge_tw(r: Dict[str, Any], *, base: str) -> str:
    """
    右側 badge 文字：
    - 主板 locked：顯示 X連漲停（>=2）/ 漲停
    - 興櫃大漲：顯示 X連10%+（>=2）/ 10%+
    """
    md = safe_str(r.get("market_detail") or "").lower()
    is_open_limit_pool = md in ("rotc", "emerging", "open_limit")

    if is_open_limit_pool:
        n = to_int(r.get("surge_streak", 0) or r.get("streak", 0))
        if n >= 2:
            return f"{n}連10%+"
        return "10%+"

    if base != "漲停鎖死":
        return base

    n = to_int(r.get("streak", 0))
    if n >= 2:
        return f"{n}連漲停"
    return "漲停"


# =============================================================================
# Build rows by sector (TOP)
# =============================================================================
def build_top_rows_by_sector_tw(payload: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    mix top box rows:
    - 主板：locked / touch_only
    - 興櫃池：close>=10% or high>=10%
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    main = payload.get("snapshot_main") or []
    opn = payload.get("snapshot_open") or []
    rows: List[Dict[str, Any]] = []
    if isinstance(main, list):
        rows.extend([r for r in main if isinstance(r, dict)])
    if isinstance(opn, list):
        rows.extend([r for r in opn if isinstance(r, dict)])

    if not rows:
        return out

    for r in rows:
        lt = safe_str(r.get("limit_type") or "standard").lower()
        md = safe_str(r.get("market_detail") or "").lower()

        is_locked = to_bool(r.get("is_limitup_locked", False)) or to_bool(r.get("is_true_limitup", False))
        is_touch = to_bool(r.get("is_limitup_touch", False))
        is_touch_only = to_bool(r.get("is_touch_only", False)) or (is_touch and (not is_locked))

        ret = pct(r.get("ret", 0.0))
        ret_high = pct(r.get("ret_high", 0.0))

        in_locked = (lt == "standard") and is_locked
        in_touch_only = (lt == "standard") and is_touch_only

        is_open_limit_pool = md in ("rotc", "emerging", "open_limit")
        in_emerging_10p = is_open_limit_pool and ((ret >= 0.10) or (ret_high >= 0.10))

        if not (in_locked or in_touch_only or in_emerging_10p):
            continue

        sector = norm_sector(r.get("sector") or "未分類")
        sym = safe_str(r.get("symbol") or "")
        name = safe_str(r.get("name") or sym)

        if in_locked:
            base_badge = "漲停鎖死"
            status = "locked"
        elif in_touch_only:
            base_badge = "漲停鎖死失敗"
            status = "touch"
        else:
            base_badge = "漲幅10%+"
            status = "surge"

        badge = streak_badge_tw(r, base=base_badge)

        out.setdefault(sector, []).append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "ret": ret,
                "ret_pct": ret * 100.0,
                "badge_text": badge,
                "line1": line1_tw(r),
                "line2": line2_tw_top(r),
                "market_detail": safe_str(r.get("market_detail") or ""),
                "limitup_status": status,
                "is_true_limitup": bool(in_locked),
                "is_surge_ge10": bool(in_emerging_10p),
            }
        )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0) or 0.0), reverse=True)

    return out


# =============================================================================
# Build peers by sector (BOTTOM)
# =============================================================================
def _watchlist_symbols(payload: Dict[str, Any]) -> set[str]:
    wl = payload.get("open_limit_watchlist") or payload.get("emerging_watchlist") or []
    out: set[str] = set()
    if isinstance(wl, list):
        for r in wl:
            if not isinstance(r, dict):
                continue
            sym = safe_str(r.get("symbol") or "")
            if sym:
                out.add(sym)
    return out


def build_peers_by_sector_tw(
    payload: Dict[str, Any],
    sector_keys: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    peers bottom box（同產業・未進榜）：
    ✅ 只有會出頁的 sector 才做 peers（sector_keys 由 pipeline 傳入，= 有 top 的 sector）
    ✅ peers 來源混合：
       A) snapshot_main（上市/上櫃）：排除 locked / touch_only
       B) snapshot_open（興櫃）：排除 >=10%（close/high 任一 >=10）+ 排除 watchlist
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not sector_keys:
        return out

    sector_set = {norm_sector(s) for s in sector_keys}

    # ---- A) standard board peers ----
    main = payload.get("snapshot_main") or []
    if isinstance(main, list):
        for r in main:
            if not isinstance(r, dict):
                continue

            lt = safe_str(r.get("limit_type") or "standard").lower()
            if lt != "standard":
                continue

            is_locked = to_bool(r.get("is_limitup_locked", False)) or to_bool(r.get("is_true_limitup", False))
            is_touch = to_bool(r.get("is_limitup_touch", False))
            is_touch_only = to_bool(r.get("is_touch_only", False)) or (is_touch and (not is_locked))
            if is_locked or is_touch_only:
                continue

            sector = norm_sector(r.get("sector") or "未分類")
            if sector not in sector_set:
                continue

            sym = safe_str(r.get("symbol") or "")
            name = safe_str(r.get("name") or sym)
            ret = pct(r.get("ret", 0.0))
            ret_pct = ret * 100.0

            out.setdefault(sector, []).append(
                {
                    "symbol": sym,
                    "name": name,
                    "sector": sector,
                    "ret": ret,
                    "ret_pct": ret_pct,
                    "line1": line1_tw(r),
                    "line2": line2_tw_peer(r),
                    "market_detail": safe_str(r.get("market_detail") or ""),
                }
            )

    # ---- B) emerging/open-limit peers ----
    wl_syms = _watchlist_symbols(payload)

    opn = payload.get("snapshot_open") or []
    if isinstance(opn, list):
        for r in opn:
            if not isinstance(r, dict):
                continue

            md = safe_str(r.get("market_detail") or "").lower()
            if md not in ("rotc", "emerging", "open_limit"):
                continue

            sym = safe_str(r.get("symbol") or "")
            if sym and sym in wl_syms:
                continue

            sector = norm_sector(r.get("sector") or "未分類")
            if sector not in sector_set:
                continue

            ret = pct(r.get("ret", 0.0))
            ret_high = pct(r.get("ret_high", 0.0))

            # ✅ 未進榜：close/high 都 < 10%
            if (ret >= 0.10) or (ret_high >= 0.10):
                continue

            name = safe_str(r.get("name") or sym)
            out.setdefault(sector, []).append(
                {
                    "symbol": sym,
                    "name": name,
                    "sector": sector,
                    "ret": ret,
                    "ret_pct": ret * 100.0,
                    "line1": line1_tw(r),
                    "line2": line2_tw_peer(r),
                    "market_detail": safe_str(r.get("market_detail") or ""),
                }
            )

    for k in out:
        out[k].sort(key=lambda x: float(x.get("ret", 0.0) or 0.0), reverse=True)

    return out


def count_locked_touch_surge(rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    locked = 0
    touch = 0
    surge = 0
    for r in rows:
        s = safe_str(r.get("limitup_status") or "").lower()
        if s == "touch":
            touch += 1
        elif s == "surge":
            surge += 1
        else:
            locked += 1
    return locked, touch, surge


# =============================================================================
# Debug printing (for cli.py imports)
# =============================================================================
def print_open_limit_watchlist(payload: Dict[str, Any], n: int = 10) -> None:
    n = max(0, int(n))
    wl = payload.get("open_limit_watchlist") or payload.get("emerging_watchlist") or []
    if not isinstance(wl, list):
        wl = []

    print("\n" + "=" * 96)
    print(f"[DEBUG] open_limit_watchlist (top {n}) | total={len(wl)}")
    print("=" * 96)

    if n <= 0:
        print("(skip) --debug-rows=0")
        return

    head = wl[:n]
    if not head:
        print("(empty)")
        return

    for i, r in enumerate(head):
        if not isinstance(r, dict):
            continue
        sym = safe_str(r.get("symbol"))
        name = safe_str(r.get("name"))
        sec = safe_str(r.get("sector"))
        md = safe_str(r.get("market_detail"))
        ret = pct(r.get("ret", 0.0)) * 100.0
        rh = pct(r.get("ret_high", 0.0)) * 100.0
        locked = to_bool(r.get("is_surge10_locked", False))
        opened = to_bool(r.get("is_surge10_opened", False))
        touch = to_bool(r.get("is_surge10_touch", False))
        flag = "LOCKED" if locked else "OPENED" if opened else "TOUCH" if touch else "-"
        print(f"[{i:02d}] {sym:10s} {name} | {sec} | md={md} | ret={ret:+.2f}% high={rh:+.2f}% | {flag}")

    print("-" * 96)
    print("Tip: 想找 3184 -> 用：python ... --debug-rows 200 | findstr 3184")
    print("-" * 96)


def print_sector_top_rows(payload: Dict[str, Any], sector: str, n: int = 30) -> None:
    sector = safe_str(sector)
    if not sector:
        return

    top_rows = build_top_rows_by_sector_tw(payload)
    rows = top_rows.get(sector) or []

    print("\n" + "=" * 96)
    print(f"[DEBUG] sector top rows | sector={sector} | total={len(rows)} | show={min(len(rows), n)}")
    print("=" * 96)

    for i, r in enumerate(rows[: max(0, int(n))]):
        sym = safe_str(r.get("symbol"))
        name = safe_str(r.get("name"))
        badge = safe_str(r.get("badge_text"))
        line2 = safe_str(r.get("line2"))
        ret = pct(r.get("ret", 0.0)) * 100.0
        status = safe_str(r.get("limitup_status"))
        print(f"[{i:02d}] {sym:10s} {name} | {badge} | {ret:+.2f}% | {status} | {line2}")
