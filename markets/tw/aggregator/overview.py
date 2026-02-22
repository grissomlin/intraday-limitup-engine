# markets/tw/aggregator/overview.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Set

import pandas as pd

from .normalize import _add_ret_fields
from .touch_semantics import fix_touch_double_count_for_overview_rows
from ..config import EMERGING_STRONG_RET, NO_LIMIT_THEME_RET


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if pd.isna(x):
            return default
    except Exception:
        pass
    try:
        return int(x)
    except Exception:
        return default


def _pct(n: int, d: int) -> float:
    return (float(n) / float(d)) if d > 0 else 0.0


def merge_sector_with_universe(
    *,
    sector_rows: List[Dict[str, Any]],
    universe_by_sector: List[Dict[str, Any]],
    key_count: str = "count",
) -> List[Dict[str, Any]]:
    """
    將 sector_summary rows 補上：
      - sector_total / universe_cnt
      - share_of_universe = count / sector_total
      - locked_pct / touched_pct（用 sector_total 當分母）
    """
    if not sector_rows:
        return []

    uni = pd.DataFrame(universe_by_sector or [])
    if uni.empty or "sector" not in uni.columns:
        uni = pd.DataFrame(columns=["sector", "count"])

    uni["sector"] = uni.get("sector", "").fillna("").replace("", "未分類")
    uni["count"] = pd.to_numeric(uni.get("count", 0), errors="coerce").fillna(0).astype(int)
    uni_map = {str(r["sector"]): int(r["count"]) for r in uni.to_dict(orient="records")}

    def _sf(x: Any) -> float:
        try:
            if x is None:
                return 0.0
            if pd.isna(x):
                return 0.0
        except Exception:
            pass
        try:
            return float(x)
        except Exception:
            return 0.0

    out: List[Dict[str, Any]] = []
    for r in sector_rows:
        rr = dict(r or {})
        sec = str(rr.get("sector") or "").strip() or "未分類"
        sector_total = int(uni_map.get(sec, 0))

        rr["sector_total"] = sector_total
        rr["universe_cnt"] = sector_total

        c = _sf(rr.get(key_count, rr.get("total_cnt", 0)))
        rr["share_of_universe"] = (c / sector_total) if sector_total > 0 else 0.0

        locked_cnt = _sf(rr.get("locked_cnt", 0))
        touch_cnt = _sf(rr.get("touch_cnt", 0))
        if sector_total > 0:
            rr["locked_pct"] = locked_cnt / sector_total
            rr["touched_pct"] = touch_cnt / sector_total
        else:
            rr["locked_pct"] = 0.0
            rr["touched_pct"] = 0.0

        out.append(rr)

    return out


def build_overview(
    *,
    dfS: pd.DataFrame,
    dfO: pd.DataFrame,
    open_limit_watchlist: List[Dict[str, Any]],
    sector_summary_main: List[Dict[str, Any]],
    open_limit_sector_summary: List[Dict[str, Any]],
    universe: Dict[str, Any],
    surge_ret: float,
) -> Dict[str, Any]:
    """
    Overview bundle（給 overview renderer 用）：
    - counts/pct
    - sectors_main / sectors_open（都補 sector_total + share_of_universe，並修 touch double-count）

    ⚠️ 注意：
    - open_limit_watchlist 是「觸及門檻 (ret_high>=surge_ret)」語意
    - footer 的 10%+（exclusive）定義是「收盤>=surge_ret 且不含漲停/觸及」
      因此 open-limit 在 footer 的 10%+ 也必須用「收盤>=surge_ret」計數
    """

    uni_total = _safe_int((universe or {}).get("total"), 0)
    uni_by_sector = (universe or {}).get("by_sector") or []

    locked_total = int((dfS.get("is_limitup_locked", False) == True).sum()) if dfS is not None else 0
    touch_total = int((dfS.get("is_limitup_touch", False) == True).sum()) if dfS is not None else 0
    touch_only_total = int((dfS.get("is_touch_only", False) == True).sum()) if dfS is not None else 0

    bigmove10_ex_locked_total = (
        int((dfS.get("is_bigmove10_ex_locked", False) == True).sum()) if dfS is not None else 0
    )
    surge_ge10_total = int((dfS.get("is_surge_ge10", False) == True).sum()) if dfS is not None else 0

    no_limit_theme_total = int((dfS.get("is_no_limit_theme", False) == True).sum()) if dfS is not None else 0

    # -------------------------------------------------------------------------
    # Open-limit totals
    # -------------------------------------------------------------------------
    # watchlist total: touch semantics (ret_high>=threshold, includes opened)
    open_limit_watchlist_total = int(len(open_limit_watchlist or []))

    # close>=10% total: footer exclusive semantics
    open_limit_close_ge10_total = 0
    try:
        if dfO is not None and not dfO.empty:
            # _add_ret_fields writes ret/ret_high into dfO
            _add_ret_fields(dfO)
            open_limit_close_ge10_total = int(
                (pd.to_numeric(dfO.get("ret"), errors="coerce").fillna(0.0) >= float(surge_ret)).sum()
            )
    except Exception:
        open_limit_close_ge10_total = 0

    # ✅ IMPORTANT:
    # renderer footer (exclusive note mode) currently uses:
    #   bigmove10_ex_locked_total + open_limit_theme_total
    # so we bind open_limit_theme_total to "close>=10%" (NOT watchlist).
    open_limit_theme_total = int(open_limit_close_ge10_total)

    # EXCLUSIVE bigmove10_total（note_mode=exclusive）
    bigmove10_total = int(bigmove10_ex_locked_total + open_limit_close_ge10_total)

    # INCLUSIVE（給 gain-bins 或你想看「全部 10%+ 或 touch」的頁）
    inclusive_syms: Set[str] = set()
    try:
        if dfS is not None and not dfS.empty:
            sym = dfS.get("symbol")
            if sym is not None:
                s_sym = sym.astype(str)
                m_close10 = (pd.to_numeric(dfS.get("ret"), errors="coerce").fillna(0.0) >= float(surge_ret))
                inclusive_syms |= set(s_sym[m_close10].tolist())

                m_touch = (dfS.get("is_limitup_touch", False) == True)
                inclusive_syms |= set(s_sym[m_touch].tolist())

        # open-limit: include watchlist symbols (touch semantics)
        for r in (open_limit_watchlist or []):
            if isinstance(r, dict):
                ss = str(r.get("symbol") or "").strip()
                if ss:
                    inclusive_syms.add(ss)
    except Exception:
        inclusive_syms = set()

    bigmove10_inclusive_total = int(len(inclusive_syms))

    # MIX：要用 open_limit_watchlist_total（觸及語意），不是 close>=10%
    mix_total = int(
        locked_total
        + touch_only_total
        + bigmove10_ex_locked_total
        + no_limit_theme_total
        + open_limit_watchlist_total
    )

    touch_only_ret_ge10_total = (
        int((dfS.get("is_touch_only_ret_ge10", False) == True).sum()) if dfS is not None else 0
    )
    touch_only_ret_lt10_total = (
        int((dfS.get("is_touch_only_ret_lt10", False) == True).sum()) if dfS is not None else 0
    )

    open_limit_locked_total = 0
    open_limit_opened_total = 0
    try:
        open_limit_locked_total = int(
            sum(1 for r in (open_limit_watchlist or []) if bool((r or {}).get("is_surge10_locked")))
        )
        open_limit_opened_total = int(
            sum(1 for r in (open_limit_watchlist or []) if bool((r or {}).get("is_surge10_opened")))
        )
    except Exception:
        open_limit_locked_total = 0
        open_limit_opened_total = 0

    sectors_main = merge_sector_with_universe(
        sector_rows=sector_summary_main or [],
        universe_by_sector=uni_by_sector,
        key_count="count",
    )
    sectors_open = merge_sector_with_universe(
        sector_rows=open_limit_sector_summary or [],
        universe_by_sector=uni_by_sector,
        key_count="count",
    )

    # 防 double-count
    sectors_main = fix_touch_double_count_for_overview_rows(sectors_main)
    sectors_open = fix_touch_double_count_for_overview_rows(sectors_open)

    return {
        "counts": {
            "universe_total": uni_total,
            "universe_main": int(len(dfS)) if dfS is not None else 0,
            "universe_open": int(len(dfO)) if dfO is not None else 0,

            "locked_total": locked_total,
            "touch_total": touch_total,  # raw（含 locked）
            "touch_only_total": touch_only_total,

            "bigmove10_main_total": int(surge_ge10_total),
            "bigmove10_total": int(bigmove10_total),  # EXCLUSIVE
            "bigmove10_inclusive_total": int(bigmove10_inclusive_total),

            "bigmove10_ex_locked_total": int(bigmove10_ex_locked_total),
            "open_limit_close_ge10_total": int(open_limit_close_ge10_total),

            "no_limit_theme_total": int(no_limit_theme_total),
            "mix_total": int(mix_total),

            "touch_only_ret_ge10_total": int(touch_only_ret_ge10_total),
            "touch_only_ret_lt10_total": int(touch_only_ret_lt10_total),

            # open-limit totals
            "open_limit_watchlist_total": int(open_limit_watchlist_total),  # touch semantics
            "open_limit_theme_total": int(open_limit_theme_total),  # ✅ close>=10% (compat for footer)
            "open_limit_locked_total": int(open_limit_locked_total),
            "open_limit_opened_total": int(open_limit_opened_total),
        },
        "pct": {
            "locked_total": _pct(locked_total, uni_total),
            "touch_only_total": _pct(touch_only_total, uni_total),

            "bigmove10_total": _pct(bigmove10_total, uni_total),
            "bigmove10_inclusive_total": _pct(bigmove10_inclusive_total, uni_total),

            "bigmove10_ex_locked_total": _pct(bigmove10_ex_locked_total, uni_total),
            "open_limit_close_ge10_total": _pct(open_limit_close_ge10_total, uni_total),

            "no_limit_theme_total": _pct(no_limit_theme_total, uni_total),
            "mix_total": _pct(mix_total, uni_total),

            # open-limit pct
            "open_limit_watchlist_total": _pct(open_limit_watchlist_total, uni_total),
            "open_limit_theme_total": _pct(open_limit_theme_total, uni_total),
            "open_limit_locked_total": _pct(open_limit_locked_total, uni_total),
            "open_limit_opened_total": _pct(open_limit_opened_total, uni_total),
        },
        "sectors_main": sectors_main,
        "sectors_open": sectors_open,
        "universe_by_sector": uni_by_sector,
        "params": {
            "surge_ret_threshold": float(surge_ret),
            "open_limit_ret_high_threshold": float(EMERGING_STRONG_RET),
            "no_limit_theme_ret_threshold": float(NO_LIMIT_THEME_RET),
        },
    }


__all__ = [
    "build_overview",
    "merge_sector_with_universe",
]
