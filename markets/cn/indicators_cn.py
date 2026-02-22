# markets/cn/indicators_cn.py
# -*- coding: utf-8 -*-
"""
CN indicators: 连板(streak) / streak_prev / prev_was_limitup_locked (+touch)

改版重點：
- 不再用 ret >= limit_rate 近似封板（容易被四捨五入 / tick 影響）
- 改為：先算 limit_price（漲停價），再用 high/close 判斷 touch/locked
- streak 預設以 locked（封板）連續天數計算，更符合「連板」語意
- 額外提供 prev_was_limitup_touch（昨日盤中觸及漲停但可能炸板）

漲跌幅限制（比例）推斷：
- 主板：10%
- 创业板/科创板：20%  (30xxxx / 68xxxx)
- 北交所：30%         (8xxxx / 4xxxx 常見)
- ST / *ST：5%        （用 name 判斷，含 ST）

⚠️ 注意：
A 股實際漲停價有更細節的「價格進位/四捨五入」規則；
此版採用「保守、可重現」的兩位小數 ROUND_HALF_UP 計算，
並透過 eps_price 容忍資料源誤差，通常已足夠穩定。
"""

from __future__ import annotations

import os
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, Tuple

import pandas as pd

_ST_RE = re.compile(r"(^|\s)\*?ST", re.IGNORECASE)


# =============================================================================
# Small helpers
# =============================================================================
def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _is_st_name(name: Any) -> bool:
    n = _safe_str(name).upper()
    if not n:
        return False
    return bool(_ST_RE.search(n))


def _infer_cn_limit_rate(symbol: Any, name: Any = None) -> float:
    """
    回傳該股日漲跌幅限制（比例）。
    """
    sym = _safe_str(symbol)

    # ST / *ST（5%）
    if _is_st_name(name):
        return 0.05

    # 北交所（常見 8xxxx / 4xxxx）
    if sym.startswith("8") or sym.startswith("4"):
        return 0.30

    # 创业板 30xxxx / 科创板 68xxxx（20%）
    if sym.startswith("30") or sym.startswith("68"):
        return 0.20

    # 其他：主板 10%
    return 0.10


def _round2_half_up(x: float) -> float:
    """
    A 股價格基本是 0.01 精度（大多數情況）。
    用 Decimal 做兩位小數四捨五入（ROUND_HALF_UP），避免 python round 的 banker's rounding。
    """
    try:
        d = Decimal(str(float(x))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return float(d)
    except Exception:
        # fallback（很少走到）
        return float(round(float(x), 2))


def _calc_limit_price(prev_close: float, limit_rate: float) -> float:
    """
    漲停價（近似）：prev_close * (1 + limit_rate)，再四捨五入到 2 位。
    """
    return _round2_half_up(float(prev_close) * (1.0 + float(limit_rate)))


def _get_streak_mode() -> str:
    """
    連板計算用哪一種旗標：
    - locked: 以封板(收盤在漲停價)計算（預設、建議）
    - touch : 以觸及(盤中到過漲停價)計算（可選）
    """
    v = os.getenv("CN_STREAK_MODE", "locked").strip().lower()
    return v if v in ("locked", "touch") else "locked"


# =============================================================================
# Public API
# =============================================================================
def compute_cn_streak_maps(
    daily_df: pd.DataFrame,
    *,
    ymd_effective: str,
    eps_price: float = 1e-4,
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, bool], Dict[str, bool]]:
    """
    daily_df 需要欄位：
      symbol, date(or ymd), close, prev_close(or last_close), name(可選)
    若要計算 touch（盤中觸及），daily_df 需額外有 high 欄位（可選）。

    回傳：
      streak_map[symbol] = 今日连板数（以 ymd_effective 為最後一天）
      streak_prev_map[symbol] = 昨日连板数（以 ymd_effective-1 那天為最後一天）
      prev_was_locked_map[symbol] = 昨日是否封板（True/False）
      prev_was_touch_map[symbol]  = 昨日是否觸及（True/False；若無 high 會是 False）
    """
    if daily_df is None or daily_df.empty:
        return {}, {}, {}, {}

    df = daily_df.copy()

    # normalize columns
    if "date" in df.columns and "ymd" not in df.columns:
        df["ymd"] = df["date"]

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["ymd"] = pd.to_datetime(df["ymd"], errors="coerce").dt.strftime("%Y-%m-%d")

    # numeric
    if "close" in df.columns:
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "high" in df.columns:
        df["high"] = pd.to_numeric(df["high"], errors="coerce")

    # prefer prev_close / last_close
    if "prev_close" in df.columns:
        df["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce")
    elif "last_close" in df.columns:
        df["prev_close"] = pd.to_numeric(df["last_close"], errors="coerce")
    else:
        # compute from close
        df = df.sort_values(["symbol", "ymd"], kind="mergesort")
        df["prev_close"] = df.groupby("symbol")["close"].shift(1)

    # only keep <= ymd_effective
    df = df[df["ymd"].notna() & (df["ymd"] <= ymd_effective)]
    if df.empty:
        return {}, {}, {}, {}

    df = df.sort_values(["symbol", "ymd"], kind="mergesort")

    # name optional
    if "name" not in df.columns:
        df["name"] = ""

    # valid rows
    m = df["prev_close"].notna() & (df["prev_close"] > 0) & df["close"].notna()
    df = df[m].copy()
    if df.empty:
        return {}, {}, {}, {}

    # limit_rate + limit_price
    df["limit_rate"] = [
        _infer_cn_limit_rate(sym, nm) for sym, nm in zip(df["symbol"].tolist(), df["name"].tolist())
    ]
    df["limit_price"] = [
        _calc_limit_price(pc, lr) for pc, lr in zip(df["prev_close"].tolist(), df["limit_rate"].tolist())
    ]

    # locked / touch
    lp = pd.to_numeric(df["limit_price"], errors="coerce")
    df["is_locked"] = df["close"] >= (lp - float(eps_price))

    if "high" in df.columns:
        df["is_touch"] = df["high"].notna() & (df["high"] >= (lp - float(eps_price)))
    else:
        df["is_touch"] = False

    mode = _get_streak_mode()
    flag_col = "is_locked" if mode == "locked" else "is_touch"

    # per-symbol compute streaks
    streak_map: Dict[str, int] = {}
    streak_prev_map: Dict[str, int] = {}
    prev_locked_map: Dict[str, bool] = {}
    prev_touch_map: Dict[str, bool] = {}

    for sym, g in df.groupby("symbol", sort=False):
        flags = g[flag_col].tolist()
        locked_flags = g["is_locked"].tolist()
        touch_flags = g["is_touch"].tolist()
        ymds = g["ymd"].tolist()

        # must end at ymd_effective
        if not ymds:
            continue
        idx = len(ymds) - 1
        if ymds[idx] != ymd_effective:
            # 資料不齊：略過（避免把缺資料的 streak 算錯）
            continue

        # 今日 streak（依 mode）
        s = 0
        i = idx
        while i >= 0 and bool(flags[i]):
            s += 1
            i -= 1
        streak_map[sym] = s

        # 昨日 locked/touch（永遠用 locked_flags/touch_flags，和 mode 無關）
        if idx - 1 >= 0:
            prev_locked_map[sym] = bool(locked_flags[idx - 1])
            prev_touch_map[sym] = bool(touch_flags[idx - 1])

            # 昨日 streak（依 mode）
            sp = 0
            j = idx - 1
            while j >= 0 and bool(flags[j]):
                sp += 1
                j -= 1
            streak_prev_map[sym] = sp
        else:
            prev_locked_map[sym] = False
            prev_touch_map[sym] = False
            streak_prev_map[sym] = 0

    return streak_map, streak_prev_map, prev_locked_map, prev_touch_map


def apply_streaks_to_snapshot(
    snapshot_rows: Iterable[Dict[str, Any]],
    streak_map: Dict[str, int],
    streak_prev_map: Dict[str, int],
    prev_locked_map: Dict[str, bool],
    prev_touch_map: Dict[str, bool] | None = None,
) -> list[Dict[str, Any]]:
    """
    把 streak / streak_prev / prev_was_limitup_locked / prev_was_limitup_touch 塞回每一列。
    - prev_touch_map 可選（若上游 daily_df 沒 high，會全 False 或 None）
    """
    prev_touch_map = prev_touch_map or {}
    out: list[Dict[str, Any]] = []

    for r in snapshot_rows or []:
        rr = dict(r)
        sym = _safe_str(rr.get("symbol"))

        rr["streak"] = int(streak_map.get(sym, 0) or 0)
        rr["streak_prev"] = int(streak_prev_map.get(sym, 0) or 0)
        rr["prev_was_limitup_locked"] = bool(prev_locked_map.get(sym, False))
        rr["prev_was_limitup_touch"] = bool(prev_touch_map.get(sym, False))

        out.append(rr)

    return out
