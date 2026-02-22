# markets/tw/rules.py
# -*- coding: utf-8 -*-

"""
TW 漲停規則（通用：日K/盤中都能用）
- 輕量、無副作用、易測試
- 預設：一般股票 10% 漲停
"""

from __future__ import annotations
from typing import Dict, Optional


# -----------------------------
# Tick size（台股常見跳動）
# -----------------------------
def get_tick_size(price: float) -> float:
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.1
    if price < 500:
        return 0.5
    if price < 1000:
        return 1.0
    return 5.0


def round_to_tick(price: float) -> float:
    """四捨五入到 tick（一般用於把資料對齊報價格）"""
    tick = get_tick_size(price)
    return round(price / tick) * tick


def floor_to_tick(price: float) -> float:
    """✅ 保守：向下取到 tick（用於漲停價計算更穩）"""
    tick = get_tick_size(price)
    return (price // tick) * tick


# -----------------------------
# 漲停價計算
# -----------------------------
def calc_limitup_price(prev_close: float, up_rate: float = 0.10) -> float:
    """
    ✅ 建議用 floor_to_tick：
    - 避免因 round() 造成漲停價被算高半格，導致誤判「沒鎖」
    """
    if prev_close is None or prev_close <= 0:
        raise ValueError("prev_close must be > 0")

    raw = prev_close * (1.0 + up_rate)
    return float(floor_to_tick(raw))


# -----------------------------
# 觸及 / 鎖住
# -----------------------------
def is_limitup_touch(high_price: Optional[float], limitup_price: float) -> bool:
    if high_price is None or limitup_price is None:
        return False
    return float(high_price) >= float(limitup_price)


def is_limitup_locked(last_price: Optional[float], limitup_price: float) -> bool:
    """
    ✅ 先把 last round 到 tick 再比，最穩
    """
    if last_price is None or limitup_price is None:
        return False
    lp = float(round_to_tick(float(last_price)))
    lu = float(limitup_price)
    tick = get_tick_size(lu)
    return abs(lp - lu) <= (tick / 2.0)


def is_limitup_locked_overshoot(last_price: Optional[float], limitup_price: float) -> bool:
    """
    ✅ 寬鬆版：允許資料源出現「超過漲停價」(理論上制度內不會發生)
    - 只要 last >= limitup_price（含 tick/2 容忍）就視為 locked
    - 建議只在 TW standard + yfinance 這種資料源下開啟，用來修正 streak/狀態
    """
    if last_price is None or limitup_price is None:
        return False
    lu = float(limitup_price)
    tick = get_tick_size(lu)
    return float(last_price) >= (lu - tick / 2.0)


# -----------------------------
# 統一輸出（給 downloader 用）
# -----------------------------
def summarize_tick(
    *,
    symbol: str,
    prev_close: float,
    last_price: float,
    high_price: float,
    up_rate: float = 0.10,
    extra: Optional[Dict] = None,
) -> Dict:
    limitup_price = calc_limitup_price(prev_close, up_rate=up_rate)

    out = {
        "symbol": symbol,
        "prev_close": float(prev_close),
        "limitup_price": float(limitup_price),
        "last_price": float(last_price),
        "high_price": float(high_price),
        "is_limitup_touch": is_limitup_touch(high_price, limitup_price),
        "is_limitup_locked": is_limitup_locked(last_price, limitup_price),
    }

    if extra:
        out.update(extra)

    return out
