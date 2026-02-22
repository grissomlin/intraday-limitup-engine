# markets/jp/jp_limit_rules.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JPLimitResult:
    last_close: float
    limit_amount: float
    limit_price: float
    limit_pct: float  # (limit_price/last_close - 1)


def jp_limit_amount(last_close: float) -> float:
    """
    TSE/JPX price limit (upper) by last_close (base price).
    This is the standard tiered "値幅制限" table used for most stocks.

    NOTE:
    - This function returns the *yen amount* (upper limit move).
    - There are rare special cases (expanded limits / special measures) not covered here.
    """
    p = float(last_close)

    # guard
    if p <= 0:
        return 0.0

    # Tiers (base price -> limit amount in JPY)
    # Using "< next_threshold" style makes boundaries unambiguous.
    if p < 100:
        return 30
    if p < 200:
        return 50
    if p < 500:
        return 80
    if p < 700:
        return 100
    if p < 1000:
        return 150
    if p < 1500:
        return 300
    if p < 2000:
        return 400
    if p < 3000:
        return 500
    if p < 5000:
        return 700
    if p < 7000:
        return 1000
    if p < 10000:
        return 1500
    if p < 15000:
        return 3000
    if p < 20000:
        return 4000
    if p < 30000:
        return 5000
    if p < 50000:
        return 7000
    if p < 70000:
        return 10000
    if p < 100000:
        return 15000
    if p < 150000:
        return 30000
    if p < 200000:
        return 40000
    if p < 300000:
        return 50000
    if p < 500000:
        return 70000
    if p < 700000:
        return 100000
    if p < 1000000:
        return 150000
    if p < 1500000:
        return 300000

    # If you need higher tiers later, extend here.
    # For now, cap at 300,000 for >= 1,500,000 JPY base.
    return 300000


def jp_calc_limit(last_close: float) -> JPLimitResult:
    lc = float(last_close) if last_close is not None else 0.0
    amt = float(jp_limit_amount(lc))
    lp = lc + amt
    pct = (lp / lc - 1.0) if lc > 0 else 0.0
    return JPLimitResult(last_close=lc, limit_amount=amt, limit_price=lp, limit_pct=float(pct))


def is_true_limitup(close: float, last_close: float, *, eps: float = 1e-6) -> bool:
    if last_close is None or float(last_close) <= 0 or close is None:
        return False
    res = jp_calc_limit(float(last_close))
    return float(close) >= float(res.limit_price) - eps
