# markets/jp/jp_labels.py
# -*- coding: utf-8 -*-
from __future__ import annotations


def surge_label(ret: float) -> str:
    """
    JP Big Mover labels (bin-style, US-like)

      +10–20%   大幅高
      +20–30%   急騰
      +30–40%   爆騰
      +40–50%   異常高
      +50–100%  超急騰
      ≥100%     倍増
    """
    try:
        r = float(ret)
    except Exception:
        r = 0.0

    if r >= 1.00:
        return "倍増"
    if r >= 0.50:
        return "超急騰"
    if r >= 0.40:
        return "異常高"
    if r >= 0.30:
        return "爆騰"
    if r >= 0.20:
        return "急騰"
    return "大幅高"
