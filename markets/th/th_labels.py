# markets/th/th_labels.py
# -*- coding: utf-8 -*-
from __future__ import annotations


def surge_label(ret: float) -> str:
    """
    TH Big Mover labels (bin-style, US-like)

      +10–20%   พุ่งแรง
      +20–30%   พุ่งจัด
      +30–40%   พุ่งเดือด
      +40–50%   พุ่งผิดปกติ
      +50–100%  พุ่งสุดๆ
      ≥100%     เท่าตัว
    """
    try:
        r = float(ret)
    except Exception:
        r = 0.0

    if r >= 1.00:
        return "เท่าตัว"
    if r >= 0.50:
        return "พุ่งสุดๆ"
    if r >= 0.40:
        return "พุ่งผิดปกติ"
    if r >= 0.30:
        return "พุ่งเดือด"
    if r >= 0.20:
        return "พุ่งจัด"
    return "พุ่งแรง"
