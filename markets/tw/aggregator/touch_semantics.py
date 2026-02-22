# markets/tw/aggregator/touch_semantics.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


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


def fix_touch_double_count_for_overview_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    CRITICAL FIX (TW):
    很多 builder 的 touch_cnt 是 touch_total（含 locked）。
    若 renderer 用 locked_cnt + touch_cnt => locked 會被 double count。

    做法（對 renderer-facing rows 一律安全）：
      - 保留原 touch total 到 touch_cnt_total
      - 計算 touch_only_cnt = max(touch_cnt_total - locked_cnt, 0)
      - 覆寫 touch_cnt = touch_only_cnt（相容舊 renderer）
    """
    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r or {})
        locked = _safe_int(rr.get("locked_cnt"), 0)
        touch_total = _safe_int(rr.get("touch_cnt"), 0)
        touch_only = max(touch_total - locked, 0)

        rr["touch_cnt_total"] = touch_total
        rr["touch_only_cnt"] = touch_only
        rr["touch_cnt"] = touch_only
        out.append(rr)

    return out


__all__ = [
    "fix_touch_double_count_for_overview_rows",
]
