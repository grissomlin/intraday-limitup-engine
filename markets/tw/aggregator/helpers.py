# markets/tw/aggregator/helpers.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from typing import Any

import pandas as pd


EPS = 1e-6


def norm_ymd(x: Any) -> str:
    s = str(x or "").strip()
    return s[:10] if len(s) >= 10 else s


def safe_int(x: Any, default: int = 0) -> int:
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


def sanitize_nan(obj: Any) -> Any:
    """Make payload JSON-safe: convert NaN/Inf/pd.NA to None recursively."""
    try:
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
    except Exception:
        pass

    try:
        if obj is pd.NA:
            return None
    except Exception:
        pass

    if obj is None:
        return None

    if isinstance(obj, dict):
        return {k: sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_nan(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(sanitize_nan(v) for v in obj)
    return obj
