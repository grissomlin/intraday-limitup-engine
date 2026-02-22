# scripts/render_images_tw/utils_tw.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from typing import Any, List


def pct(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def to_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def to_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def norm_sector(x: Any) -> str:
    s = safe_str(x)
    if not s or s.lower() in ("nan", "none") or s in ("—", "-", "--", "－", "–"):
        return "未分類"
    return s


def sanitize_filename(s: str) -> str:
    s = safe_str(s)
    if not s:
        return "unknown"
    s = s.replace(" ", "_")
    s = re.sub(r"[^\w\u4e00-\u9fff\u3400-\u4dbf\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    n = max(1, int(n))
    return [lst[i : i + n] for i in range(0, len(lst), n)]
