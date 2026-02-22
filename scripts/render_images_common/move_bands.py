# scripts/render_images_common/move_bands.py
# -*- coding: utf-8 -*-
"""
Move band helper (shared by US/UK/CA/DE/AU...).

Band definition (by daily return ret):
- 0: 10%  ≤ ret < 20%
- 1: 20%  ≤ ret < 30%
- 2: 30%  ≤ ret < 40%
- 3: 40%  ≤ ret < 50%
- 4: 50%  ≤ ret < 100%
- 5: ret ≥ 100%

This module intentionally does NOT translate text.
Render layer should map band -> i18n key: "move_band_{band}".
"""

from __future__ import annotations

from typing import Optional, Tuple


def move_band(ret: float) -> int:
    """Return band int in [0..5], or -1 if ret < 10%."""
    if ret is None:
        return -1
    try:
        r = float(ret)
    except Exception:
        return -1

    if r >= 1.00:
        return 5
    if r >= 0.50:
        return 4
    if r >= 0.40:
        return 3
    if r >= 0.30:
        return 2
    if r >= 0.20:
        return 1
    if r >= 0.10:
        return 0
    return -1


def move_key(band: int) -> str:
    """Return i18n key for a band."""
    if band is None:
        return ""
    try:
        b = int(band)
    except Exception:
        return ""
    if 0 <= b <= 5:
        return f"move_band_{b}"
    return ""


def move_badge(ret: float) -> Tuple[int, str]:
    """Convenience: (band, key)."""
    b = move_band(ret)
    return b, move_key(b)
