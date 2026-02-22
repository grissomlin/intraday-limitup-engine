# scripts/render_images_common/overview/text.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties


def text_px(fig, renderer, text: str, fontprops: FontProperties, fontsize: float) -> float:
    t = plt.Text(0, 0, text, fontproperties=fontprops, fontsize=fontsize)
    t.set_figure(fig)
    bbox = t.get_window_extent(renderer=renderer)
    return float(bbox.width)


def ellipsize_to_px(fig, renderer, text: str, max_px: float, fontprops: FontProperties, fontsize: float) -> str:
    if not text:
        return ""
    if max_px <= 0:
        return "..."
    if text_px(fig, renderer, text, fontprops, fontsize) <= max_px:
        return text

    suffix = "..."
    base = text.strip()
    lo, hi = 0, len(base)
    best = suffix

    while lo <= hi:
        mid = (lo + hi) // 2
        cand = base[:mid].rstrip() + suffix
        if text_px(fig, renderer, cand, fontprops, fontsize) <= max_px:
            best = cand
            lo = mid + 1
        else:
            hi = mid - 1

    return best


def safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""
