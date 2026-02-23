# scripts/render_images_common/overview/text.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Optional

import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties


def _ensure_fp(fp: Optional[FontProperties]) -> FontProperties:
    """
    Ensure we ALWAYS have a FontProperties object for measurement.
    If caller passes None, fall back to current rcParams sans-serif list
    so bbox measurement matches actual render fallback behavior.
    """
    if isinstance(fp, FontProperties):
        return fp

    fam = plt.rcParams.get("font.sans-serif") or ["sans-serif"]
    # FontProperties can take a list as family; matplotlib will fallback in order.
    return FontProperties(family=fam)


def text_px(
    fig,
    renderer,
    text: str,
    fontprops: Optional[FontProperties],
    fontsize: float,
) -> float:
    """
    Measure text width in pixels using the SAME FontProperties as actual drawing.

    Notes:
    - We intentionally use plt.Text (not ax.text) to avoid leaving artists on axes.
    - The key is: pass `fontproperties=fontprops` so bbox measurement matches render font.
    - When fontprops is None, we use rcParams['font.sans-serif'] to avoid DejaVu-only drift.
    """
    fp = _ensure_fp(fontprops)
    t = plt.Text(0, 0, text, fontproperties=fp, fontsize=fontsize)
    t.set_figure(fig)
    bbox = t.get_window_extent(renderer=renderer)
    return float(bbox.width)


def ellipsize_to_px(
    fig,
    renderer,
    text: str,
    max_px: float,
    fontprops: Optional[FontProperties],
    fontsize: float,
) -> str:
    """
    Ellipsize `text` so its measured width <= max_px, using the SAME FontProperties.
    """
    if not text:
        return ""
    if max_px <= 0:
        return "..."

    fp = _ensure_fp(fontprops)

    if text_px(fig, renderer, text, fp, fontsize) <= max_px:
        return text

    suffix = "..."
    base = text.strip()
    lo, hi = 0, len(base)
    best = suffix

    while lo <= hi:
        mid = (lo + hi) // 2
        cand = base[:mid].rstrip() + suffix
        if text_px(fig, renderer, cand, fp, fontsize) <= max_px:
            best = cand
            lo = mid + 1
        else:
            hi = mid - 1

    return best


def safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""
