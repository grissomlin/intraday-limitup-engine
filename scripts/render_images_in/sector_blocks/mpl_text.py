# scripts/render_images_common/mpl_text.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional

import matplotlib.axes
import matplotlib.figure


def ensure_renderer(fig: matplotlib.figure.Figure) -> None:
    try:
        fig.canvas.draw()
    except Exception:
        pass


def text_width_px(
    ax: matplotlib.axes.Axes,
    fig: matplotlib.figure.Figure,
    s: str,
    *,
    x: float,
    y: float,
    fontsize: int,
    weight: str = "normal",
) -> float:
    """
    Measure text width in pixels using a hidden text artist.
    """
    t_obj = ax.text(x, y, s, fontsize=fontsize, weight=weight, alpha=0.0)
    ensure_renderer(fig)
    bb = t_obj.get_window_extent(renderer=fig.canvas.get_renderer())
    t_obj.remove()
    return float(bb.width)


def px_to_data_dx(ax: matplotlib.axes.Axes, px: float, *, y_data: float) -> float:
    """
    Convert a horizontal pixel delta into data coords delta at a given y (data coords).
    """
    p0 = ax.transData.transform((0.0, y_data))
    p1 = (p0[0] + px, p0[1])
    inv = ax.transData.inverted()
    return float(inv.transform(p1)[0] - inv.transform(p0)[0])


def fit_ellipsis_by_px(
    ax: matplotlib.axes.Axes,
    fig: matplotlib.figure.Figure,
    s: str,
    *,
    x: float,
    y: float,
    fontsize: int,
    max_px: float,
    weight: str = "bold",
    ellipsis: str = "…",
    min_chars: int = 1,
) -> str:
    """
    Fit string into max_px by truncating and adding ellipsis.
    Uses binary search on char length (fast enough for per-row usage).
    """
    s = (s or "").strip()
    if max_px <= 0:
        return ""
    if not s:
        return ""

    w = text_width_px(ax, fig, s, x=x, y=y, fontsize=fontsize, weight=weight)
    if w <= max_px:
        return s

    # if even ellipsis doesn't fit, return empty
    w_e = text_width_px(ax, fig, ellipsis, x=x, y=y, fontsize=fontsize, weight=weight)
    if w_e > max_px:
        return ""

    lo = min_chars
    hi = len(s)
    best = ""

    while lo <= hi:
        mid = (lo + hi) // 2
        cand = s[:mid] + ellipsis
        w_c = text_width_px(ax, fig, cand, x=x, y=y, fontsize=fontsize, weight=weight)
        if w_c <= max_px:
            best = cand
            lo = mid + 1
        else:
            hi = mid - 1

    return best if best else ellipsis
