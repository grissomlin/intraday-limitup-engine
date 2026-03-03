# scripts/render_images_common/mpl_pills.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import matplotlib.axes
import matplotlib.figure


def limit_pct_from_row(row: Dict[str, Any], default_pct: float = 10.0) -> float:
    v = row.get("limit_rate", None)
    if v is None:
        v = row.get("band_pct", None)
    if v is None:
        return float(default_pct)

    try:
        x = float(v)
    except Exception:
        return float(default_pct)

    pct = x * 100.0 if 0 < abs(x) <= 1.5 else x
    if pct <= 0:
        return float(default_pct)
    return float(pct)


def limit_label(pct: float) -> str:
    p = float(pct or 10.0)
    if abs(p - round(p)) < 0.05:
        return f"Limit {int(round(p))}%"
    return f"Limit {p:.1f}%"


def limit_colors(limit_pct: float, theme: str) -> Tuple[str, str]:
    theme = (theme or "dark").strip().lower()
    if theme == "dark":
        if limit_pct >= 20:
            return ("#ff922b", "#111111")
        if limit_pct >= 10:
            return ("#74c0fc", "#111111")
        return ("#adb5bd", "#111111")
    else:
        if limit_pct >= 20:
            return ("#ffd8a8", "#7c2d12")
        if limit_pct >= 10:
            return ("#d0ebff", "#0b7285")
        return ("#f1f3f5", "#343a40")


def draw_pill(
    ax: matplotlib.axes.Axes,
    *,
    x: float,
    y: float,
    text: str,
    fg: str,
    bg: str,
    fontsize: int,
    pad: float = 0.14,
    alpha: float = 0.95,
) -> None:
    if not text:
        return
    ax.text(
        x, y, text,
        ha="left", va="center",
        fontsize=fontsize, color=fg, weight="bold",
        bbox=dict(boxstyle=f"round,pad={pad}", facecolor=bg, edgecolor="none", alpha=alpha),
    )


def draw_pill_after_text(
    ax: matplotlib.axes.Axes,
    fig: matplotlib.figure.Figure,
    *,
    text_x: float,
    text_y: float,
    text_str: str,
    text_fontsize: int,
    pill_text: str,
    pill_fontsize: int,
    pill_fg: str,
    pill_bg: str,
    x_right_limit: float,
    gap_px: float,
    measure_text_width_px_fn,   # callable(ax, fig, s, x, y, fontsize, weight) -> px
    px_to_data_dx_fn,           # callable(ax, px, y_data=...) -> dx
    pad: float = 0.14,
    try_font_deltas: Tuple[int, ...] = (0, -2, -4, -6),
    fallback_y: Optional[float] = None,
) -> bool:
    if not pill_text:
        return False

    w1_px = measure_text_width_px_fn(ax, fig, text_str, x=text_x, y=text_y, fontsize=text_fontsize, weight="bold")
    x0 = text_x + px_to_data_dx_fn(ax, w1_px + gap_px, y_data=text_y)

    if x0 >= x_right_limit:
        if fallback_y is not None and fallback_y != text_y:
            return draw_pill_after_text(
                ax, fig,
                text_x=text_x, text_y=fallback_y, text_str=text_str, text_fontsize=text_fontsize,
                pill_text=pill_text, pill_fontsize=pill_fontsize,
                pill_fg=pill_fg, pill_bg=pill_bg,
                x_right_limit=x_right_limit, gap_px=gap_px,
                measure_text_width_px_fn=measure_text_width_px_fn,
                px_to_data_dx_fn=px_to_data_dx_fn,
                pad=pad, try_font_deltas=try_font_deltas, fallback_y=None,
            )
        return False

    for d in try_font_deltas:
        fs = max(14, int(pill_fontsize + d))
        w_px = measure_text_width_px_fn(ax, fig, pill_text, x=x0, y=text_y, fontsize=fs, weight="bold") + 22.0
        if x0 + px_to_data_dx_fn(ax, w_px, y_data=text_y) <= x_right_limit:
            draw_pill(ax, x=x0, y=text_y, text=pill_text, fg=pill_fg, bg=pill_bg, fontsize=fs, pad=pad)
            return True

    if fallback_y is not None and fallback_y != text_y:
        return draw_pill_after_text(
            ax, fig,
            text_x=text_x, text_y=fallback_y, text_str=text_str, text_fontsize=text_fontsize,
            pill_text=pill_text, pill_fontsize=pill_fontsize,
            pill_fg=pill_fg, pill_bg=pill_bg,
            x_right_limit=x_right_limit, gap_px=gap_px,
            measure_text_width_px_fn=measure_text_width_px_fn,
            px_to_data_dx_fn=px_to_data_dx_fn,
            pad=pad, try_font_deltas=try_font_deltas, fallback_y=None,
        )

    return False


def draw_limit_pill_after_name(
    ax: matplotlib.axes.Axes,
    fig: matplotlib.figure.Figure,
    *,
    name_x: float,
    name_y: float,
    display_name: str,
    name_fontsize: int,
    row: Dict[str, Any],
    theme: str,
    x_right_limit: float,
    pill_fontsize: int,
    gap_px: float,
    measure_text_width_px_fn,
    px_to_data_dx_fn,
    pad: float = 0.14,
    fallback_y: Optional[float] = None,
) -> bool:
    pct = limit_pct_from_row(row, default_pct=10.0)
    pill_text = limit_label(pct)
    pill_bg, pill_fg = limit_colors(pct, theme)

    return draw_pill_after_text(
        ax, fig,
        text_x=name_x, text_y=name_y, text_str=display_name, text_fontsize=name_fontsize,
        pill_text=pill_text, pill_fontsize=pill_fontsize,
        pill_fg=pill_fg, pill_bg=pill_bg,
        x_right_limit=x_right_limit, gap_px=gap_px,
        measure_text_width_px_fn=measure_text_width_px_fn,
        px_to_data_dx_fn=px_to_data_dx_fn,
        pad=pad,
        fallback_y=fallback_y,
    )
