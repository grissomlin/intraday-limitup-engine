# scripts/render_images_in/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from .layout import LayoutSpec, calc_rows_layout

from scripts.render_images_common.mpl_text import (
    ensure_renderer,
    text_width_px,
    px_to_data_dx,
)
from scripts.render_images_common.mpl_pills import (
    limit_pct_from_row,
    limit_label,
    limit_colors,
    draw_pill_after_text,
)

# =============================================================================
# Font
# =============================================================================
def setup_font() -> str | None:
    try:
        font_candidates = [
            "Inter", "Segoe UI", "Arial",
            "Noto Sans", "Noto Sans CJK SC",
            "Noto Sans CJK TC", "Noto Sans CJK JP",
            "Microsoft YaHei", "Microsoft JhengHei",
            "PingFang SC", "PingFang TC",
            "WenQuanYi Zen Hei", "Arial Unicode MS",
        ]
        available = {f.name for f in fm.fontManager.ttflist}
        for f in font_candidates:
            if f in available:
                plt.rcParams["font.sans-serif"] = [f]
                plt.rcParams["axes.unicode_minus"] = False
                return f
    except Exception:
        pass
    return None


# =============================================================================
# Utils
# =============================================================================
def _safe_str(x: Any) -> str:
    try:
        return str(x).strip() if x is not None else ""
    except Exception:
        return ""


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _ellipsize(s: str, max_chars: int) -> str:
    s = _safe_str(s)
    if max_chars <= 0:
        return ""
    return s if len(s) <= max_chars else (s[: max_chars - 1] + "…")


def _fmt_ret_pct(ret: float) -> str:
    r = _safe_float(ret, 0.0)
    pct = (r * 100.0) if abs(r) < 1.5 else r
    return f"{pct:+.2f}%"


def get_ret_color(ret: float, theme: str) -> str:
    if (theme or "dark").strip().lower() == "dark":
        return "#ff6b6b" if ret >= 0 else "#4dabf7"
    return "#d9480f" if ret >= 0 else "#1864ab"


# =============================================================================
# Draw
# =============================================================================
def draw_block_table(
    out_path: Path,
    *,
    layout: LayoutSpec,
    sector: str,
    cutoff: str,
    locked_cnt: int,
    touch_cnt: int,
    theme_cnt: int,
    hit_shown: Optional[int] = None,
    hit_total: Optional[int] = None,
    touch_shown: Optional[int] = None,
    touch_total: Optional[int] = None,
    big_shown: Optional[int] = None,
    big_total: Optional[int] = None,
    sector_shown_total: Optional[int] = None,
    sector_all_total: Optional[int] = None,
    limitup_rows: List[Dict[str, Any]] | None = None,
    peer_rows: List[Dict[str, Any]] | None = None,
    page_idx: int = 1,
    page_total: int = 1,
    width: int = 1080,
    height: int = 1920,
    rows_per_page: int = 6,
    theme: str = "dark",
    time_note: str = "",
    has_more_peers: bool = False,
    lang: str = "en",
    market: str = "IN",
):
    setup_font()

    theme = (theme or "dark").lower()
    is_dark = theme == "dark"

    bg = "#0b0d10" if is_dark else "#ffffff"
    fg = "#f1f3f5" if is_dark else "#111111"
    sub = "#adb5bd" if is_dark else "#555555"
    box = "#14171c" if is_dark else "#f6f8fa"
    line = "#343a40" if is_dark else "#d0d7de"
    divider = "#2b2f36" if is_dark else "#e1e5ea"

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_axis_off()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    # layout geometry
    top_y0 = getattr(layout, "top_box_y0", 0.84)
    top_y1 = getattr(layout, "top_box_y1", 0.485)
    bot_y0 = getattr(layout, "bot_box_y0", 0.465)
    bot_y1 = getattr(layout, "bot_box_y1", 0.085)

    ax.add_patch(plt.Rectangle((0.05, top_y1), 0.90, top_y0 - top_y1, facecolor=box, edgecolor=line, linewidth=2))
    ax.add_patch(plt.Rectangle((0.05, bot_y1), 0.90, bot_y0 - bot_y1, facecolor=box, edgecolor=line, linewidth=2))

    # header
    ax.text(0.5, 0.97, _ellipsize(sector, 22), ha="center", va="top",
            fontsize=int(getattr(layout, "title_fs", 62)),
            color=fg, weight="bold")

    if time_note:
        lines = [x.strip() for x in time_note.split("\n") if x.strip()]
        ax.text(0.5, 0.91, lines[0], ha="center", va="top",
                fontsize=int(getattr(layout, "subtitle_fs", 30)),
                color=sub, weight="bold")
        if len(lines) > 1:
            ax.text(0.5, 0.88, lines[1], ha="center", va="top",
                    fontsize=int(getattr(layout, "subtitle_fs", 30)) - 2,
                    color=sub, weight="bold")

    ensure_renderer(fig)

    # rows layout
    two_line = True
    y_start_top, row_h_top = calc_rows_layout(top_y0 - 0.04, top_y1, rows_per_page, two_line=two_line)
    y_start_bot, row_h_bot = calc_rows_layout(bot_y0 - 0.04, bot_y1, rows_per_page + 1, two_line=two_line)

    x_name = 0.08
    x_tag = 0.94

    # =======================
    # TOP rows
    # =======================
    for i, r in enumerate(limitup_rows or []):
        if i >= rows_per_page:
            break

        y_center = y_start_top - i * row_h_top
        y1 = y_center + row_h_top * 0.22
        y2 = y_center - row_h_top * 0.22

        display_line1 = _ellipsize(_safe_str(r.get("line1")), 26)

        ax.text(x_name, y1, display_line1,
                ha="left", va="center",
                fontsize=int(getattr(layout, "row_name_fs", 28)),
                color=fg, weight="bold")

        # ===== LIMIT PILL =====
        pct = limit_pct_from_row(r, default_pct=10.0)
        pill_text = limit_label(pct)
        pill_bg, pill_fg = limit_colors(pct, theme)

        x_right_limit = x_tag - px_to_data_dx(
            ax,
            180,
            y_data=y1,
        )

        draw_pill_after_text(
            ax, fig,
            text_x=x_name,
            text_y=y1,
            text_str=display_line1,
            text_fontsize=int(getattr(layout, "row_name_fs", 28)),
            pill_text=pill_text,
            pill_fontsize=int(getattr(layout, "row_name_fs", 28)),
            pill_fg=pill_fg,
            pill_bg=pill_bg,
            x_right_limit=x_right_limit,
            gap_px=10,
            measure_text_width_px_fn=text_width_px,
            px_to_data_dx_fn=px_to_data_dx,
        )

        ret = _safe_float(r.get("ret"), 0.0)
        ret_text = _fmt_ret_pct(ret)

        x_ret = x_tag - px_to_data_dx(ax, 18, y_data=y2)
        ax.text(x_ret, y2, ret_text,
                ha="right", va="center",
                fontsize=24,
                color=get_ret_color(ret, theme),
                weight="bold")

    # =======================
    # BOTTOM rows (peer)
    # =======================
    for i, r in enumerate(peer_rows or []):
        if i >= rows_per_page + 1:
            break

        y_center = y_start_bot - i * row_h_bot
        y1 = y_center + row_h_bot * 0.22
        y2 = y_center - row_h_bot * 0.22

        display_line1 = _ellipsize(_safe_str(r.get("line1")), 26)

        ax.text(x_name, y1, display_line1,
                ha="left", va="center",
                fontsize=int(getattr(layout, "row_name_fs", 28)),
                color=fg, weight="bold")

        ret = _safe_float(r.get("ret"), 0.0)
        ret_text = _fmt_ret_pct(ret)

        x_ret = x_tag - px_to_data_dx(ax, 18, y_data=y1)
        ax.text(x_ret, y1, ret_text,
                ha="right", va="center",
                fontsize=24,
                color=get_ret_color(ret, theme),
                weight="bold")

        # ===== peer limit pill =====
        pct = limit_pct_from_row(r, default_pct=10.0)
        pill_text = limit_label(pct)
        pill_bg, pill_fg = limit_colors(pct, theme)

        ret_w_px = text_width_px(
            ax, fig,
            ret_text,
            x=x_ret,
            y=y1,
            fontsize=24,
            weight="bold",
        )

        x_ret_left = x_ret - px_to_data_dx(ax, ret_w_px + 10, y_data=y1)

        draw_pill_after_text(
            ax, fig,
            text_x=x_name,
            text_y=y1,
            text_str=display_line1,
            text_fontsize=int(getattr(layout, "row_name_fs", 28)),
            pill_text=pill_text,
            pill_fontsize=26,
            pill_fg=pill_fg,
            pill_bg=pill_bg,
            x_right_limit=x_ret_left,
            gap_px=10,
            measure_text_width_px_fn=text_width_px,
            px_to_data_dx_fn=px_to_data_dx,
            fallback_y=y2,
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100)
    plt.close(fig)
    return out_path
