# scripts/render_images_in/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from .layout import LayoutSpec, calc_rows_layout

# =============================================================================
# i18n (optional)
# =============================================================================
try:
    from scripts.render_images_common.i18n import t as _i18n_t  # type: ignore
except Exception:
    _i18n_t = None  # type: ignore


def _t(lang: str, key: str, default: str, **kwargs: Any) -> str:
    if _i18n_t is None:
        try:
            return default.format(**kwargs)
        except Exception:
            return default
    try:
        return _i18n_t(lang, key, default=default, **kwargs)
    except Exception:
        try:
            return default.format(**kwargs)
        except Exception:
            return default


# =============================================================================
# Font
# =============================================================================
def setup_font() -> str | None:
    """
    India page default lang may be English, but we still want a font that can render
    symbols and potential CJK if needed.
    """
    try:
        font_candidates = [
            "Inter",
            "Segoe UI",
            "Arial",
            "Noto Sans",
            "Noto Sans CJK SC",
            "Noto Sans CJK TC",
            "Noto Sans CJK JP",
            "Microsoft YaHei",
            "Microsoft JhengHei",
            "PingFang SC",
            "PingFang TC",
            "WenQuanYi Zen Hei",
            "Arial Unicode MS",
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


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
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


def _count_hit_touch_big(rows: List[Dict[str, Any]] | None) -> Tuple[int, int, int]:
    hit = 0
    touch = 0
    big = 0
    for r in (rows or []):
        st = _safe_str(r.get("limitup_status") or "").lower()
        if st in ("touch", "bomb"):
            touch += 1
        elif st == "big":
            big += 1
        else:
            hit += 1
    return hit, touch, big


# =============================================================================
# India: limit band pill helper
# =============================================================================
def _limit_pct_from_row(row: Dict[str, Any]) -> float:
    """
    India rows may have:
      - limit_rate: ratio (0.20)
      - band_pct:   ratio (0.20)
      - sometimes already percent (20)
    """
    v = row.get("limit_rate", None)
    if v is None:
        v = row.get("band_pct", None)
    if v is None:
        return 0.0
    try:
        x = float(v)
    except Exception:
        return 0.0

    if abs(x) <= 1.5:
        return x * 100.0
    return x


def _limit_colors(limit_pct: float, theme: str) -> Tuple[str, str]:
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


def get_ret_color(ret: float, theme: str) -> str:
    # keep CN convention? For India, green/red is common, but don't change now.
    # We'll use: up = red-ish, down = blue-ish consistent with your other pages.
    if theme == "dark":
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
    cutoff: str,  # kept for compat; not used here
    locked_cnt: int,   # backward-compat (unused)
    touch_cnt: int,    # backward-compat (unused)
    theme_cnt: int,    # backward-compat (unused)
    big_cnt: int = 0,  # backward-compat (unused)
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
    theme = (theme or "dark").strip().lower()
    is_dark = theme == "dark"

    # i18n labels
    big_label = _t(lang, "term_bigmove10", "Big")
    locked_label = _t(lang, "term_limitup", "Locked")
    touched_label = _t(lang, "term_touched", "Touch")

    # Colors
    if is_dark:
        bg = "#0b0d10"
        fg = "#f1f3f5"
        sub = "#adb5bd"
        line = "#343a40"
        box = "#14171c"
        divider = "#2b2f36"

        badge_red = "#fa5252"
        limitup_pill_fg = "#ffffff"

        touch_pill_bg = "#845ef7"
        touch_pill_fg = "#ffffff"

        big_pill_bg = "#f59f00"
        big_pill_fg = "#ffffff"

        shadow = "#000000"
    else:
        bg = "#ffffff"
        fg = "#111111"
        sub = "#555555"
        line = "#d0d7de"
        box = "#f6f8fa"
        divider = "#e1e5ea"

        badge_red = "#ff6b6b"
        limitup_pill_fg = "#111111"

        touch_pill_bg = "#845ef7"
        touch_pill_fg = "#ffffff"

        big_pill_bg = "#f59f00"
        big_pill_fg = "#ffffff"

        shadow = "#000000"

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_axis_off()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    def _ensure_renderer():
        try:
            fig.canvas.draw()
        except Exception:
            pass

    def _text_width_px(s: str, x: float, y: float, fontsize: int, weight: str = "normal") -> float:
        t_obj = ax.text(x, y, s, fontsize=fontsize, weight=weight, alpha=0.0)
        _ensure_renderer()
        bb = t_obj.get_window_extent(renderer=fig.canvas.get_renderer())
        t_obj.remove()
        return float(bb.width)

    def _px_to_data_dx(px: float, y_data: float) -> float:
        p0 = ax.transData.transform((0.0, y_data))
        p1 = (p0[0] + px, p0[1])
        inv = ax.transData.inverted()
        return float(inv.transform(p1)[0] - inv.transform(p0)[0])

    def _shadow_text(
        x: float,
        y: float,
        s: str,
        *,
        fontsize: int,
        color: str,
        ha: str,
        va: str,
        weight: str = "bold",
        alpha: float = 1.0,
        shadow_alpha: float = 0.55,
        dx: float = 0.0015,
        dy: float = -0.0015,
    ):
        ax.text(x + dx, y + dy, s, fontsize=fontsize, color=shadow, ha=ha, va=va, weight=weight, alpha=shadow_alpha)
        ax.text(x, y, s, fontsize=fontsize, color=color, ha=ha, va=va, weight=weight, alpha=alpha)

    # Box geometry (DO NOT CHANGE)
    top_y0 = getattr(layout, "top_box_y0", 0.84)
    top_y1 = getattr(layout, "top_box_y1", 0.485)
    bot_y0 = getattr(layout, "bot_box_y0", 0.465)
    bot_y1 = getattr(layout, "bot_box_y1", 0.085)

    ax.add_patch(
        plt.Rectangle((0.05, top_y1), 0.90, (top_y0 - top_y1), facecolor=box, edgecolor=line, linewidth=2, alpha=0.98)
    )
    ax.add_patch(
        plt.Rectangle((0.05, bot_y1), 0.90, (bot_y0 - bot_y1), facecolor=box, edgecolor=line, linewidth=2, alpha=0.98)
    )

    # Header
    title_fs = int(getattr(layout, "title_fs", 62))
    subtitle_fs = int(getattr(layout, "subtitle_fs", 30))
    page_fs = int(getattr(layout, "page_fs", 26))
    footer_fs = int(getattr(layout, "footer_fs_2", 20))

    header_title_y = float(getattr(layout, "header_title_y", 0.972))
    header_subtitle_y = float(getattr(layout, "header_subtitle_y", 0.910))

    ax.text(
        0.5,
        header_title_y,
        _ellipsize(_safe_str(sector), 22),
        ha="center",
        va="top",
        fontsize=title_fs,
        color=fg,
        weight="bold",
    )

    if time_note:
        ax.text(
            0.5,
            header_subtitle_y,
            _safe_str(time_note),
            ha="center",
            va="top",
            fontsize=subtitle_fs,
            color=sub,
            weight="bold",
            alpha=0.92,
        )

    if page_total > 1:
        ax.text(
            0.97,
            header_title_y,
            f"{page_idx}/{page_total}",
            ha="right",
            va="top",
            fontsize=page_fs,
            color=sub,
            weight="bold",
            alpha=0.90,
        )

    footer_text = _t(lang, "footer_disclaimer", "Data source: public market info | Not investment advice")
    ax.text(
        0.05,
        float(getattr(layout, "footer_y2", 0.020)),
        footer_text,
        ha="left",
        va="bottom",
        fontsize=footer_fs,
        color="#FFD54A",
        alpha=0.70,
    )

    # Compute counts for title
    if hit_shown is None or hit_total is None or touch_shown is None or touch_total is None:
        hs, ts, bs = _count_hit_touch_big(limitup_rows or [])
        hit_shown, hit_total = hs, hs
        touch_shown, touch_total = ts, ts
        if big_shown is None:
            big_shown = bs
        if big_total is None:
            big_total = bs

    if big_shown is None:
        big_shown = 0
    if big_total is None:
        big_total = 0

    ratio_part = ""
    if sector_shown_total is not None and sector_all_total:
        try:
            shown = int(sector_shown_total)
            total = int(sector_all_total)
            if total > 0:
                pct = shown / total * 100.0
                ratio_part = f"  {shown}/{total}({pct:.0f}%)"
        except Exception:
            ratio_part = ""

    # header order: Locked -> Touch -> 10%+ -> ratio
    top_title_left = f"{locked_label}{int(hit_shown)}/{int(hit_total)}  {touched_label}{int(touch_shown)}/{int(touch_total)}"
    if int(big_total) > 0:
        top_title_left += f"  10%+{int(big_shown)}/{int(big_total)}"
    top_title_left += ratio_part

    # Titles inside boxes
    box_title_fs = int(getattr(layout, "box_title_fs", 32))
    x_left = 0.07
    title_pad_from_top = 0.010
    title_bar_h_ratio = 0.040

    top_span = (top_y0 - top_y1)
    top_title_y = top_y0 - top_span * title_pad_from_top

    bot_span = (bot_y0 - bot_y1)
    bot_title_y = bot_y0 - bot_span * title_pad_from_top

    _shadow_text(
        x_left,
        top_title_y,
        top_title_left,
        fontsize=box_title_fs,
        color=fg,
        ha="left",
        va="top",
        weight="bold",
        alpha=0.98,
    )

    bottom_title_text = _t(lang, "box_title_bottom", "Same sector (not locked/touch/10%+)")
    _shadow_text(
        x_left,
        bot_title_y,
        bottom_title_text,
        fontsize=box_title_fs,
        color=fg,
        ha="left",
        va="top",
        weight="bold",
        alpha=0.95,
    )

    top_div_y = top_y0 - top_span * title_bar_h_ratio
    bot_div_y = bot_y0 - bot_span * title_bar_h_ratio

    ax.plot([0.06, 0.94], [top_div_y, top_div_y], color=divider, linewidth=1.1, alpha=0.75)
    ax.plot([0.06, 0.94], [bot_div_y, bot_div_y], color=divider, linewidth=1.1, alpha=0.75)

    # Rows layout
    two_line = bool(getattr(layout, "two_line", True))

    top_rows_area_y_top = top_div_y
    top_rows_area_y_bottom = top_y1

    bot_rows_area_y_top = bot_div_y
    bot_rows_area_y_bottom = bot_y1

    y_start_top, row_h_top = calc_rows_layout(
        top_rows_area_y_top, top_rows_area_y_bottom, int(rows_per_page), two_line=two_line
    )

    bot_rows_layout_n = int(rows_per_page) + 1
    y_start_bot, row_h_bot = calc_rows_layout(
        bot_rows_area_y_top, bot_rows_area_y_bottom, bot_rows_layout_n, two_line=two_line
    )

    limit_rows = list(limitup_rows or [])
    top_is_empty = (len(limit_rows) == 0)

    show_rows_peer = bot_rows_layout_n
    if top_is_empty:
        show_rows_peer = int(rows_per_page)

    # Right padding
    BADGE_RIGHT_PAD_PX = 26.0
    RET_RIGHT_PAD_PX = 18.0

    # Pills: after company name (ONLY limit band pill)
    row_name_fs = int(getattr(layout, "row_name_fs", getattr(layout, "row_fs_1", 28)))
    ret_fs = max(row_name_fs - 4, 18)
    pill_fs = row_name_fs
    pill_pad = 0.14
    pill_gap_px = 10.0
    safe_gap_to_right_px = 12.0

    def _draw_pill(x: float, y: float, text: str, fg_color: str, bg_color: str) -> float:
        if not text:
            return x
        ax.text(
            x, y, text,
            ha="left", va="center",
            fontsize=pill_fs,
            color=fg_color,
            weight="bold",
            bbox=dict(boxstyle=f"round,pad={pill_pad}", facecolor=bg_color, edgecolor="none", alpha=0.95),
        )
        w_px = _text_width_px(text, x, y, fontsize=pill_fs, weight="bold") + 18.0
        return x + _px_to_data_dx(w_px, y)

    def _draw_pills_after_name(line1: str, y: float, row: Dict[str, Any], x_right_limit: float) -> None:
        w1_px = _text_width_px(line1, layout.x_name, y, fontsize=row_name_fs, weight="bold")
        x = layout.x_name + _px_to_data_dx(w1_px + pill_gap_px, y)
        if x >= x_right_limit:
            return

        limit_pct = _limit_pct_from_row(row)
        if limit_pct <= 0:
            return

        lim_bg, lim_fg = _limit_colors(limit_pct, theme)
        label = f"涨跌幅上限{int(round(limit_pct))}%"
        _draw_pill(x, y, label, lim_fg, lim_bg)

    x_name = float(getattr(layout, "x_name", 0.08))
    x_tag = float(getattr(layout, "x_tag", 0.94))

    # Draw TOP rows
    if top_is_empty:
        empty_top = _t(lang, "empty_limitup", "(No Locked/Touch/Big data on this page)")
        ax.text(
            0.5,
            (top_rows_area_y_top + top_rows_area_y_bottom) / 2,
            empty_top,
            ha="center",
            va="center",
            fontsize=int(getattr(layout, "empty_hint_fs", 34)),
            color=sub,
            weight="bold",
            alpha=0.90,
        )
    else:
        _ensure_renderer()
        n = min(len(limit_rows), int(rows_per_page))

        x_right_for_pills = x_tag - _px_to_data_dx(safe_gap_to_right_px + BADGE_RIGHT_PAD_PX, y_start_top)

        for i in range(n):
            y_center = y_start_top - i * row_h_top

            if two_line:
                y1 = y_center + row_h_top * 0.22
                y2 = y_center - row_h_top * 0.22
            else:
                y1 = y_center
                y2 = y_center

            r = limit_rows[i]
            line1 = _safe_str(r.get("line1") or "")
            line2 = _safe_str(r.get("line2") or "")

            ax.text(
                x_name, y1, _ellipsize(line1, 26),
                ha="left", va="center",
                fontsize=row_name_fs, color=fg, weight="bold"
            )

            _draw_pills_after_name(line1, y1, r, x_right_limit=x_right_for_pills)

            if two_line and line2:
                ax.text(
                    x_name, y2, _ellipsize(line2.replace("|", " ").replace("｜", " "), 34),
                    ha="left", va="center",
                    fontsize=row_name_fs, color=sub, weight="bold", alpha=0.95
                )

            status = _safe_str(r.get("limitup_status") or "").lower()

            x_badge = x_tag - _px_to_data_dx(BADGE_RIGHT_PAD_PX, y1)

            if status in ("touch", "bomb"):
                ax.text(
                    x_badge, y1, touched_label,
                    ha="right", va="center",
                    fontsize=int(getattr(layout, "row_tag_fs", 26)),
                    color=touch_pill_fg, weight="bold",
                    bbox=dict(boxstyle="round,pad=0.30", facecolor=touch_pill_bg, edgecolor="none", alpha=0.96),
                )
            elif status == "big":
                ax.text(
                    x_badge, y1, big_label,
                    ha="right", va="center",
                    fontsize=int(getattr(layout, "row_tag_fs", 26)),
                    color=big_pill_fg, weight="bold",
                    bbox=dict(boxstyle="round,pad=0.30", facecolor=big_pill_bg, edgecolor="none", alpha=0.96),
                )
            else:
                ax.text(
                    x_badge, y1, locked_label,
                    ha="right", va="center",
                    fontsize=int(getattr(layout, "row_tag_fs", 26)),
                    color=limitup_pill_fg, weight="bold",
                    bbox=dict(boxstyle="round,pad=0.32", facecolor=badge_red, edgecolor="none", alpha=0.95),
                )

            # ret at second line right (CN style)
            ret = _safe_float(r.get("ret") or 0.0, 0.0)
            ret_text = _safe_str(r.get("ret_text") or "") or _fmt_ret_pct(ret)
            x_ret_draw = x_tag - _px_to_data_dx(RET_RIGHT_PAD_PX, y2)
            ax.text(
                x_ret_draw, y2, ret_text,
                ha="right", va="center",
                fontsize=ret_fs,
                color=get_ret_color(ret, theme),
                weight="bold"
            )

            if i < n - 1:
                ax.plot(
                    [0.06, 0.94],
                    [y_center - row_h_top * 0.50, y_center - row_h_top * 0.50],
                    color=divider, linewidth=1, alpha=0.55
                )

    # Draw BOTTOM rows (peers)
    peers = list(peer_rows or [])
    if not peers:
        empty_peer = _t(lang, "empty_peer", "(No data on this page)")
        ax.text(
            0.5,
            (bot_rows_area_y_top + bot_rows_area_y_bottom) / 2,
            empty_peer,
            ha="center",
            va="center",
            fontsize=int(getattr(layout, "empty_hint_fs", 34)),
            color=sub,
            weight="bold",
            alpha=0.90,
        )
    else:
        _ensure_renderer()

        n2 = min(len(peers), show_rows_peer)

        x_right_for_pills2 = x_tag - _px_to_data_dx(safe_gap_to_right_px + RET_RIGHT_PAD_PX, y_start_bot)

        for i in range(n2):
            y_center = y_start_bot - i * row_h_bot
            r = peers[i]

            line1 = _safe_str(r.get("line1") or "")
            line2 = _safe_str(r.get("line2") or "")

            y1 = y_center + row_h_bot * 0.22
            y2 = y_center - row_h_bot * 0.22

            ax.text(
                x_name, y1, _ellipsize(line1, 26),
                ha="left", va="center",
                fontsize=row_name_fs, color=fg, weight="bold"
            )

            _draw_pills_after_name(line1, y1, r, x_right_limit=x_right_for_pills2)

            if line2:
                ax.text(
                    x_name, y2, _ellipsize(line2.replace("|", " ").replace("｜", " "), 34),
                    ha="left", va="center",
                    fontsize=row_name_fs, color=sub, weight="bold", alpha=0.95
                )

            ret = _safe_float(r.get("ret") or 0.0, 0.0)
            ret_text = _safe_str(r.get("ret_text") or "") or _fmt_ret_pct(ret)
            x_ret_draw = x_tag - _px_to_data_dx(RET_RIGHT_PAD_PX, y1)
            ax.text(
                x_ret_draw, y1, ret_text,
                ha="right", va="center",
                fontsize=ret_fs,
                color=get_ret_color(ret, theme),
                weight="bold"
            )

            if i < n2 - 1:
                ax.plot(
                    [0.06, 0.94],
                    [y_center - row_h_bot * 0.50, y_center - row_h_bot * 0.50],
                    color=divider, linewidth=1, alpha=0.55
                )

        if has_more_peers:
            hint_text = _t(lang, "more_hint", "(More data not shown)")
            ax.text(
                0.5,
                bot_y1 + 0.004,
                hint_text,
                ha="center",
                va="bottom",
                fontsize=max(footer_fs + 2, 20),
                color=sub,
                weight="bold",
                alpha=0.80,
            )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100)
    plt.close(fig)
    return out_path
