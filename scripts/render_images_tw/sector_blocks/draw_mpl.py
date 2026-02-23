# scripts/render_images_tw/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt

from .layout import LayoutSpec, calc_rows_layout
from ._font import setup_cjk_font
from ._time import parse_cutoff, get_market_time_info
from ._tags import strip_inline_emerging_tag, is_emerging_row, pick_board_tag_style
from ._badge import (
    get_ret_color,
    is_surge_row,
    pick_move_band_tag,
    badge_is_manual_streak_text,
    badge_is_generic_surge,
)
from ._textfit import TextFitter

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
# Debug (optional)
# =============================================================================
def _env_bool(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


TW_SECTOR_DEBUG = _env_bool("TW_SECTOR_DEBUG", "0")
TW_SECTOR_DEBUG_N = _env_int("TW_SECTOR_DEBUG_N", 12)


# =============================================================================
# Small safe helpers
# =============================================================================
def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default


# =============================================================================
# Streak helpers (fallback)
# =============================================================================
_STREAK_TODAY_KEYS = (
    "streak_today",
    "streak_len",
    "streak",
    "limitup_streak",
    "streak_locked",
    "streak_touch",
    "streak_len_today",
    "surge_streak",  # ✅ open-limit streak
)

_STREAK_PREV_KEYS = (
    "streak_prev",
    "prev_streak",
    "streak_len_prev",
    "prev_limitup_streak",
    "streak_prev_len",
    "surge_streak_prev",  # ✅ open-limit prev streak
)


def _pick_streak_today(row: Dict[str, Any]) -> Optional[int]:
    for k in _STREAK_TODAY_KEYS:
        if k in row:
            v = _safe_int(row.get(k), default=-1)
            if v >= 0:
                return v
    return None


def _pick_streak_prev(row: Dict[str, Any]) -> Optional[int]:
    for k in _STREAK_PREV_KEYS:
        if k in row:
            v = _safe_int(row.get(k), default=-1)
            if v >= 0:
                return v
    return None


def _format_streak_line2(row: Dict[str, Any], *, kind: str) -> str:
    today = _pick_streak_today(row)
    prev = _pick_streak_prev(row)

    if kind == "peer":
        if prev is not None and prev >= 1:
            return f"前一交易日：{prev}連"
        return ""

    parts: List[str] = []
    if today is not None and today >= 1:
        parts.append(f"今日：{today}連")
    if prev is not None and prev >= 1:
        parts.append(f"前一交易日：{prev}連")
    return "｜".join(parts) if parts else ""


def _peer_line2(r: Dict[str, Any]) -> str:
    s2 = _safe_str(r.get("line2") or "")
    if s2:
        return s2

    s = _safe_str(r.get("status_text") or "")
    if s:
        return s

    if bool(r.get("prev_is_limitup_locked", False)):
        return "前一交易日：漲停鎖死"
    if bool(r.get("prev_is_limitup_touch", False)):
        return "前一交易日：漲停（未鎖）"
    if bool(r.get("prev_is_surge10", False)) or bool(r.get("prev_is_surge10_touch", False)):
        return "前一交易日：有大漲"

    if bool(r.get("prev_was_limitup_locked", False)) or bool(r.get("prev_was_locked", False)):
        return "前一交易日：有漲停"

    return "前一交易日：無"


# =============================================================================
# Main renderer
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
    limitup_rows: List[Dict[str, Any]],
    peer_rows: List[Dict[str, Any]],
    page_idx: int,
    page_total: int,
    width: int,
    height: int,
    rows_per_page: int,
    theme: str = "light",
    time_note: str = "",
    has_more_peers: bool = False,
    hit_shown: Optional[int] = None,
    hit_total: Optional[int] = None,
    touch_shown: Optional[int] = None,
    touch_total: Optional[int] = None,
    surge_shown: Optional[int] = None,
    surge_total: Optional[int] = None,
    sector_share: Optional[float] = None,
) -> None:
    setup_cjk_font()

    if theme == "light":
        bg = "#eef3f6"
        fg = "#111111"
        sub = "#555555"
        box = "#f7f7f7"
        line = "#cfd8e3"
        line2_color = "#444444"

        tag_theme = "#d32f2f"  # 漲停鎖死
        tag_fail = "#8e44ad"  # 漲停鎖死失敗
        tag_surge = "#2e7d32"
        tag_surge_streak = "#8e44ad"  # ✅ 興櫃 streak(10%+) 紫底白字
    else:
        bg = "#0f0f1e"
        fg = "#ffffff"
        sub = "#999999"
        box = "#1a1a2e"
        line = "#2d2d44"
        line2_color = "#cfcfcf"

        tag_theme = "#e53935"
        tag_fail = "#9b59b6"
        tag_surge = "#43a047"
        tag_surge_streak = "#9b59b6"

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor=bg)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(bg)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    tf = TextFitter(fig=fig, ax=ax, fg=fg)

    # -------------------------
    # Title
    # -------------------------
    ax.text(
        0.5,
        layout.header_title_y,
        _safe_str(sector),
        ha="center",
        va="top",
        fontsize=layout.title_fs,
        color=fg,
        weight="bold",
    )

    # ✅ subtitle supports TWO lines
    subtitle = (time_note or "").strip()
    if subtitle:
        lines = [s.strip() for s in subtitle.splitlines() if s.strip()]
        if len(lines) == 1:
            ax.text(
                0.5,
                layout.header_subtitle_y,
                lines[0],
                ha="center",
                va="top",
                fontsize=layout.subtitle_fs,
                color=sub,
                weight="bold",
                alpha=0.90,
            )
        else:
            ax.text(
                0.5,
                layout.header_subtitle_y,
                lines[0],
                ha="center",
                va="top",
                fontsize=layout.subtitle_fs,
                color=sub,
                weight="bold",
                alpha=0.90,
            )
            delta = 0.040
            ax.text(
                0.5,
                layout.header_subtitle_y - delta,
                lines[1],
                ha="center",
                va="top",
                fontsize=layout.subtitle_fs,
                color=sub,
                weight="bold",
                alpha=0.90,
            )

    if page_total > 1:
        ax.text(
            0.97,
            layout.header_title_y,
            f"{page_idx}/{page_total}",
            ha="right",
            va="top",
            fontsize=layout.page_fs,
            color=sub,
            weight="bold",
            alpha=0.90,
        )

    ax.text(
        0.05,
        layout.footer_y2,
        "資料：資料來源：公開市場資料整理｜僅供參考（非投資建議）",
        ha="left",
        va="bottom",
        fontsize=layout.footer_fs_2,
        color=sub,
        alpha=0.85,
    )

    # -------------------------
    # Boxes
    # -------------------------
    top_y0, top_y1 = layout.top_box_y0, layout.top_box_y1
    bot_y0, bot_y1 = layout.bot_box_y0, layout.bot_box_y1

    ax.add_patch(
        plt.Rectangle(
            (0.05, top_y1),
            0.90,
            (top_y0 - top_y1),
            facecolor=box,
            edgecolor=line,
            linewidth=2,
            alpha=0.98,
        )
    )
    ax.add_patch(
        plt.Rectangle(
            (0.05, bot_y1),
            0.90,
            (bot_y0 - bot_y1),
            facecolor=box,
            edgecolor=line,
            linewidth=2,
            alpha=0.98,
        )
    )

    top_span = (top_y0 - top_y1)
    bot_span = (bot_y0 - bot_y1)
    top_title_y = top_y0 - top_span * 0.035
    bot_title_y = bot_y0 - bot_span * 0.035

    hit_cnt_fallback = int(locked_cnt or 0)
    touch_cnt_fallback = int(touch_cnt or 0)
    surge_cnt_fallback = int((surge_total if surge_total is not None else theme_cnt) or 0)

    use_precise = (
        hit_shown is not None
        and hit_total is not None
        and touch_shown is not None
        and touch_total is not None
        and surge_shown is not None
        and surge_total is not None
    )

    if use_precise:
        hs, ht = int(hit_shown), int(hit_total)
        ts, tt = int(touch_shown), int(touch_total)
        gs, gt = int(surge_shown), int(surge_total)
        top_title = f"漲停鎖死 {hs}/{ht}｜漲停鎖死失敗 {ts}/{tt}｜漲幅10%+ {gs}/{gt}"
    else:
        top_title = (
            f"漲停鎖死 {hit_cnt_fallback}/{hit_cnt_fallback}"
            f"｜漲停鎖死失敗 {touch_cnt_fallback}/{touch_cnt_fallback}"
            f"｜漲幅10%+ {surge_cnt_fallback}/{surge_cnt_fallback}"
        )

    if sector_share is not None:
        try:
            top_title += f"｜產業{float(sector_share) * 100.0:.1f}%"
        except Exception:
            pass

    top_title_fs = tf.fit_left_fontsize(
        top_title,
        x_left=0.08,
        x_right=0.92,
        y=top_title_y,
        base_fs=int(layout.box_title_fs),
        min_fs=max(20, int(layout.box_title_fs) - 10),
        weight="bold",
    )

    ax.text(
        0.08,
        top_title_y,
        top_title,
        ha="left",
        va="center",
        fontsize=top_title_fs,
        color=fg,
        weight="bold",
    )

    ax.text(
        0.08,
        bot_title_y,
        "同產業・無10%+或漲停股",
        ha="left",
        va="center",
        fontsize=layout.box_title_fs,
        color=fg,
        weight="bold",
        alpha=0.95,
    )

    MAX_ROWS_PER_BOX = max(1, int(rows_per_page or 6))
    y_start_top, row_h_top = calc_rows_layout(top_y0, top_y1, MAX_ROWS_PER_BOX, two_line=layout.two_line)
    y_start_bot, row_h_bot = calc_rows_layout(bot_y0, bot_y1, MAX_ROWS_PER_BOX, two_line=layout.two_line)

    x_name = layout.x_name
    x_tag = layout.x_tag
    safe_right = x_tag - 0.10

    def draw_empty_hint(y0: float, y1: float, text: str) -> None:
        cy = (y0 + y1) / 2
        ax.text(
            0.5,
            cy,
            text,
            ha="center",
            va="center",
            fontsize=layout.empty_hint_fs,
            color=sub,
            alpha=0.55,
        )

    def draw_rows(rows: List[Dict[str, Any]], y_start: float, row_h: float, kind: str) -> None:
        if not rows:
            draw_empty_hint(
                top_y0 if kind == "top" else bot_y0,
                top_y1 if kind == "top" else bot_y1,
                "（本頁無符合資料）" if kind == "top" else "（本頁無資料）",
            )
            return

        n = min(len(rows), MAX_ROWS_PER_BOX)
        for i in range(n):
            y = y_start - i * row_h
            r = rows[i] if isinstance(rows[i], dict) else {}

            line1_raw = _safe_str(r.get("line1") or "") or _safe_str(r.get("name") or "")
            line1 = strip_inline_emerging_tag(line1_raw)

            line2_raw = _safe_str(r.get("line2") or "")
            if not line2_raw:
                line2_raw = _format_streak_line2(r, kind="peer" if kind == "peer" else "top")

            if kind == "peer":
                line2 = _peer_line2(r)
            else:
                line2 = line2_raw

            ret_pct_raw = r.get("ret_pct", None)
            if ret_pct_raw is None:
                try:
                    rr = float(r.get("ret", 0.0) or 0.0)
                except Exception:
                    rr = 0.0
                ret_pct = rr * 100.0
            else:
                ret_pct = _safe_float(ret_pct_raw, 0.0)

            ret_decimal = ret_pct / 100.0
            lang = str(r.get("lang") or "zh_hant").strip().lower() or "zh_hant"

            if layout.two_line:
                y1 = y + row_h * 0.22
                y2 = y - row_h * 0.22

                fit1 = tf.ellipsis_fit(line1, x_name, safe_right, y1, layout.row_name_fs, weight="medium")
                tf.ensure_renderer()
                t1 = ax.text(
                    x_name,
                    y1,
                    fit1,
                    ha="left",
                    va="center",
                    fontsize=layout.row_name_fs,
                    color=fg,
                    weight="medium",
                )

                # --- Emerging pill ---
                if is_emerging_row(r):
                    try:
                        renderer = tf.ensure_renderer()
                        bb1 = t1.get_window_extent(renderer=renderer)
                        ax_bb = ax.get_window_extent(renderer=renderer)
                        ax_w = float(ax_bb.width) if ax_bb.width else 1.0

                        gap_px = 12.0
                        x_pill = x_name + (float(bb1.width) + gap_px) / ax_w

                        board_kind = str(r.get("board_kind") or r.get("market_detail") or "").strip().lower()
                        _, pill_bg = pick_board_tag_style(board_kind)

                        pill_text = "興櫃"
                        tmp = ax.text(0, 0, pill_text, fontsize=max(10, int(layout.row_name_fs * 0.78)))
                        tb = tmp.get_window_extent(renderer=renderer)
                        tmp.remove()

                        pill_w = (float(tb.width) + 18.0) / ax_w
                        if (x_pill + pill_w) < (x_tag - 0.02):
                            ax.text(
                                x_pill,
                                y1,
                                pill_text,
                                ha="left",
                                va="center",
                                fontsize=max(10, int(layout.row_name_fs * 0.78)),
                                color="white",
                                weight="bold",
                                bbox=dict(
                                    boxstyle="round,pad=0.25,rounding_size=0.18",
                                    facecolor=pill_bg,
                                    alpha=0.95,
                                    edgecolor="none",
                                ),
                                zorder=7,
                            )
                    except Exception:
                        pass

                if line2:
                    fit2 = tf.ellipsis_fit(line2, x_name, safe_right, y2, layout.row_line2_fs, weight="normal")
                    ax.text(
                        x_name,
                        y2,
                        fit2,
                        ha="left",
                        va="center",
                        fontsize=layout.row_line2_fs,
                        color=line2_color,
                        weight="normal",
                        alpha=0.95,
                    )

                # =========================
                # badge (top-right)
                # =========================
                badge_text = _safe_str(r.get("badge_text") or "")
                if badge_text:
                    if is_surge_row(r, badge_text):
                        if badge_is_manual_streak_text(badge_text):
                            badge_bg = tag_surge_streak
                        else:
                            if badge_is_generic_surge(badge_text):
                                band_text, band_bg = pick_move_band_tag(ret_decimal, t_func=_t, lang=lang)
                                badge_text = band_text
                                badge_bg = band_bg
                            else:
                                badge_bg = tag_surge_streak if "10%" in badge_text else tag_surge
                    else:
                        b = badge_text.strip()
                        badge_bg = tag_fail if ("失敗" in b) else tag_theme

                    ax.text(
                        x_tag,
                        y1,
                        badge_text,
                        ha="right",
                        va="center",
                        fontsize=layout.row_tag_fs,
                        color="white",
                        weight="bold",
                        bbox=dict(
                            boxstyle=f"round,pad={layout.badge_pad_limitup}",
                            facecolor=badge_bg,
                            alpha=0.9,
                            edgecolor="none",
                        ),
                    )

                # =========================
                # return chip (bottom-right)
                # =========================
                if abs(ret_pct) > 0.0001:
                    if ret_pct >= 0:
                        ret_color = get_ret_color(ret_decimal, theme)
                    else:
                        ret_color = "#c62828" if theme == "light" else "#ef5350"

                    sign = "+" if ret_pct > 0 else ""
                    if abs(ret_decimal) >= 1.00:
                        tag_text2 = f"{sign}{ret_pct:.0f}%"
                        tag_fontsize = layout.row_tag_fs - 2
                    elif abs(ret_decimal) >= 0.10:
                        tag_text2 = f"{sign}{ret_pct:.1f}%"
                        tag_fontsize = layout.row_tag_fs
                    else:
                        tag_text2 = f"{sign}{ret_pct:.2f}%"
                        tag_fontsize = layout.row_tag_fs

                    ax.text(
                        x_tag,
                        y2,
                        tag_text2,
                        ha="right",
                        va="center",
                        fontsize=tag_fontsize,
                        color="white",
                        weight="bold",
                        bbox=dict(
                            boxstyle=f"round,pad={layout.badge_pad_peer}",
                            facecolor=ret_color,
                            alpha=0.9,
                            edgecolor="none",
                        ),
                    )

            else:
                fit1 = tf.ellipsis_fit(
                    line1 or _safe_str(r.get("name", "")),
                    x_name,
                    safe_right,
                    y,
                    layout.row_name_fs,
                )
                ax.text(
                    x_name,
                    y,
                    fit1,
                    ha="left",
                    va="center",
                    fontsize=layout.row_name_fs,
                    color=fg,
                    weight="medium",
                )

            if i < n - 1:
                ax.plot(
                    [0.08, 0.91],
                    [y - row_h * 0.50, y - row_h * 0.50],
                    color=line,
                    linewidth=1,
                    alpha=0.5,
                )

        if kind == "peer" and has_more_peers:
            hint_y = (y_start - (n - 1) * row_h) - row_h * 0.75
            ax.text(
                0.5,
                hint_y,
                "（尚有未顯示資料）",
                ha="center",
                va="top",
                fontsize=max(layout.footer_fs_2 + 6, 26),
                color=sub,
                alpha=0.85,
                weight="bold",
            )

    draw_rows(limitup_rows, y_start_top, row_h_top, "top")
    draw_rows(peer_rows, y_start_bot, row_h_bot, "peer")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100, facecolor=bg)
    plt.close(fig)
