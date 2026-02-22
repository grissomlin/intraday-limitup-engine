# scripts/render_images_us/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from .layout import LayoutSpec, calc_rows_layout


# =============================================================================
# Fonts / Text helpers
# =============================================================================
def setup_chinese_font() -> str | None:
    """
    盡量找可顯示中英文的字型。
    注意：Microsoft JhengHei 對某些數學符號(≥)可能缺 glyph，所以我們在文字上避免使用 ≥。
    """
    try:
        font_candidates = [
            "Microsoft JhengHei",
            "Microsoft YaHei",
            "PingFang TC",
            "PingFang SC",
            "Noto Sans CJK TC",
            "Noto Sans CJK SC",
            "Noto Sans CJK JP",
            "SimHei",
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


def strip_symbol_suffix(sym: str) -> str:
    s = (sym or "").strip()
    if "." in s:
        s = s.split(".", 1)[0]
    return s.strip()


def parse_cutoff(payload: Dict[str, Any]) -> str:
    ymd = str(payload.get("ymd") or payload.get("ymd_effective") or "").strip()
    asof = str(payload.get("asof") or "").strip()
    gen = str(payload.get("generated_at") or "").strip()
    t = asof or gen
    hhmm = ""
    if "T" in t:
        hhmm = t.split("T", 1)[1][:5]
    elif len(t) >= 5 and ":" in t:
        hhmm = t[:5]
    if ymd and hhmm:
        return f"{ymd}  |  asof {hhmm}"
    return ymd or ""


def sector_counts(limitup_rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    locked = sum(1 for r in limitup_rows if str(r.get("limitup_status", "")).lower() == "locked")
    touch = sum(1 for r in limitup_rows if str(r.get("limitup_status", "")).lower() == "touch_only")
    theme = sum(1 for r in limitup_rows if str(r.get("limitup_status", "")).lower() == "no_limit_theme")
    return locked, touch, theme


def _safe_ascii_math(s: str) -> str:
    """
    避免 ≥ / ≤ / ＞ 這類 glyph 變方塊：統一轉成 ASCII >= <= >
    """
    if not s:
        return ""
    return (
        s.replace("≥", ">=")
        .replace("≤", "<=")
        .replace("＞", ">")
        .replace("＜", "<")
    )


# =============================================================================
# Drawing
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
    theme: str = "dark",
):
    setup_chinese_font()

    # -------------------------
    # Colors
    # -------------------------
    if theme == "light":
        bg = "#f8f9fa"
        fg = "#111111"
        sub = "#555555"
        box = "#ffffff"
        line = "#e0e0e0"
        peer_color = "#1976d2"
        line2_color = "#333333"
    else:
        # 跟 TW 類似深藍底
        bg = "#0f0f1e"
        fg = "#ffffff"
        sub = "#999999"
        box = "#1a1a2e"
        line = "#2d2d44"
        peer_color = "#2196f3"
        line2_color = "#e6e6e6"

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor=bg)
    ax = fig.add_subplot(111)
    ax.set_facecolor(bg)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # -------------------------
    # Header
    # -------------------------
    ax.text(
        0.5,
        layout.header_title_y,
        sector,
        ha="center",
        va="top",
        fontsize=layout.title_fs,
        color=fg,
        weight="bold",
    )

    # subtitle counts (optional)
    parts = []
    if locked_cnt > 0:
        parts.append(f"Locked {locked_cnt}")
    if touch_cnt > 0:
        parts.append(f"Touched {touch_cnt}")
    if theme_cnt > 0:
        parts.append(f"Theme {theme_cnt}")
    subtitle = "  |  ".join(parts) if parts else ""
    if subtitle:
        ax.text(
            0.5,
            layout.header_subtitle_y,
            subtitle,
            ha="center",
            va="top",
            fontsize=getattr(layout, "subtitle_fs", max(18, layout.title_fs // 2)),
            color=sub,
        )

    if page_total > 1:
        ax.text(
            0.97,
            layout.header_title_y,
            f"{page_idx}/{page_total}",
            ha="right",
            va="top",
            fontsize=getattr(layout, "page_fs", 24),
            color=sub,
            alpha=0.85,
        )

    # -------------------------
    # Footer
    # -------------------------
    y1 = layout.footer_y1
    y2 = layout.footer_y2

    if cutoff:
        ax.text(0.03, y1, cutoff, ha="left", va="bottom", fontsize=layout.footer_fs_1, color=sub, alpha=0.85)

    ax.text(
        0.03,
        y2,
        "Data: yfinance | Not financial advice",
        ha="left",
        va="bottom",
        fontsize=layout.footer_fs_2,
        color=sub,
        alpha=0.78,
    )

    ax.text(
        0.97,
        y1,
        "Note: streak = consecutive days",
        ha="right",
        va="bottom",
        fontsize=layout.footer_note_fs,
        color=sub,
        alpha=0.78,
    )
    ax.text(
        0.97,
        y2,
        ">=10% means daily return >= 10%",
        ha="right",
        va="bottom",
        fontsize=layout.footer_note_fs,
        color=sub,
        alpha=0.78,
    )

    # -------------------------
    # Boxes
    # -------------------------
    top_y0, top_y1b = layout.top_box_y0, layout.top_box_y1
    bot_y0, bot_y1b = layout.bot_box_y0, layout.bot_box_y1

    ax.add_patch(
        plt.Rectangle(
            (0.05, top_y1b),
            0.90,
            (top_y0 - top_y1b),
            facecolor=box,
            edgecolor=line,
            linewidth=2,
            alpha=0.95,
        )
    )
    ax.add_patch(
        plt.Rectangle(
            (0.05, bot_y1b),
            0.90,
            (bot_y0 - bot_y1b),
            facecolor=box,
            edgecolor=line,
            linewidth=2,
            alpha=0.95,
        )
    )

    # ✅ 多留一點標題與第一列距離（你說的「不要擠」）
    top_label_pad = 0.035
    bot_label_pad = 0.035

    ax.text(
        0.08,
        top_y0 - 0.02,
        "Movers (>=10%)",
        ha="left",
        va="center",
        fontsize=layout.box_title_fs,
        color=fg,
        weight="bold",
    )
    ax.text(
        0.08,
        bot_y0 - 0.02,
        "Peers (<10%)",
        ha="left",
        va="center",
        fontsize=layout.box_title_fs,
        color=fg,
        weight="bold",
        alpha=0.92,
    )

    MAX_ROWS_PER_BOX = int(rows_per_page or 6)

    x_name = layout.x_name
    x_prev = layout.x_prev
    x_tag = layout.x_tag

    # ✅ two-line 會提高 row_h，避免重疊
    y_start_top, row_h_top = calc_rows_layout(top_y0 - top_label_pad, top_y1b, MAX_ROWS_PER_BOX, two_line=layout.two_line)
    y_start_bot, row_h_bot = calc_rows_layout(bot_y0 - bot_label_pad, bot_y1b, MAX_ROWS_PER_BOX, two_line=layout.two_line)

    renderer = None

    def _ensure_renderer():
        nonlocal renderer
        if renderer is None:
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()

    def _ellipsis_fit(text: str, x_left: float, x_right: float, y: float, fontsize: int, weight: str = "medium") -> str:
        if not text:
            return ""
        text = _safe_ascii_math(text)

        _ensure_renderer()
        t = ax.text(
            x_left,
            y,
            text,
            ha="left",
            va="center",
            fontsize=fontsize,
            color=fg,
            weight=weight,
            alpha=0.0,
        )

        def ok(s: str) -> bool:
            t.set_text(s)
            bb = t.get_window_extent(renderer=renderer)
            p0 = ax.transData.transform((x_left, y))
            p1 = ax.transData.transform((x_right, y))
            avail = max(1.0, (p1[0] - p0[0]))
            return bb.width <= avail

        if ok(text):
            t.remove()
            return text

        base = text
        ell = "..."
        lo, hi = 0, len(base)
        best = ell
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = base[:mid].rstrip()
            cand = (cand + ell) if cand else ell
            if ok(cand):
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1

        t.remove()
        return best

    def draw_empty_hint(y0: float, y1: float, text: str):
        cy = (y0 + y1) / 2
        ax.text(0.5, cy, text, ha="center", va="center", fontsize=layout.empty_hint_fs, color=sub, alpha=0.9)

    def draw_rows(rows: List[Dict[str, Any]], y_start: float, row_h: float, kind: str):
        if not rows:
            if kind == "limitup":
                draw_empty_hint(top_y0, top_y1b, "(No movers on this page)")
            else:
                draw_empty_hint(bot_y0, bot_y1b, "(No peers on this page)")
            return

        n = min(len(rows), MAX_ROWS_PER_BOX)
        for i in range(n):
            y = y_start - i * row_h
            r = rows[i]

            line1 = str(r.get("line1") or "").strip()
            line2 = str(r.get("line2") or "").strip()

            # 讓第一行可以更長：右側保留區縮小一點（你目前沒右側 badge）
            safe_right = min(x_prev, x_tag) - 0.03
            safe_right = max(x_name + 0.05, safe_right)

            if line2:
                # ✅ 行距再拉大一點，避免你看到的「黏在一起」
                y1t = y + row_h * 0.24
                y2t = y - row_h * 0.24

                fit1 = _ellipsis_fit(line1, x_name, safe_right, y1t, layout.row_name_fs, weight="bold")
                ax.text(
                    x_name,
                    y1t,
                    fit1,
                    ha="left",
                    va="center",
                    fontsize=layout.row_name_fs,
                    color=fg,
                    weight="bold",
                )

                # ✅ 第二行：加粗 + 字體變大（你嫌小）
                line2_fs = max(layout.row_line2_fs, int(layout.row_name_fs * 0.72))
                fit2 = _ellipsis_fit(line2, x_name, safe_right, y2t, line2_fs, weight="bold")
                ax.text(
                    x_name,
                    y2t,
                    fit2,
                    ha="left",
                    va="center",
                    fontsize=line2_fs,
                    color=line2_color,
                    weight="bold",
                    alpha=0.97,
                )
            else:
                fit1 = _ellipsis_fit(line1, x_name, safe_right, y, layout.row_name_fs, weight="bold")
                ax.text(
                    x_name,
                    y,
                    fit1,
                    ha="left",
                    va="center",
                    fontsize=layout.row_name_fs,
                    color=fg,
                    weight="bold",
                )

            # row divider
            if i < n - 1:
                ax.plot(
                    [0.08, 0.91],
                    [y - row_h * 0.5, y - row_h * 0.5],
                    color=line,
                    linewidth=1,
                    alpha=0.5,
                )

    draw_rows(limitup_rows, y_start_top, row_h_top, "limitup")
    draw_rows(peer_rows, y_start_bot, row_h_bot, "peer")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100, facecolor=bg, bbox_inches="tight", pad_inches=0.06)
    plt.close(fig)
