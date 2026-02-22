# scripts/render_images_th/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from .layout import LayoutSpec, calc_rows_layout


# =============================================================================
# Font (Thai + CJK safe)
# =============================================================================
def setup_thai_font() -> str | None:
    """
    Goal:
    - Thai (TH)
    - Do not break mixed footer (EN/CN) too badly (fallback ok)
    """
    font_candidates = [
        # Common in Linux/Colab
        "Noto Sans Thai",
        "Noto Sans Thai UI",
        "Noto Sans",
        # Windows common
        "Tahoma",
        "Leelawadee UI",
        "TH Sarabun New",
        # CJK fallbacks (in case your UI still has Chinese/JP)
        "Noto Sans CJK TC",
        "Microsoft JhengHei",
        "Arial Unicode MS",
        "DejaVu Sans",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    for f in font_candidates:
        if f in available:
            plt.rcParams["font.sans-serif"] = [f]
            plt.rcParams["axes.unicode_minus"] = False
            return f
    return None


# =============================================================================
# Payload helpers
# =============================================================================
def parse_cutoff(payload: Dict[str, Any]) -> str:
    return str(payload.get("ymd_effective") or payload.get("ymd") or "").strip()


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _format_produced_time_note(payload: Dict[str, Any]) -> str:
    """
    Prefer main.py produced meta.time (time_builders schema):
      - market_finished_at  (YYYY-MM-DD HH:MM)
      - market_tz_offset / market_utc_offset  ("+07:00") -> display as "UTC+07:00"

    fallback:
      - payload["generated_at"] -> YYYY-MM-DD HH:MM (best effort)
    """
    meta = payload.get("meta") or {}
    t = meta.get("time") or {}

    produced_at = _safe_str(t.get("market_finished_at"))

    # ✅ time_builders provides offset fields, not "market_tz" = "UTC+07:00"
    off = _safe_str(t.get("market_tz_offset") or t.get("market_utc_offset"))
    tz_label = f"UTC{off}" if off else ""

    if not produced_at:
        ga = _safe_str(payload.get("generated_at"))
        if "T" in ga and len(ga) >= 16:
            produced_at = ga.replace("T", " ", 1)[:16]
        elif " " in ga and len(ga) >= 16:
            produced_at = ga[:16]

    if not produced_at:
        return ""

    if tz_label:
        return f"อัปเดต {produced_at} ({tz_label})"
    return f"อัปเดต {produced_at}"


def get_market_time_info(payload: Dict[str, Any]) -> Tuple[str, str]:
    ymd = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    time_note = _format_produced_time_note(payload)
    return ymd, time_note


# =============================================================================
# Colors
# =============================================================================
def get_ret_color(ret: float, theme: str = "light") -> str:
    # Same palette you used
    if ret >= 1.00:
        return "#1565c0" if theme == "light" else "#1e88e5"
    elif ret >= 0.50:
        return "#1976d2" if theme == "light" else "#2196f3"
    elif ret >= 0.30:
        return "#1e88e5" if theme == "light" else "#42a5f5"
    elif ret >= 0.20:
        return "#42a5f5" if theme == "light" else "#64b5f6"
    else:
        return "#64b5f6" if theme == "light" else "#90caf9"


# =============================================================================
# Main draw
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
    time_note: str = "",
    has_more_peers: bool = False,
    hit_shown: Optional[int] = None,
    hit_total: Optional[int] = None,
    touch_shown: Optional[int] = None,
    touch_total: Optional[int] = None,
    locked_shown: Optional[int] = None,
    locked_total: Optional[int] = None,
    # ✅ New: ceiling pct (so UI can say "สูงสุด +30%" without reading DB)
    ceiling_pct: float = 0.30,
):
    setup_thai_font()

    # -------------------------
    # Theme
    # -------------------------
    if theme == "light":
        bg, fg, sub = "#eef3f6", "#111111", "#555555"
        box, line = "#f7f7f7", "#cfd8e3"
        line2_color = "#444444"
        down_color = "#c62828"

        # ✅ Badge colors (different for Strong / Surge / Lock / Touch)
        tag_strong = "#7b1fa2"  # purple
        tag_surge = "#d81b60"   # pink/red
        tag_lock = "#6a1b9a"    # deep purple
        tag_touch = "#8e24aa"   # purple-magenta
    else:
        bg, fg, sub = "#0f0f1e", "#ffffff", "#999999"
        box, line = "#1a1a2e", "#2d2d44"
        line2_color = "#cfcfcf"
        down_color = "#ef5350"

        # ✅ Badge colors (different for Strong / Surge / Lock / Touch)
        tag_strong = "#9c27b0"
        tag_surge = "#ec407a"
        tag_lock = "#7b1fa2"
        tag_touch = "#ab47bc"

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor=bg)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # -------- renderer helpers (for ellipsis / auto-fit) --------
    renderer = None

    def _ensure_renderer():
        nonlocal renderer
        if renderer is None:
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()

    def _text_width_px(
        s: str,
        x: float,
        y: float,
        fontsize: int,
        *,
        ha: str = "left",
        va: str = "center",
        weight: str = "bold",
    ) -> float:
        _ensure_renderer()
        t = ax.text(x, y, s, ha=ha, va=va, fontsize=fontsize, color=sub, weight=weight, alpha=0.0)
        bb = t.get_window_extent(renderer=renderer)
        t.remove()
        return float(bb.width)

    def _avail_px(x_left: float, x_right: float, y: float) -> float:
        _ensure_renderer()
        p0 = ax.transData.transform((x_left, y))
        p1 = ax.transData.transform((x_right, y))
        return max(1.0, (p1[0] - p0[0]))

    def _fit_ellipsis_center(
        text: str,
        y: float,
        fontsize: int,
        max_x_left: float = 0.06,
        max_x_right: float = 0.94,
    ) -> str:
        _ensure_renderer()
        text = _safe_str(text)
        if not text:
            return ""

        avail = _avail_px(max_x_left, max_x_right, y)
        if _text_width_px(text, 0.5, y, fontsize=fontsize, ha="center", weight="bold") <= avail:
            return text

        base = text
        ell = "..."
        lo, hi = 0, len(base)
        best = ell
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = base[:mid].rstrip()
            cand = (cand + ell) if cand else ell
            if _text_width_px(cand, 0.5, y, fontsize=fontsize, ha="center", weight="bold") <= avail:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    def _ellipsis_fit_left(
        text: str,
        x_left: float,
        x_right: float,
        y: float,
        fontsize: int,
        *,
        weight: str = "medium",
    ) -> str:
        _ensure_renderer()
        text = _safe_str(text)
        if not text:
            return ""

        t = ax.text(x_left, y, text, ha="left", va="center", fontsize=fontsize, color=fg, weight=weight, alpha=0.0)
        avail = _avail_px(x_left, x_right, y)

        def ok(s: str) -> bool:
            t.set_text(s)
            bb = t.get_window_extent(renderer=renderer)
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

    def _auto_fit_left(
        text: str,
        x_left: float,
        x_right: float,
        y: float,
        fs: int,
        *,
        min_fs: int,
        weight: str = "bold",
    ) -> Tuple[str, int]:
        """
        Auto shrink fontsize to fit width; if still too long, ellipsis.
        Returns (text_to_draw, fontsize)
        """
        _ensure_renderer()
        text = _safe_str(text)
        if not text:
            return "", fs

        avail = _avail_px(x_left, x_right, y)

        cur = int(fs)
        while cur >= int(min_fs):
            w = _text_width_px(text, x_left, y, fontsize=cur, ha="left", weight=weight)
            if w <= avail:
                return text, cur
            cur -= 1

        return _ellipsis_fit_left(text, x_left, x_right, y, int(min_fs), weight=weight), int(min_fs)

    def _ceil_label() -> str:
        try:
            p = int(round(float(ceiling_pct) * 100.0))
            return f"สูงสุด +{p}%"
        except Exception:
            return "สูงสุด +30%"

    # -------------------------
    # Title
    # -------------------------
    sector_show = _fit_ellipsis_center(_safe_str(sector), layout.header_title_y, layout.title_fs)
    ax.text(
        0.5,
        layout.header_title_y,
        sector_show,
        ha="center",
        va="top",
        fontsize=layout.title_fs,
        color=fg,
        weight="bold",
    )

    # page indicator
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
        )

    # -------------------------
    # Header line: time only
    # -------------------------
    if time_note:
        ax.text(
            0.5,
            layout.header_subtitle_y,
            time_note,
            ha="center",
            va="top",
            fontsize=layout.subtitle_fs,
            color=sub,
            alpha=0.9,
            weight="bold",
        )

    # -------------------------
    # Footer
    # -------------------------
    ax.text(
        0.05,
        layout.footer_y2,
        "แหล่งข้อมูล: ข้อมูลตลาดสาธารณะ | เพื่อข้อมูลเท่านั้น ไม่ใช่คำแนะนำการลงทุน",
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

    ax.add_patch(plt.Rectangle((0.05, top_y1), 0.90, top_y0 - top_y1, facecolor=box, edgecolor=line, linewidth=2))
    ax.add_patch(plt.Rectangle((0.05, bot_y1), 0.90, bot_y0 - bot_y1, facecolor=box, edgecolor=line, linewidth=2))

    # -------------------------
    # Top-box title = counts line
    #   ✅ add: แตะเพดาน (สูงสุด +30%)
    # -------------------------
    sep = " | "
    touch_title = f"แตะเพดาน ({_ceil_label()})"

    if (
        touch_total is not None
        and touch_shown is not None
        and hit_total is not None
        and hit_shown is not None
        and locked_total is not None
        and locked_shown is not None
    ):
        big_total = int(hit_total) - int(touch_total) - int(locked_total)
        big_shown = int(hit_shown) - int(touch_shown) - int(locked_shown)
        if big_total < 0:
            big_total = 0
        if big_shown < 0:
            big_shown = 0

        top_title = sep.join(
            [
                f"10%+ {big_shown}/{big_total}",
                f"ล็อกเพดาน {int(locked_shown)}/{int(locked_total)}",
                f"{touch_title} {int(touch_shown)}/{int(touch_total)}",
            ]
        )
    else:
        top_title = sep.join(
            [
                f"10%+ {int(theme_cnt)}",
                f"ล็อกเพดาน {int(locked_cnt)}",
                f"{touch_title} {int(touch_cnt)}",
            ]
        )

    bot_title = "หุ้นกลุ่มเดียวกัน (ไม่เข้า Top)"

    top_title_y = top_y0 - 0.02
    bot_title_y = bot_y0 - 0.02

    top_title_fit, top_title_fs = _auto_fit_left(
        top_title, 0.08, 0.92, top_title_y, layout.box_title_fs, min_fs=max(18, layout.box_title_fs - 12), weight="bold"
    )
    bot_title_fit, bot_title_fs = _auto_fit_left(
        bot_title, 0.08, 0.92, bot_title_y, layout.box_title_fs, min_fs=max(18, layout.box_title_fs - 10), weight="bold"
    )

    ax.text(0.08, top_title_y, top_title_fit, fontsize=top_title_fs, color=fg, weight="bold", ha="left", va="center")
    ax.text(0.08, bot_title_y, bot_title_fit, fontsize=bot_title_fs, color=fg, weight="bold", ha="left", va="center")

    # -------------------------
    # Rows layout
    # -------------------------
    MAX_TOP = max(1, int(rows_per_page), len(limitup_rows or []))
    MAX_BOT = max(1, int(rows_per_page), len(peer_rows or []))

    y_top, h_top = calc_rows_layout(top_y0, top_y1, MAX_TOP, two_line=True)
    y_bot, h_bot = calc_rows_layout(bot_y0, bot_y1, MAX_BOT, two_line=True)

    GAP_TUNE = 0.010
    y_top -= GAP_TUNE
    y_bot -= GAP_TUNE

    x_name, x_tag = layout.x_name, layout.x_tag
    safe_right = x_tag - 0.10

    def draw_empty_hint(y0: float, y1: float, text: str):
        cy = (y0 + y1) / 2
        ax.text(0.5, cy, text, ha="center", va="center", fontsize=layout.empty_hint_fs, color=sub, alpha=0.55)

    def _is_touch_row(r: Dict[str, Any]) -> bool:
        st = _safe_str(r.get("limitup_status") or "")
        if st == "touch":
            return True
        if bool(r.get("is_touch_only")):
            return True
        if bool(r.get("is_limitup_touch")) and (not bool(r.get("is_limitup_locked"))):
            return True
        return False

    def _badge_kind(r: Dict[str, Any]) -> str:
        """
        Prefer cli-provided badge_kind for stable coloring.
        Expected: 'locked' | 'touch' | 'surge' | 'strong' (or '')
        """
        bk = _safe_str(r.get("badge_kind") or "")
        if bk:
            return bk
        st = _safe_str(r.get("limitup_status") or "")
        if st in ("locked", "touch"):
            return st
        return "strong"

    def _badge_text_th(r: Dict[str, Any]) -> str:
        """
        Prefer status->Thai mapping. Else use badge_text already in Thai from cli.
        """
        st = _safe_str(r.get("limitup_status") or "")
        if st == "touch":
            return "แตะเพดาน"
        if st == "locked":
            return "ล็อกเพดาน"
        return _safe_str(r.get("badge_text") or "")

    def _badge_color_for(r: Dict[str, Any]) -> str:
        bk = _badge_kind(r)
        if bk == "touch":
            return tag_touch
        if bk == "locked":
            return tag_lock
        if bk == "surge":
            return tag_surge
        return tag_strong

    def draw_rows(rows: List[Dict[str, Any]], y_start: float, row_h: float, kind: str, max_rows: int):
        if not rows:
            if kind == "limitup":
                draw_empty_hint(top_y0, top_y1, "(ไม่มีหุ้นเด่นในหน้านี้)")
            else:
                draw_empty_hint(bot_y0, bot_y1, "(ไม่มีหุ้นกลุ่มเดียวกันในหน้านี้)")
            return

        max_rows = max(1, int(max_rows))
        LINE_SPREAD = 0.22
        SEP_POS = 0.44

        for i, r in enumerate(rows[:max_rows]):
            y = y_start - i * row_h

            line1 = _safe_str(r.get("line1") or "")
            line2 = _safe_str(r.get("line2") or "")

            y1 = y + row_h * LINE_SPREAD
            y2 = y - row_h * LINE_SPREAD

            fit1 = _ellipsis_fit_left(line1, x_name, safe_right, y1, layout.row_name_fs, weight="medium")
            fit2 = _ellipsis_fit_left(line2, x_name, safe_right, y2, layout.row_line2_fs, weight="normal")

            ax.text(x_name, y1, fit1, ha="left", va="center", fontsize=layout.row_name_fs, color=fg, weight="medium")
            ax.text(x_name, y2, fit2, ha="left", va="center", fontsize=layout.row_line2_fs, color=line2_color, alpha=0.95)

            # Right badges (top line)
            if kind == "limitup":
                badge_show = _badge_text_th(r)
                badge_color = _badge_color_for(r)

                if badge_show:
                    ax.text(
                        x_tag,
                        y1,
                        badge_show,
                        ha="right",
                        va="center",
                        fontsize=layout.row_tag_fs,
                        color="white",
                        weight="bold",
                        bbox=dict(
                            boxstyle="round,pad=0.35",
                            facecolor=badge_color,
                            edgecolor="none",
                            alpha=0.9,
                        ),
                    )

            # Second line pill (Bloomberg rule)
            if kind == "limitup" and _is_touch_row(r):
                ax.text(
                    x_tag,
                    y2,
                    _ceil_label(),
                    ha="right",
                    va="center",
                    fontsize=layout.row_tag_fs,
                    color="white",
                    weight="bold",
                    bbox=dict(
                        boxstyle="round,pad=0.30",
                        facecolor=tag_touch,
                        edgecolor="none",
                        alpha=0.9,
                    ),
                )
            else:
                ret_show = float(r.get("ret") or 0.0)
                if abs(ret_show) > 1e-12:
                    ret_pct = ret_show * 100.0
                    color = get_ret_color(ret_show, theme) if ret_show > 0 else down_color
                    sign = "+" if ret_show > 0 else ""
                    ax.text(
                        x_tag,
                        y2,
                        f"{sign}{ret_pct:.1f}%",
                        ha="right",
                        va="center",
                        fontsize=layout.row_tag_fs,
                        color="white",
                        weight="bold",
                        bbox=dict(
                            boxstyle="round,pad=0.30",
                            facecolor=color,
                            edgecolor="none",
                            alpha=0.9,
                        ),
                    )

            if i < min(len(rows), max_rows) - 1:
                ax.plot([0.08, 0.91], [y - row_h * SEP_POS, y - row_h * SEP_POS], color=line, linewidth=1, alpha=0.5)

        if kind == "peer" and has_more_peers:
            hint_y = (y_start - (min(len(rows), max_rows) - 1) * row_h) - row_h * 0.75
            ax.text(
                0.5,
                hint_y,
                "(ยังมีรายการเพิ่มเติมที่ไม่แสดง)",
                ha="center",
                va="top",
                fontsize=max(layout.footer_fs_2 + 6, 26),
                color=sub,
                alpha=0.85,
                weight="bold",
            )

    draw_rows(limitup_rows, y_top, h_top, "limitup", max_rows=MAX_TOP)
    draw_rows(peer_rows, y_bot, h_bot, "peer", max_rows=MAX_BOT)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100, facecolor=bg)
    plt.close(fig)