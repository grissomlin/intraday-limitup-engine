# scripts/render_images_kr/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from .layout import LayoutSpec, calc_rows_layout

EPS = 1e-12


# =============================================================================
# Font (KR + Han safe)
# =============================================================================
def setup_korean_font() -> str | None:
    """
    목표: 한국어 + 한자권(간체/번체)까지 안전하게 표시.
    최선: Noto Sans CJK KR (Hangul + Han 포함)
    """
    font_candidates = [
        "Noto Sans CJK KR",
        "Noto Sans CJK TC",
        "Malgun Gothic",
        "AppleGothic",
        "NanumGothic",
        "Arial Unicode MS",
        "Microsoft JhengHei",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    for f in font_candidates:
        if f in available:
            plt.rcParams["font.sans-serif"] = [f]
            plt.rcParams["axes.unicode_minus"] = False
            return f
    return None


# =============================================================================
# Utils
# =============================================================================
def parse_cutoff(payload: Dict[str, Any]) -> str:
    return str(payload.get("ymd_effective") or payload.get("ymd") or "").strip()


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _format_trade_and_update_note(payload: Dict[str, Any]) -> str:
    """
    ✅ 일본처럼 2줄로 분리

    1) trade date (KR, 한국어로)
       예: "한국 거래일: 2026-01-30"

    2) update (date + time + timezone 필수)
       예: "업데이트: 2026-01-30 11:02 (UTC+09:00)"

    우선순위 (meta.time):
      - market_finished_at      (YYYY-MM-DD HH:MM)
      - market_tz_offset        ("+09:00")  -> "UTC+09:00"
      - market_utc_offset       ("+09:00")  -> "UTC+09:00" (alias)
    fallback:
      - payload["generated_at"] 앞 16자리 -> YYYY-MM-DD HH:MM
      - tz 없으면 괄호 없이 표기
    """
    # --- trade date ---
    trade_ymd = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    trade_line = f"한국 거래일: {trade_ymd}" if trade_ymd else ""

    # --- update time ---
    meta = payload.get("meta") or {}
    t = meta.get("time") or {}

    produced_at = _safe_str(t.get("market_finished_at"))

    # timezone label: prefer offset -> "UTC+09:00"
    off = _safe_str(t.get("market_tz_offset") or t.get("market_utc_offset"))
    tz_label = f"UTC{off}" if off else ""

    if not produced_at:
        ga = _safe_str(payload.get("generated_at"))
        if "T" in ga and len(ga) >= 16:
            produced_at = ga.replace("T", " ", 1)[:16]
        elif " " in ga and len(ga) >= 16:
            produced_at = ga[:16]

    update_line = ""
    if produced_at:
        if tz_label:
            update_line = f"업데이트: {produced_at} ({tz_label})"
        else:
            update_line = f"업데이트: {produced_at}"

    # join (only include non-empty lines)
    lines = [x for x in [trade_line, update_line] if _safe_str(x)]
    return "\n".join(lines)


def get_market_time_info(payload: Dict[str, Any]) -> Tuple[str, str]:
    # cutoff/trade ymd
    ymd = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    # ✅ 2-line subtitle
    time_note = _format_trade_and_update_note(payload)
    return ymd, time_note


def get_ret_color(ret: float, theme: str = "light") -> str:
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
    # ✅ NEW: ceiling pct label in top-box title (KR = 30% by default)
    ceiling_pct: float = 0.30,
):
    setup_korean_font()

    # -------------------------
    # Theme
    # -------------------------
    if theme == "light":
        bg, fg, sub = "#eef3f6", "#111111", "#555555"
        box, line = "#f7f7f7", "#cfd8e3"
        tag_theme = "#7b1fa2"       # locked / touch / ceiling pill
        tag_surge_10 = "#3949ab"    # 강세 (10%~20%)
        tag_surge_20 = "#6a1b9a"    # 급등 (20%~30%)
        tag_surge_30 = "#c62828"    # ✅ rare: 30%+ (red)
        line2_color = "#444444"
        down_color = "#c62828"
    else:
        bg, fg, sub = "#0f0f1e", "#ffffff", "#999999"
        box, line = "#1a1a2e", "#2d2d44"
        tag_theme = "#9c27b0"       # locked / touch / ceiling pill
        tag_surge_10 = "#5c6bc0"    # 강세 (10%~20%)
        tag_surge_20 = "#ab47bc"    # 급등 (20%~30%)
        tag_surge_30 = "#ef5350"    # ✅ rare: 30%+ (red)
        line2_color = "#cfcfcf"
        down_color = "#ef5350"

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor=bg)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # -------- renderer helpers (ellipsis / auto-fit) --------
    renderer = None

    def _ensure_renderer():
        nonlocal renderer
        if renderer is None:
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()

    def _avail_px(x_left: float, x_right: float, y: float) -> float:
        _ensure_renderer()
        p0 = ax.transData.transform((x_left, y))
        p1 = ax.transData.transform((x_right, y))
        return max(1.0, (p1[0] - p0[0]))

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

    def _fit_ellipsis_center(text: str, y: float, fontsize: int, max_x_left: float = 0.06, max_x_right: float = 0.94) -> str:
        _ensure_renderer()
        text = _safe_str(text)
        if not text:
            return ""

        avail = _avail_px(max_x_left, max_x_right, y)
        if _text_width_px(text, 0.5, y, fontsize=fontsize, ha="center", va="top", weight="bold") <= avail:
            return text

        base = text
        ell = "..."
        lo, hi = 0, len(base)
        best = ell
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = base[:mid].rstrip()
            cand = (cand + ell) if cand else ell
            if _text_width_px(cand, 0.5, y, fontsize=fontsize, ha="center", va="top", weight="bold") <= avail:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    def _ellipsis_fit_left(text: str, x_left: float, x_right: float, y: float, fontsize: int, *, weight: str = "medium") -> str:
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
        폭에 맞게 글자 크기를 자동 축소. 그래도 길면 min_fs에서 ellipsis.
        return (text_to_draw, fontsize)
        """
        _ensure_renderer()
        text = _safe_str(text)
        if not text:
            return "", int(fs)

        avail = _avail_px(x_left, x_right, y)
        cur = int(fs)
        while cur >= int(min_fs):
            w = _text_width_px(text, x_left, y, fontsize=cur, ha="left", va="center", weight=weight)
            if w <= avail:
                return text, cur
            cur -= 1

        return _ellipsis_fit_left(text, x_left, x_right, y, int(min_fs), weight=weight), int(min_fs)

    def _ceil_label() -> str:
        try:
            p = int(round(float(ceiling_pct) * 100.0))
            return f"최대 +{p}%"
        except Exception:
            return "최대 +30%"

    def _is_touch_row(r: Dict[str, Any]) -> bool:
        st = _safe_str(r.get("limitup_status") or "")
        if st == "touch":
            return True
        if bool(r.get("is_touch_only")):
            return True
        if bool(r.get("is_limitup_touch")) and (not bool(r.get("is_limitup_locked"))):
            return True
        return False

    # -------------------------
    # Title (Sector)
    # -------------------------
    sector_show = _fit_ellipsis_center(_safe_str(sector), layout.header_title_y, layout.title_fs)
    ax.text(0.5, layout.header_title_y, sector_show, ha="center", va="top", fontsize=layout.title_fs, color=fg, weight="bold")

    # page indicator
    if page_total > 1:
        ax.text(0.97, layout.header_title_y, f"{page_idx}/{page_total}", ha="right", va="top", fontsize=layout.page_fs, color=sub, weight="bold")

    # -------------------------
    # Header subtitle: ✅ 2 lines (trade date + update)
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
            linespacing=1.25,  # ✅ nicer spacing for 2 lines
        )

    # -------------------------
    # Footer
    # -------------------------
    ax.text(
        0.05,
        layout.footer_y2,
        "데이터: 공개 자료 기반 | 참고용(투자 조언 아님)",
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
    # Top-box title = counts line (auto-fit)
    # -------------------------
    sep = " | "
    touch_title = f"터치({_ceil_label()})"

    if (
        hit_total is not None
        and hit_shown is not None
        and touch_total is not None
        and touch_shown is not None
        and locked_total is not None
        and locked_shown is not None
    ):
        surge_total = int(hit_total) - int(touch_total) - int(locked_total)
        surge_shown = int(hit_shown) - int(touch_shown) - int(locked_shown)
        if surge_total < 0:
            surge_total = 0
        if surge_shown < 0:
            surge_shown = 0

        top_title = sep.join(
            [
                f"급등(10%+) {surge_shown}/{surge_total}",
                f"상한가 {int(locked_shown)}/{int(locked_total)}",
                f"{touch_title} {int(touch_shown)}/{int(touch_total)}",
            ]
        )
    else:
        top_title = sep.join(
            [
                f"급등(10%+) {int(theme_cnt)}",
                f"상한가 {int(locked_cnt)}",
                f"{touch_title} {int(touch_cnt)}",
            ]
        )

    bot_title = "동일 업종: 이벤트 제외"

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

    def draw_rows(rows: List[Dict[str, Any]], y_start: float, row_h: float, kind: str, max_rows: int):
        if not rows:
            if kind == "limitup":
                draw_empty_hint(top_y0, top_y1, "(이 페이지에 이벤트 종목 없음)")
            else:
                draw_empty_hint(bot_y0, bot_y1, "(이 페이지에 보충 종목 없음)")
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

            # Right badges (top line)  ✅ 3-tier surge colors
            if kind == "limitup":
                st = _safe_str(r.get("limitup_status") or "")
                badge = _safe_str(r.get("badge_text") or "")
                retv = float(r.get("ret") or 0.0)

                if st == "touch":
                    badge_show = "터치"
                    badge_bg = tag_theme
                elif st == "locked":
                    badge_show = "상한가"
                    badge_bg = tag_theme
                else:
                    badge_show = badge
                    # 10~20, 20~30, 30+ (red)
                    if retv >= 0.30:
                        badge_bg = tag_surge_30
                    elif retv >= 0.20:
                        badge_bg = tag_surge_20
                    else:
                        badge_bg = tag_surge_10

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
                        bbox=dict(boxstyle="round,pad=0.35", facecolor=badge_bg, edgecolor="none", alpha=0.9),
                    )

            # Second-line pill (✅ Scheme A)
            if kind == "limitup" and _is_touch_row(r):
                # touch-only: show ceiling label instead of close ret
                ax.text(
                    x_tag,
                    y2,
                    _ceil_label(),
                    ha="right",
                    va="center",
                    fontsize=layout.row_tag_fs,
                    color="white",
                    weight="bold",
                    bbox=dict(boxstyle="round,pad=0.30", facecolor=tag_theme, edgecolor="none", alpha=0.9),
                )
            else:
                # show close ret (support negative too)
                ret_show = float(r.get("ret") or 0.0)
                if abs(ret_show) > EPS:
                    ret_pct = ret_show * 100.0
                    if ret_show > 0:
                        color = get_ret_color(ret_show, theme)
                        sign = "+"
                    else:
                        color = down_color
                        sign = ""  # minus sign already in format
                    ax.text(
                        x_tag,
                        y2,
                        f"{sign}{ret_pct:.1f}%",
                        ha="right",
                        va="center",
                        fontsize=layout.row_tag_fs,
                        color="white",
                        weight="bold",
                        bbox=dict(boxstyle="round,pad=0.30", facecolor=color, edgecolor="none", alpha=0.9),
                    )

            if i < min(len(rows), max_rows) - 1:
                ax.plot([0.08, 0.91], [y - row_h * SEP_POS, y - row_h * SEP_POS], color=line, linewidth=1, alpha=0.5)

        if kind == "peer" and has_more_peers:
            hint_y = (y_start - (min(len(rows), max_rows) - 1) * row_h) - row_h * 0.75
            ax.text(
                0.5,
                hint_y,
                "(추가 종목 더 있음)",
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