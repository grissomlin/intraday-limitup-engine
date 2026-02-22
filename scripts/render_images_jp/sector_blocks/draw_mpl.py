# scripts/render_images_jp/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from .layout import LayoutSpec, calc_rows_layout

# =============================================================================
# Font
# =============================================================================
def setup_cjk_font() -> str | None:
    """JP needs JP-capable font; include Noto Sans CJK JP first if available."""
    try:
        font_candidates = [
            "Noto Sans CJK JP",
            "Noto Sans JP",
            "Yu Gothic",
            "Meiryo",
            "MS Gothic",
            "Arial Unicode MS",
            # fallbacks (CJK-safe)
            "Microsoft YaHei",
            "Microsoft JhengHei",
            "PingFang TC",
            "PingFang SC",
            "Noto Sans CJK TC",
            "Noto Sans CJK SC",
            "SimHei",
            "WenQuanYi Zen Hei",
            "Noto Sans",
            "DejaVu Sans",
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
# Payload helpers
# =============================================================================
def parse_cutoff(payload: Dict[str, Any]) -> str:
    # ✅ JP: prefer effective trading day first
    ymd = str(payload.get("ymd_effective") or payload.get("ymd") or "").strip()
    return ymd or ""


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
        return default


def _parse_hhmm_from_iso(dt_iso: str) -> str:
    s = _safe_str(dt_iso)
    if not s:
        return ""
    try:
        if "T" in s:
            t = s.split("T", 1)[1]
        elif " " in s:
            t = s.split(" ", 1)[1]
        else:
            t = s
        return t[:5]
    except Exception:
        return ""


def _parse_ymd_from_any(dt_any: str) -> str:
    """
    Extract YYYY-MM-DD from:
      - "2026-02-20 09:35:00"
      - "2026-02-20T09:35:00+09:00"
      - "2026-02-20T00:35:00Z"
    """
    s = _safe_str(dt_any)
    if not s:
        return ""
    try:
        if "T" in s:
            return s.split("T", 1)[0][:10]
        if " " in s:
            return s.split(" ", 1)[0][:10]
        return s[:10]
    except Exception:
        return ""


def _hhmm_from_asof(asof: str) -> str:
    """
    "14:07" -> "14:07"
    "14:07:59" -> "14:07"
    """
    s = _safe_str(asof)
    if not s:
        return ""
    return s[:5]


def _short_utc_offset(offset: str) -> str:
    """
    "+09:00" -> "+9"
    "-05:00" -> "-5"
    "+08"    -> "+8"
    "+9"     -> "+9"
    """
    s = _safe_str(offset)
    if not s:
        return ""
    s = s.replace("UTC", "").replace("utc", "").strip()

    try:
        if len(s) >= 6 and (s[0] in "+-") and s[3] == ":":
            hh = int(s[0:3])  # "+09" -> 9, "-05" -> -5
            sign = "+" if hh >= 0 else "-"
            return f"{sign}{abs(hh)}"
        if len(s) >= 2 and (s[0] in "+-"):
            hh = int(s)
            sign = "+" if hh >= 0 else "-"
            return f"{sign}{abs(hh)}"
    except Exception:
        pass

    return s


def get_market_time_info(payload: Dict[str, Any]) -> Tuple[str, str]:
    """
    ✅ JP subtitle (2 lines, align overview style)

      JST（UTC+9） 2026-02-20 前場
      更新 2026-02-22 14:07

    Rules:
    - Trading day: ymd_effective (or ymd fallback)
    - Update date: payload['ymd'] (run/requested date), fallback to generated_at date, fallback to trading day
    - Update time: payload['asof'] first, fallback to pipeline_finished_at_market HH:MM, fallback to generated_at HH:MM
    """
    ymd_effective = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    slot = _safe_str(payload.get("slot") or "")
    meta = payload.get("meta") or {}

    if slot == "close":
        session = "終値"
    elif slot == "midday":
        session = "前場"
    elif slot == "open":
        session = "寄り付き"
    else:
        session = slot or "データ"

    # label: JST（UTC+9）
    tz = _safe_str(meta.get("market_tz") or meta.get("tz") or "") or "JST"
    offset = _safe_str(meta.get("market_utc_offset") or meta.get("utc_offset") or "")  # often "+09:00"
    off_short = _short_utc_offset(offset) or "+9"
    market_label = f"{tz}（UTC{off_short}）"

    # Update sources
    # 1) date: prefer payload ymd (requested/run day)
    ymd_run = _safe_str(payload.get("ymd") or "")
    gen_at = _safe_str(payload.get("generated_at") or "")
    upd_ymd = ymd_run or _parse_ymd_from_any(gen_at) or ymd_effective

    # 2) time: prefer asof (CLI)
    hhmm = _hhmm_from_asof(_safe_str(payload.get("asof") or ""))

    # fallback time from pipeline_finished_at_market / generated_at
    if not hhmm:
        updated_market = _safe_str(meta.get("pipeline_finished_at_market") or "")
        if not updated_market:
            updated_market = gen_at
        hhmm = _parse_hhmm_from_iso(updated_market)

    # Two-line note
    line1 = f"{market_label} {ymd_effective} {session}".strip() if ymd_effective else f"{market_label} {session}".strip()
    line2 = f"更新 {upd_ymd} {hhmm}".strip() if (upd_ymd or hhmm) else "更新"

    return ymd_effective, f"{line1}\n{line2}"


# =============================================================================
# Colors
# =============================================================================
def get_ret_color(ret: float, theme: str = "light") -> str:
    """Positive return color scale (blue-ish). Negative is handled separately in draw_rows()."""
    if ret >= 1.00:
        return "#1565c0" if theme == "light" else "#1e88e5"
    elif ret >= 0.50:
        return "#1976d2" if theme == "light" else "#2196f3"
    elif ret >= 0.20:
        return "#2196f3" if theme == "light" else "#42a5f5"
    else:
        return "#42a5f5" if theme == "light" else "#64b5f6"


# =============================================================================
# Main renderer
# =============================================================================
def draw_block_table(
    out_path: Path,
    *,
    layout: LayoutSpec,
    sector: str,
    cutoff: str,
    locked_cnt: int,  # S高張り付き
    touch_cnt: int,  # 一時S高
    theme_cnt: int,  # legacy arg; now treat as surge_total
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
    sector_share: Optional[float] = None,  # e.g. 0.123 => 12.3%
):
    setup_cjk_font()

    if theme == "light":
        bg = "#eef3f6"
        fg = "#111111"
        sub = "#555555"
        box = "#f7f7f7"
        line = "#cfd8e3"
        tag_theme = "#7b1fa2"
        line2_color = "#444444"
    else:
        bg = "#0f0f1e"
        fg = "#ffffff"
        sub = "#999999"
        box = "#1a1a2e"
        line = "#2d2d44"
        tag_theme = "#9c27b0"
        line2_color = "#cfcfcf"

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor=bg)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(bg)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    renderer = None

    def _ensure_renderer():
        nonlocal renderer
        if renderer is None:
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()

    def _text_width_px_center(s: str, x: float, y: float, fontsize: int, weight: str = "bold") -> float:
        _ensure_renderer()
        t = ax.text(x, y, s, ha="center", va="top", fontsize=fontsize, color=fg, weight=weight, alpha=0.0)
        bb = t.get_window_extent(renderer=renderer)
        t.remove()
        return float(bb.width)

    def _fit_center_ellipsis(text: str, y: float, fontsize: int, max_left: float = 0.05, max_right: float = 0.95) -> str:
        s = _safe_str(text)
        if not s:
            return ""
        _ensure_renderer()
        p0 = ax.transData.transform((max_left, y))
        p1 = ax.transData.transform((max_right, y))
        avail = max(1.0, (p1[0] - p0[0]))
        if _text_width_px_center(s, 0.5, y, fontsize=fontsize, weight="bold") <= avail:
            return s

        ell = "..."
        lo, hi = 0, len(s)
        best = ell
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = s[:mid].rstrip()
            cand = (cand + ell) if cand else ell
            if _text_width_px_center(cand, 0.5, y, fontsize=fontsize, weight="bold") <= avail:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    def _fit_left_fontsize(
        text: str,
        x_left: float,
        x_right: float,
        y: float,
        base_fs: int,
        min_fs: int = 22,
        weight: str = "bold",
    ) -> int:
        """Auto-shrink fontsize until the text fits between x_left~x_right (data coords)."""
        _ensure_renderer()
        t = ax.text(
            x_left, y, text, ha="left", va="center",
            fontsize=int(base_fs), color=fg, weight=weight, alpha=0.0
        )
        p0 = ax.transData.transform((x_left, y))
        p1 = ax.transData.transform((x_right, y))
        avail = max(1.0, (p1[0] - p0[0]))

        fs = int(base_fs)
        min_fs = int(min_fs)
        while fs > min_fs:
            t.set_fontsize(fs)
            bb = t.get_window_extent(renderer=renderer)
            if bb.width <= avail:
                t.remove()
                return fs
            fs -= 1
        t.remove()
        return min_fs

    # -------------------------
    # Title
    # -------------------------
    title_text = _fit_center_ellipsis(sector, layout.header_title_y, layout.title_fs, max_left=0.05, max_right=0.95)
    ax.text(
        0.5,
        layout.header_title_y,
        title_text,
        ha="center",
        va="top",
        fontsize=layout.title_fs,
        color=fg,
        weight="bold",
    )

    # -------------------------
    # Subtitle (2 lines)
    # -------------------------
    subtitle_raw = (time_note or "").strip()
    if subtitle_raw:
        lines = [s.strip() for s in subtitle_raw.split("\n") if s.strip()]

        if len(lines) >= 2:
            line1 = lines[0]
            line2 = lines[1]
        else:
            # fallback: split at ｜更新
            if "｜更新" in subtitle_raw:
                a, b = subtitle_raw.split("｜更新", 1)
                line1 = a.strip()
                line2 = ("更新 " + b.strip()).strip()
            else:
                line1 = subtitle_raw
                line2 = ""

        ax.text(
            0.5,
            layout.header_subtitle_y,
            line1,
            ha="center",
            va="top",
            fontsize=layout.subtitle_line2_fs,
            color=sub,
            weight="bold",
            alpha=0.95,
        )

        if line2:
            ax.text(
                0.5,
                layout.header_subtitle_y - 0.035,
                line2,
                ha="center",
                va="top",
                fontsize=max(14, int(layout.subtitle_line2_fs) - 2),
                color=sub,
                weight="bold",
                alpha=0.88,
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
        "データ：公開情報｜参考情報（投資助言ではありません）",
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

    # ✅ 上框標題：統計 + 業種
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
        top_title = f"S高{hs}/{ht}｜一時S高{ts}/{tt}｜10%+{gs}/{gt}"
    else:
        top_title = (
            f"S高 {hit_cnt_fallback}/{hit_cnt_fallback} ｜ "
            f"一時S高 {touch_cnt_fallback}/{touch_cnt_fallback} ｜ "
            f"10%+ {surge_cnt_fallback}/{surge_cnt_fallback}"
        )

    if sector_share is not None:
        try:
            top_title += f"｜業種内{float(sector_share) * 100.0:.1f}%"
        except Exception:
            pass

    top_title_fs = _fit_left_fontsize(
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
        "同業・未達（Top以外）",
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

    def _ellipsis_fit(text: str, x_left: float, x_right: float, y: float, fontsize: int, weight: str = "medium") -> str:
        if not text:
            return ""
        _ensure_renderer()
        t = ax.text(x_left, y, text, ha="left", va="center", fontsize=fontsize, color=fg, weight=weight, alpha=0.0)

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
        ax.text(0.5, cy, text, ha="center", va="center", fontsize=layout.empty_hint_fs, color=sub, alpha=0.55)

    def draw_rows(rows: List[Dict[str, Any]], y_start: float, row_h: float, kind: str):
        if not rows:
            if kind == "top":
                draw_empty_hint(top_y0, top_y1, "（このページの該当データなし）")
            else:
                draw_empty_hint(bot_y0, bot_y1, "（このページのデータなし）")
            return

        n = min(len(rows), MAX_ROWS_PER_BOX)
        safe_right = x_tag - 0.10

        for i in range(n):
            y = y_start - i * row_h
            r = rows[i]
            line1 = _safe_str(r.get("line1") or "")
            line2 = _safe_str(r.get("line2") or "")

            if layout.two_line and line2:
                y1 = y + row_h * 0.22
                y2 = y - row_h * 0.22

                fit1 = _ellipsis_fit(line1, x_name, safe_right, y1, layout.row_name_fs, weight="medium")
                ax.text(x_name, y1, fit1, ha="left", va="center", fontsize=layout.row_name_fs, color=fg, weight="medium")

                fit2 = _ellipsis_fit(line2, x_name, safe_right, y2, layout.row_line2_fs, weight="normal")
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

                # Right badges (status)
                badge_text = _safe_str(r.get("badge_text") or "")
                if badge_text:
                    streak = _safe_int(r.get("streak", 0), 0)

                    if badge_text == "ストップ高":
                        tag_text = f"S高{streak}連" if streak > 1 else "S高"
                    elif badge_text in ("一時S高", "一時ストップ高"):
                        tag_text = "一時S高"
                    else:
                        tag_text = badge_text

                    ax.text(
                        x_tag,
                        y1,
                        tag_text,
                        ha="right",
                        va="center",
                        fontsize=layout.row_tag_fs,
                        color="white",
                        weight="bold",
                        bbox=dict(
                            boxstyle=f"round,pad={layout.badge_pad_limitup}",
                            facecolor=tag_theme,
                            alpha=0.9,
                            edgecolor="none",
                        ),
                    )

                # Return badge
                ret_pct_raw = r.get("ret_pct", None)
                if ret_pct_raw is None:
                    ret_ratio = r.get("ret", None)
                    rr = None
                    try:
                        rr = float(ret_ratio) if ret_ratio is not None else None
                    except Exception:
                        rr = None
                    ret_pct = (rr * 100.0) if (rr is not None) else 0.0
                else:
                    ret_pct = _safe_float(ret_pct_raw, 0.0)

                ret_decimal = ret_pct / 100.0

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
                fit1 = _ellipsis_fit(line1 or _safe_str(r.get("name", "")), x_name, safe_right, y, layout.row_name_fs)
                ax.text(x_name, y, fit1, ha="left", va="center", fontsize=layout.row_name_fs, color=fg, weight="medium")

            if i < n - 1:
                ax.plot([0.08, 0.91], [y - row_h * 0.50, y - row_h * 0.50], color=line, linewidth=1, alpha=0.5)

        if kind == "peer" and has_more_peers:
            hint_y = (y_start - (n - 1) * row_h) - row_h * 0.75
            ax.text(
                0.5,
                hint_y,
                "（続きあり：未表示データ）",
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