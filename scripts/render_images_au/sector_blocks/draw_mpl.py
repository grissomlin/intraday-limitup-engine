# scripts/render_images_au/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from .layout import LayoutSpec, calc_rows_layout

# =============================================================================
# Optional shared time note builder
# =============================================================================
try:
    from scripts.render_images_common.time_note import build_time_note as _build_time_note  # type: ignore
except Exception:
    _build_time_note = None  # type: ignore

# =============================================================================
# Timezone helpers (fix "Updated" showing wrong time/date)
# =============================================================================
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore

from datetime import datetime, timezone, timedelta


# =============================================================================
# Font (keep CJK-safe because you still have Chinese line2 sometimes)
# =============================================================================
def setup_chinese_font() -> str | None:
    # safe font setup (harmless for EN markets, helpful if mixed chars)
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
            "Noto Sans CJK KR",
            "Noto Sans",
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


def parse_cutoff(payload: Dict[str, Any]) -> str:
    # ✅ AU keeps effective trading day first
    ymd = str(payload.get("ymd_effective") or payload.get("ymd") or payload.get("bar_date") or "").strip()
    return ymd or ""


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _parse_utc_offset(s: str) -> Optional[timedelta]:
    """
    Parse "+11:00" / "-05:30" -> timedelta.
    """
    ss = _safe_str(s)
    if not ss:
        return None
    try:
        sign = 1
        if ss.startswith("-"):
            sign = -1
            ss = ss[1:]
        elif ss.startswith("+"):
            ss = ss[1:]

        if ":" in ss:
            hh, mm = ss.split(":", 1)
        else:
            hh, mm = ss, "0"

        h = int(hh)
        m = int(mm)
        return sign * timedelta(hours=h, minutes=m)
    except Exception:
        return None


def _compact_utc_offset_str(td: Optional[timedelta]) -> str:
    """
    timedelta -> "+11" / "-05" / "+09:30"
    (minutes==00 => no ':00')
    """
    if td is None:
        return ""
    try:
        total_seconds = int(td.total_seconds())
        sign = "+" if total_seconds >= 0 else "-"
        total_seconds = abs(total_seconds)
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        if mm == 0:
            return f"{sign}{hh:02d}"
        return f"{sign}{hh:02d}:{mm:02d}"
    except Exception:
        return ""


def _get_market_tzinfo(payload: Dict[str, Any]) -> timezone | Any:
    """
    Prefer real ZoneInfo("Australia/Sydney") (DST aware).
    If ZoneInfo missing / tzdb missing (common on Windows), fallback to fixed UTC offset
    from payload meta (market_utc_offset). If that is missing, fallback to +10:00.
    """
    meta = payload.get("meta") or {}
    meta_time = (meta.get("time") or {}) if isinstance(meta, dict) else {}

    # Preferred tz name
    market_tz = (
        _safe_str(meta_time.get("market_tz") or "")
        or _safe_str(meta.get("market_tz") or meta.get("tz") or "")
        or "Australia/Sydney"
    )

    # Try ZoneInfo first (DST correct)
    if ZoneInfo is not None:
        try:
            return ZoneInfo(market_tz)
        except Exception:
            pass

    # Fallback: fixed offset

    offset_str = (
        _safe_str(meta_time.get("market_tz_offset") or "")
        or _safe_str(meta.get("market_tz_offset") or "")
        or _safe_str(meta_time.get("market_utc_offset") or "")
        or _safe_str(meta.get("market_utc_offset") or "")
    )
    td = _parse_utc_offset(offset_str)
    if td is None:
        td = timedelta(hours=10)
    return timezone(td)


# ------------------------------
# ISO parsing + market-time conversion
# ------------------------------
def _parse_iso_dt(s: str) -> Optional[datetime]:
    """
    Parse ISO datetime string (supports trailing 'Z').
    Returns aware datetime if tz info exists; else naive.
    """
    ss = _safe_str(s)
    if not ss:
        return None
    try:
        if ss.endswith("Z"):
            ss = ss[:-1] + "+00:00"
        return datetime.fromisoformat(ss)
    except Exception:
        return None


def _to_market_dt(dt_any: str, *, tzinfo: Any) -> Optional[datetime]:
    """
    Convert dt_any (ISO string) to market timezone.
    If dt_any is naive, assume UTC.
    """
    if not dt_any:
        return None
    dt = _parse_iso_dt(dt_any)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return dt.astimezone(tzinfo)
    except Exception:
        return None


def _trade_ymd(payload: Dict[str, Any]) -> str:
    """
    Trading/data date shown to users.
    Prefer ymd_effective then ymd.
    """
    v = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or payload.get("bar_date") or payload.get("date") or "")
    return v


def get_market_time_info(payload: Dict[str, Any]) -> Tuple[str, str]:
    """
    ✅ AU sector subtitle becomes 2 lines (TW-style):

      Australia Data date YYYY-MM-DD
      Updated YYYY-MM-DD HH:MM (UTC+XX)

    - trade date uses ymd_effective/ymd (payload driven)
    - updated datetime uses pipeline timestamps, converted to AU market tz
    - tz display shows "(UTC+11)" (no :00 when minutes==00)
    """
    meta = payload.get("meta") or {}
    meta_time = (meta.get("time") or {}) if isinstance(meta, dict) else {}

    tzinfo = _get_market_tzinfo(payload)

    # --- choose updated datetime (avoid local machine/Taiwan time leakage) ---
    chosen_dt: Optional[datetime] = None

    # 1) already-market-time timestamp if pipeline provides it
    updated_market_str = _safe_str(meta.get("pipeline_finished_at_market") or "")
    if updated_market_str:
        chosen_dt = _to_market_dt(updated_market_str, tzinfo=tzinfo)

    # 2) UTC timestamp -> convert to market timezone
    if chosen_dt is None:
        updated_utc = (
            _safe_str(meta.get("pipeline_finished_at_utc") or "")
            or _safe_str(meta_time.get("market_finished_at_utc") or "")
        )
        if updated_utc:
            chosen_dt = _to_market_dt(updated_utc, tzinfo=tzinfo)

    # 3) other timestamp fields
    if chosen_dt is None:
        updated_any = (
            _safe_str(meta_time.get("market_finished_at") or "")
            or _safe_str(payload.get("generated_at") or "")
            or _safe_str(meta.get("pipeline_finished_at") or "")
        )
        if updated_any:
            chosen_dt = _to_market_dt(updated_any, tzinfo=tzinfo)

    # 4) fallback: render-time (UTC) -> market tz
    if chosen_dt is None:
        chosen_dt = datetime.now(timezone.utc).astimezone(tzinfo)

    trade_ymd = _trade_ymd(payload)
    upd_ymd = chosen_dt.strftime("%Y-%m-%d")
    upd_hm = chosen_dt.strftime("%H:%M")

    # tz offset: prefer meta.time.market_tz_offset if exists, else derive from chosen_dt
    off_td: Optional[timedelta] = None
    off_str = _safe_str(meta_time.get("market_tz_offset") or meta.get("market_tz_offset") or "")
    if off_str:
        off_td = _parse_utc_offset(off_str)
    if off_td is None:
        try:
            off_td = chosen_dt.utcoffset()
        except Exception:
            off_td = None

    off_compact = _compact_utc_offset_str(off_td)
    tz_disp = f"(UTC{off_compact})" if off_compact else ""

    # line1: market label + data date (your request)
    line1 = f"Australia Data date {trade_ymd}".strip()

    # line2: Updated ymd hm + tz
    # (tz without :00 already handled)
    if tz_disp:
        line2 = f"Updated {upd_ymd} {upd_hm} {tz_disp}".strip()
    else:
        line2 = f"Updated {upd_ymd} {upd_hm}".strip()

    return trade_ymd, f"{line1}\n{line2}".strip()


# =============================================================================
# Color helpers
# =============================================================================
def get_ret_color(ret: float, theme: str = "light") -> str:
    if theme == "dark":
        return "#40c057" if ret >= 0 else "#ff6b6b"
    return "#2f9e44" if ret >= 0 else "#c92a2a"


def pick_big_tag(ret_decimal: float) -> Tuple[str, str]:
    # ✅ 6 tiers (no-limit markets: align US/UK intent)
    # 10-20, 20-30, 30-40, 40-50, 50-100, 100+
    if ret_decimal >= 1.00:
        return ("MOON", "#f59f00")        # 100%+
    if ret_decimal >= 0.50:
        return ("ROCKET", "#ff6b6b")      # 50-100
    if ret_decimal >= 0.40:
        return ("RALLY", "#fa5252")       # 40-50
    if ret_decimal >= 0.30:
        return ("SURGE", "#e03131")       # 30-40
    if ret_decimal >= 0.20:
        return ("JUMP", "#15aabf")        # 20-30
    return ("MOVER", "#4dabf7")           # 10-20


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
    theme: str = "light",
    time_note: str = "",
    has_more_peers: bool = False,
    hit_shown: Optional[int] = None,
    hit_total: Optional[int] = None,
    touch_shown: Optional[int] = None,
    touch_total: Optional[int] = None,
    sector_shown_total: Optional[int] = None,
    sector_all_total: Optional[int] = None,
):
    setup_chinese_font()

    theme = (theme or "dark").strip().lower()
    if theme == "light":
        bg = "#eef3f6"
        fg = "#111111"
        sub = "#555555"
        box = "#f7f7f7"
        line = "#cfd8e3"
        tag_theme_touch = "#7b1fa2"
        line2_color = "#444444"
    else:
        bg = "#0f0f1e"
        fg = "#ffffff"
        sub = "#999999"
        box = "#1a1a2e"
        line = "#2d2d44"
        tag_theme_touch = "#9c27b0"
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

    def _center_text_width_px(s: str, x: float, y: float, fontsize: int, weight: str = "bold") -> float:
        _ensure_renderer()
        t = ax.text(
            x, y, s,
            ha="center", va="top",
            fontsize=fontsize,
            color=fg,
            weight=weight,
            alpha=0.0,
        )
        bb = t.get_window_extent(renderer=renderer)
        t.remove()
        return float(bb.width)

    def _avail_width_px(x0: float, x1: float, y: float) -> float:
        p0 = ax.transData.transform((x0, y))
        p1 = ax.transData.transform((x1, y))
        return max(1.0, (p1[0] - p0[0]))

    def _fit_center_ellipsis(text: str, x0: float, x1: float, y: float, fontsize: int) -> str:
        """Center text, ellipsize to fit [x0, x1]."""
        s = (text or "").strip()
        if not s:
            return ""

        avail = _avail_width_px(x0, x1, y)
        if _center_text_width_px(s, 0.5, y, fontsize=fontsize, weight="bold") <= avail:
            return s

        ell = "..."
        lo, hi = 0, len(s)
        best = ell
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = s[:mid].rstrip()
            cand = (cand + ell) if cand else ell
            if _center_text_width_px(cand, 0.5, y, fontsize=fontsize, weight="bold") <= avail:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    def _fit_center_shrink(text: str, x0: float, x1: float, y: float, fontsize: int, min_fs: int = 18) -> int:
        """Shrink fontsize until it fits [x0, x1]."""
        s = (text or "").strip()
        if not s:
            return fontsize
        fs = int(fontsize)
        avail = _avail_width_px(x0, x1, y)
        while fs > min_fs and _center_text_width_px(s, 0.5, y, fontsize=fs, weight="bold") > avail:
            fs -= 2
        return fs

    # -------------------------
    # Page indicator
    # -------------------------
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

    # -------------------------
    # Title (auto ellipsis, reserve space for page indicator)
    # -------------------------
    title_x0 = 0.06
    title_x1 = 0.90 if page_total > 1 else 0.94
    title_y = layout.header_title_y

    title_txt = _fit_center_ellipsis(sector, title_x0, title_x1, title_y, fontsize=layout.title_fs)

    ax.text(
        0.5,
        title_y,
        title_txt,
        ha="center",
        va="top",
        fontsize=layout.title_fs,
        color=fg,
        weight="bold",
    )

    # -------------------------
    # Subtitle (support 2 lines like TW)
    # -------------------------
    sub_block = (time_note or "").strip()
    if sub_block:
        lines = [ln.strip() for ln in sub_block.split("\n") if ln.strip()]
        # line1 at header_subtitle_y, line2 at header_subtitle_line2_y
        if len(lines) >= 1:
            sub_x0, sub_x1 = 0.06, 0.94
            y1 = layout.header_subtitle_y
            fs1 = _fit_center_shrink(lines[0], sub_x0, sub_x1, y1, fontsize=layout.subtitle_fs, min_fs=16)
            ax.text(
                0.5,
                y1,
                lines[0],
                ha="center",
                va="top",
                fontsize=fs1,
                color=sub,
                weight="bold",
                alpha=0.90,
            )

        if len(lines) >= 2:
            sub_x0, sub_x1 = 0.06, 0.94
            y2 = getattr(layout, "header_subtitle_line2_y", layout.header_subtitle_y - 0.040)
            fs2_base = getattr(layout, "subtitle_line2_fs", max(16, layout.subtitle_fs - 4))
            fs2 = _fit_center_shrink(lines[1], sub_x0, sub_x1, y2, fontsize=fs2_base, min_fs=14)
            ax.text(
                0.5,
                y2,
                lines[1],
                ha="center",
                va="top",
                fontsize=fs2,
                color=sub,
                weight="bold",
                alpha=0.90,
            )

    # Footer (align with US wording/case)
    ax.text(
        0.05,
        layout.footer_y2,
        "Source: public market data | For information only, not financial advice.",
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

    ax.text(
        0.08,
        bot_title_y,
        "Peers (not Big +10%)",
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
        ax.text(0.5, cy, text, ha="center", va="center", fontsize=layout.empty_hint_fs, color=sub, alpha=0.55)

    def draw_rows(rows: List[Dict[str, Any]], y_start: float, row_h: float, kind: str):
        if not rows:
            if kind == "limitup":
                draw_empty_hint(top_y0, top_y1, "(No Big/Touched on this page)")
            else:
                draw_empty_hint(bot_y0, bot_y1, "(No data on this page)")
            return

        n = min(len(rows), MAX_ROWS_PER_BOX)
        safe_right = x_tag - 0.18

        for i in range(n):
            y = y_start - i * row_h
            r = rows[i]

            line1 = str(r.get("line1") or "").strip()
            line2 = str(r.get("line2") or "").strip()

            if layout.two_line and line2:
                y1 = y + row_h * 0.22
                y2 = y - row_h * 0.22

                fit1 = _ellipsis_fit(line1, x_name, safe_right, y1, layout.row_name_fs, weight="medium")
                ax.text(
                    x_name, y1, fit1,
                    ha="left", va="center",
                    fontsize=layout.row_name_fs,
                    color=fg, weight="medium",
                )

                fit2 = _ellipsis_fit(line2, x_name, safe_right, y2, layout.row_line2_fs, weight="normal")
                ax.text(
                    x_name, y2, fit2,
                    ha="left", va="center",
                    fontsize=layout.row_line2_fs,
                    color=line2_color, weight="normal",
                    alpha=0.95,
                )

                if kind == "limitup":
                    is_touch = bool(r.get("touched_only")) or (str(r.get("limitup_status") or "") == "touch")
                    if is_touch:
                        tag_text = "Touched 10%"
                        tag_bg = tag_theme_touch
                    else:
                        ret_pct = float(r.get("ret_pct", 0.0) or 0.0)
                        ret_decimal = ret_pct / 100.0
                        tag_text, tag_bg = pick_big_tag(ret_decimal)

                    ax.text(
                        x_tag, y1, tag_text,
                        ha="right", va="center",
                        fontsize=layout.row_tag_fs,
                        color="white", weight="bold",
                        bbox=dict(
                            boxstyle=f"round,pad={layout.badge_pad_limitup}",
                            facecolor=tag_bg,
                            alpha=0.92,
                            edgecolor="none",
                        ),
                    )

                    ret_pct = float(r.get("ret_pct", 0.0) or 0.0)
                    ret_decimal = ret_pct / 100.0
                    ret_color = get_ret_color(ret_decimal, theme)

                    if ret_decimal >= 1.00:
                        tag_text2 = f"+{ret_pct:.0f}%"
                        tag_fontsize = layout.row_tag_fs - 2
                    elif ret_decimal >= 0.10:
                        tag_text2 = f"+{ret_pct:.1f}%"
                        tag_fontsize = layout.row_tag_fs
                    else:
                        tag_text2 = f"+{ret_pct:.2f}%"
                        tag_fontsize = layout.row_tag_fs

                    if ret_pct < 0:
                        tag_text2 = f"{ret_pct:.1f}%"

                    ax.text(
                        x_tag, y2, tag_text2,
                        ha="right", va="center",
                        fontsize=tag_fontsize,
                        color="white", weight="bold",
                        bbox=dict(
                            boxstyle=f"round,pad={layout.badge_pad_peer}",
                            facecolor=ret_color,
                            alpha=0.9,
                            edgecolor="none",
                        ),
                    )

                else:
                    ret = float(r.get("ret", 0.0) or 0.0)
                    ret_pct = ret * 100.0
                    ret_color = get_ret_color(ret, theme)

                    if ret >= 1.00:
                        tag_text2 = f"+{ret_pct:.0f}%"
                        tag_fontsize = layout.row_tag_fs - 2
                    elif ret >= 0.10:
                        tag_text2 = f"+{ret_pct:.1f}%"
                        tag_fontsize = layout.row_tag_fs
                    else:
                        tag_text2 = f"+{ret_pct:.2f}%"
                        tag_fontsize = layout.row_tag_fs

                    if ret_pct < 0:
                        tag_text2 = f"{ret_pct:.1f}%"

                    ax.text(
                        x_tag, y1, tag_text2,
                        ha="right", va="center",
                        fontsize=tag_fontsize,
                        color="white", weight="bold",
                        bbox=dict(
                            boxstyle=f"round,pad={layout.badge_pad_peer}",
                            facecolor=ret_color,
                            alpha=0.9,
                            edgecolor="none",
                        ),
                    )

            else:
                fit1 = _ellipsis_fit(line1 or str(r.get("name", "")), x_name, safe_right, y, layout.row_name_fs)
                ax.text(
                    x_name, y, fit1,
                    ha="left", va="center",
                    fontsize=layout.row_name_fs,
                    color=fg, weight="medium",
                )

            if i < n - 1:
                ax.plot([0.08, 0.91], [y - row_h * 0.50, y - row_h * 0.50], color=line, linewidth=1, alpha=0.5)

        if kind == "peer" and has_more_peers:
            hint_y = (y_start - (n - 1) * row_h) - row_h * 0.75
            ax.text(
                0.5, hint_y,
                "(More rows not shown)",
                ha="center", va="top",
                fontsize=max(layout.footer_fs_2 + 6, 26),
                color=sub, alpha=0.85, weight="bold",
            )

    draw_rows(limitup_rows, y_start_top, row_h_top, "limitup")
    draw_rows(peer_rows, y_start_bot, row_h_bot, "peer")

    # -------------------------
    # Top box title (counts + percent of sector) - align with US wording
    # -------------------------
    use_precise = (
        hit_total is not None and touch_total is not None and
        hit_shown is not None and touch_shown is not None
    )

    if use_precise:
        big_n = int(hit_total or 0)
        touch_n = int(touch_total or 0)
    else:
        big_n = int(theme_cnt or 0)
        touch_n = int(touch_cnt or 0)

    pct_part = ""
    try:
        if sector_shown_total is not None and sector_all_total:
            shown = int(sector_shown_total)
            total = int(sector_all_total)
            if total > 0:
                pct = round(shown / total * 100.0)
                pct_part = f" ({pct:.0f}% of sector)"
    except Exception:
        pct_part = ""

    top_title = f"Big +10% {big_n}  /  Touched 10% {touch_n}{pct_part}"

    # shrink if too long (left aligned)
    fs = int(layout.box_title_fs)
    _ensure_renderer()
    x_left = 0.08
    x_right = 0.94
    y = top_title_y

    avail_px = abs(ax.transData.transform((x_right, y))[0] - ax.transData.transform((x_left, y))[0])

    def _left_text_width_px(s: str, x: float, y: float, fontsize: int, weight: str = "bold") -> float:
        _ensure_renderer()
        t = ax.text(
            x, y, s,
            ha="left", va="center",
            fontsize=fontsize,
            color=fg,
            weight=weight,
            alpha=0.0,
        )
        bb = t.get_window_extent(renderer=renderer)
        t.remove()
        return float(bb.width)

    while fs > 18 and _left_text_width_px(top_title, x_left, y, fontsize=fs, weight="bold") > avail_px:
        fs -= 2

    ax.text(
        x_left,
        top_title_y,
        top_title,
        ha="left",
        va="center",
        fontsize=fs,
        color=fg,
        weight="bold",
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100, facecolor=bg)
    plt.close(fig)