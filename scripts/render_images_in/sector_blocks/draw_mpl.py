# scripts/render_images_in/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from .layout import LayoutSpec, calc_rows_layout
from .mpl_text import (
    ensure_renderer,
    text_width_px,
    px_to_data_dx,
)
from .mpl_pills import (
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
                plt.rcParams["font.family"] = "sans-serif"
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
        return int(float(x))
    except Exception:
        return default


def _safe_bool(x: Any) -> bool:
    try:
        if isinstance(x, bool):
            return x
        s = str(x).strip().lower()
        return s in {"1", "true", "yes", "y", "on"}
    except Exception:
        return False


def _fmt_ret_pct(ret: float) -> str:
    r = _safe_float(ret, 0.0)
    pct = (r * 100.0) if abs(r) < 1.5 else r
    return f"{pct:+.2f}%"


def get_ret_color(ret: float, theme: str) -> str:
    if (theme or "dark").strip().lower() == "dark":
        return "#ff6b6b" if ret >= 0 else "#4dabf7"
    return "#d9480f" if ret >= 0 else "#1864ab"


def _ellipsize_px(
    ax,
    fig,
    s: str,
    *,
    x_left: float,
    x_right: float,
    y: float,
    fontsize: int,
    weight: str = "bold",
) -> str:
    """
    Ellipsize by pixel width. Much more stable than char-count.
    """
    s = _safe_str(s)
    if not s:
        return ""
    p0 = ax.transData.transform((x_left, y))
    p1 = ax.transData.transform((x_right, y))
    avail = max(1.0, p1[0] - p0[0])

    if text_width_px(ax, fig, s, x=x_left, y=y, fontsize=fontsize, weight=weight) <= avail:
        return s

    ell = "…"
    lo, hi = 0, len(s)
    best = ell
    while lo <= hi:
        mid = (lo + hi) // 2
        cand0 = s[:mid].rstrip()
        cand = (cand0 + ell) if cand0 else ell
        w = text_width_px(ax, fig, cand, x=x_left, y=y, fontsize=fontsize, weight=weight)
        if w <= avail:
            best = cand
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def _fit_text_fs(
    ax,
    fig,
    s: str,
    *,
    x_left: float,
    x_right: float,
    y: float,
    fs_start: int,
    fs_min: int,
    weight: str = "bold",
) -> Tuple[str, int]:
    """
    Prefer shrinking font size to fit, only ellipsize as last resort.
    """
    s = _safe_str(s)
    if not s:
        return "", fs_start

    p0 = ax.transData.transform((x_left, y))
    p1 = ax.transData.transform((x_right, y))
    avail = max(1.0, p1[0] - p0[0])

    fs = int(fs_start)
    fs_min = int(max(10, fs_min))

    while fs >= fs_min:
        w = text_width_px(ax, fig, s, x=x_left, y=y, fontsize=fs, weight=weight)
        if w <= avail:
            return s, fs
        fs -= 1

    s2 = _ellipsize_px(ax, fig, s, x_left=x_left, x_right=x_right, y=y, fontsize=fs_min, weight=weight)
    return s2, fs_min


def _normalize_status(x: Any) -> str:
    """
    Normalize any status field into: hit / touch / big / ""
    """
    s = _safe_str(x).lower()
    if s in {"hit", "limit_hit", "locked"}:
        return "hit"
    if s in {"touch", "touched", "opened", "bomb"}:
        return "touch"
    if s in {"big", "big10", "big10+", "surge"}:
        return "big"
    return ""


def _status_from_row(r: Dict[str, Any]) -> str:
    """
    Prefer authoritative fields:
      - limitup_status / today_status
    """
    st = _normalize_status(r.get("limitup_status"))
    if st:
        return st

    st = _normalize_status(r.get("today_status"))
    if st:
        return st

    # fallback (older payloads)
    if _safe_bool(r.get("is_limitup_locked")):
        return "hit"
    if _safe_bool(r.get("is_limitup_touch")) or _safe_bool(r.get("is_limitup_touch_any")):
        return "touch"
    if _safe_bool(r.get("is_surge_ge10")) or _safe_bool(r.get("is_bigup10")) or _safe_bool(r.get("is_bigmove10")):
        return "big"
    return ""


def _count_status(rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    hit = touch = big = 0
    for r in rows or []:
        st = _status_from_row(r)
        if st == "big":
            big += 1
        elif st == "touch":
            touch += 1
        elif st == "hit":
            hit += 1
    return hit, touch, big


def _status_badge_for_top_row(r: Dict[str, Any], theme: str) -> Tuple[str, str, str]:
    """
    Decide TOP-RIGHT badge for TOP rows:
    - big   -> Big 10%+
    - touch -> Touched
    - hit   -> Limit Hit
    """
    theme = (theme or "dark").lower()
    is_dark = theme == "dark"

    c_orange = "#f59f00" if is_dark else "#f08c00"  # hit
    c_blue = "#4dabf7" if is_dark else "#1c7ed6"    # touch
    c_pink = "#ff6b6b" if is_dark else "#d9480f"    # big

    st = _status_from_row(r)
    if st == "big":
        return ("Big 10%+", c_pink, "#0b0d10" if is_dark else "#ffffff")
    if st == "touch":
        return ("Touched", c_blue, "#0b0d10" if is_dark else "#ffffff")
    return ("Limit Hit", c_orange, "#0b0d10" if is_dark else "#ffffff")


def _limit_pct_optional(r: Dict[str, Any]) -> Optional[float]:
    """
    Show Limit X% pill ONLY when band exists.
    Accept:
      - limit_rate_pct (5 / 10 / 20)
      - limit_rate     (5 / 10 / 20 or 0.05 / 0.10 / 0.20)
      - band_pct       (0.05 / 0.10 / 0.20)
    """
    v = r.get("limit_rate_pct", None)
    if v is not None:
        try:
            fv = float(v)
            if fv > 0:
                return fv
        except Exception:
            pass

    v = r.get("limit_rate", None)
    if v is not None:
        try:
            fv = float(v)
            if fv > 0:
                return fv * 100.0 if fv <= 1.5 else fv
        except Exception:
            pass

    bp = r.get("band_pct", None)
    if bp is not None:
        try:
            fb = float(bp)
            if fb > 0:
                return fb * 100.0
        except Exception:
            pass

    return None


def _status_label_short(st: str) -> str:
    st = _normalize_status(st)
    if st == "hit":
        return "Limit Hit"
    if st == "touch":
        return "Touched"
    if st == "big":
        return "Big 10%+"
    return ""


def _default_line2_top(r: Dict[str, Any]) -> str:
    """
    TOP line2:
    當日狀態 + 前日狀態
    """
    today = _status_label_short(r.get("today_status") or r.get("limitup_status"))
    prev = _status_label_short(r.get("prev_status") or r.get("prev_limitup_status"))

    st_today = _safe_int(r.get("streak_today"), 0)
    st_prev = _safe_int(r.get("streak_prev"), 0)

    parts: List[str] = []
    if today:
        parts.append(f"Today: {today}" + (f" ({st_today})" if st_today > 0 else ""))
    if prev:
        parts.append(f"Prev: {prev}" + (f" ({st_prev})" if st_prev > 0 else ""))

    return " | ".join(parts) if parts else ""


def _default_line2_peer(r: Dict[str, Any]) -> str:
    """
    PEER line2:
    以前日狀態為主，若有 prev_ret_pct 再附加。
    """
    prev = _status_label_short(r.get("prev_status") or r.get("prev_limitup_status"))
    st_prev = _safe_int(r.get("streak_prev"), 0)

    prev_head = ""
    if prev:
        prev_head = f"Prev: {prev}" + (f" ({st_prev})" if st_prev > 0 else "")

    prev_ret_pct = r.get("prev_ret_pct", None)
    prev_ret_str = ""
    if prev_ret_pct is not None:
        try:
            prev_ret_str = f"Prev session {float(prev_ret_pct):+.2f}%"
        except Exception:
            prev_ret_str = ""

    if prev_head and prev_ret_str:
        return f"{prev_head} | {prev_ret_str}"
    if prev_head:
        return prev_head
    if prev_ret_str:
        return prev_ret_str
    return ""


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
    limitup_rows: Optional[List[Dict[str, Any]]] = None,
    peer_rows: Optional[List[Dict[str, Any]]] = None,
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
    top_box_title: Optional[str] = None,
    bot_box_title: Optional[str] = None,
):
    setup_font()

    theme = (theme or "dark").lower()
    is_dark = theme == "dark"

    bg = "#0b0d10" if is_dark else "#ffffff"
    fg = "#f1f3f5" if is_dark else "#111111"
    sub = "#adb5bd" if is_dark else "#555555"
    box = "#14171c" if is_dark else "#f6f8fa"
    line = "#343a40" if is_dark else "#d0d7de"
    line2_color = "#cfd4da" if is_dark else "#495057"

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_axis_off()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    # -------------------------------
    # Header
    # -------------------------------
    title_fs = int(getattr(layout, "title_fs", 62))
    subtitle_fs = int(getattr(layout, "subtitle_fs", 30))

    ax.text(
        0.5, 0.97, _safe_str(sector),
        ha="center", va="top",
        fontsize=title_fs,
        color=fg, weight="bold"
    )

    if int(page_total) > 1:
        ax.text(
            0.97, 0.97, f"{int(page_idx)}/{int(page_total)}",
            ha="right", va="top",
            fontsize=max(18, subtitle_fs - 8),
            color=sub, weight="bold"
        )

    header_lines: List[str] = []
    if time_note:
        header_lines = [x.strip() for x in str(time_note).split("\n") if x.strip()]

    if header_lines:
        ax.text(
            0.5, 0.91, header_lines[0],
            ha="center", va="top",
            fontsize=subtitle_fs,
            color=sub, weight="bold"
        )
        if len(header_lines) > 1:
            ax.text(
                0.5, 0.88, header_lines[1],
                ha="center", va="top",
                fontsize=max(18, subtitle_fs - 2),
                color=sub, weight="bold"
            )

    ensure_renderer(fig)

    # -------------------------------
    # Layout geometry
    # -------------------------------
    reserve_top = 0.86 if len(header_lines) > 1 else 0.88

    top_y0 = float(getattr(layout, "top_box_y0", 0.84))
    top_y1 = float(getattr(layout, "top_box_y1", 0.485))
    bot_y0 = float(getattr(layout, "bot_box_y0", 0.465))
    bot_y1 = float(getattr(layout, "bot_box_y1", 0.085))

    top_y0 = min(top_y0, reserve_top)

    ax.add_patch(plt.Rectangle((0.05, top_y1), 0.90, top_y0 - top_y1, facecolor=box, edgecolor=line, linewidth=2))
    ax.add_patch(plt.Rectangle((0.05, bot_y1), 0.90, bot_y0 - bot_y1, facecolor=box, edgecolor=line, linewidth=2))

    # -------------------------------
    # Box titles
    # -------------------------------
    box_title_fs = int(getattr(layout, "box_title_fs", 30))
    top_title_y = top_y0 - 0.025
    bot_title_y = bot_y0 - 0.025

    L = list(limitup_rows or [])
    P = list(peer_rows or [])

    if hit_total is None or touch_total is None or big_total is None:
        _h, _t, _b = _count_status(L)
        hit_total = _h if hit_total is None else hit_total
        touch_total = _t if touch_total is None else touch_total
        big_total = _b if big_total is None else big_total

    def top_title() -> str:
        if _safe_str(top_box_title):
            t = _safe_str(top_box_title)
            if "top movers" in t.lower():
                return "Big 10%+ | Limit Hit | Touched"
            return t

        if (
            hit_shown is not None and hit_total is not None and
            touch_shown is not None and touch_total is not None and
            big_shown is not None and big_total is not None
        ):
            return (
                f"Big 10%+ {int(big_shown)}/{int(big_total)} | "
                f"Limit Hit {int(hit_shown)}/{int(hit_total)} | "
                f"Touched {int(touch_shown)}/{int(touch_total)}"
            )

        return "Big 10%+ | Limit Hit | Touched"

    def bot_title() -> str:
        if _safe_str(bot_box_title):
            return _safe_str(bot_box_title)
        if (hit_total or 0) == 0 and (touch_total or 0) == 0 and (big_total or 0) == 0:
            return "No 10%+ or limit hits"
        return "Peers (same sector)"

    top_t, top_fs = _fit_text_fs(
        ax, fig, top_title(),
        x_left=0.08, x_right=0.92, y=top_title_y,
        fs_start=box_title_fs, fs_min=max(18, box_title_fs - 10),
        weight="bold",
    )
    bot_t, bot_fs = _fit_text_fs(
        ax, fig, bot_title(),
        x_left=0.08, x_right=0.92, y=bot_title_y,
        fs_start=box_title_fs, fs_min=max(18, box_title_fs - 10),
        weight="bold",
    )

    ax.text(0.08, top_title_y, top_t, ha="left", va="center", fontsize=top_fs, color=fg, weight="bold")
    ax.text(0.08, bot_title_y, bot_t, ha="left", va="center", fontsize=bot_fs, color=fg, weight="bold")

    # -------------------------------
    # Rows layout
    # -------------------------------
    two_line = True
    y_start_top, row_h_top = calc_rows_layout(top_y0 - 0.055, top_y1, int(rows_per_page), two_line=two_line)
    y_start_bot, row_h_bot = calc_rows_layout(bot_y0 - 0.055, bot_y1, int(rows_per_page) + 1, two_line=two_line)

    x_name = 0.08
    x_tag = 0.94
    sep_x0, sep_x1 = 0.08, 0.91

    row_name_fs = int(getattr(layout, "row_name_fs", 28))
    row_line2_fs = max(18, row_name_fs - 6)
    row_tag_fs = max(20, row_name_fs - 4)

    def _draw_empty(y0: float, y1: float, msg: str):
        ax.text(
            0.5, (y0 + y1) / 2,
            msg,
            ha="center", va="center",
            fontsize=max(22, row_line2_fs),
            color=sub, alpha=0.65
        )

    # ================= TOP =================
    if not L:
        _draw_empty(top_y0, top_y1, "(No items)")
    else:
        for i, r in enumerate(L[: int(rows_per_page)]):
            y_center = y_start_top - i * row_h_top
            y1 = y_center + row_h_top * 0.22
            y2 = y_center - row_h_top * 0.22

            line1 = _safe_str(r.get("line1") or "")
            line2 = _safe_str(r.get("line2") or "")

            if not line2:
                line2 = _default_line2_top(r)

            status_txt, status_bg, status_fg = _status_badge_for_top_row(r, theme)

            badge_w_px = text_width_px(ax, fig, status_txt, x=x_tag, y=y1, fontsize=row_tag_fs, weight="bold")
            badge_pad_px = 26 + 18
            x_status_left = x_tag - px_to_data_dx(ax, badge_w_px + badge_pad_px, y_data=y1)

            safe_right = max(x_name + 0.10, x_status_left - 0.01)

            line1_fit = _ellipsize_px(
                ax, fig, line1,
                x_left=x_name, x_right=safe_right, y=y1,
                fontsize=row_name_fs, weight="bold"
            )
            line2_fit = _ellipsize_px(
                ax, fig, line2,
                x_left=x_name, x_right=x_tag - 0.08, y=y2,
                fontsize=row_line2_fs, weight="regular"
            )

            ax.text(x_name, y1, line1_fit, ha="left", va="center", fontsize=row_name_fs, color=fg, weight="bold")
            if line2_fit:
                ax.text(
                    x_name, y2, line2_fit,
                    ha="left", va="center",
                    fontsize=row_line2_fs,
                    color=line2_color,
                    weight="regular",
                    alpha=0.95
                )

            pct = _limit_pct_optional(r)
            if pct is not None:
                pill_text = limit_label(pct)
                pill_bg, pill_fg = limit_colors(pct, theme)
                draw_pill_after_text(
                    ax, fig,
                    text_x=x_name,
                    text_y=y1,
                    text_str=line1_fit,
                    text_fontsize=row_name_fs,
                    pill_text=pill_text,
                    pill_fontsize=row_tag_fs,
                    pill_fg=pill_fg,
                    pill_bg=pill_bg,
                    x_right_limit=x_status_left,
                    gap_px=10,
                    measure_text_width_px_fn=text_width_px,
                    px_to_data_dx_fn=px_to_data_dx,
                    fallback_y=y2,
                )

            ax.text(
                x_tag, y1, status_txt,
                ha="right", va="center",
                fontsize=row_tag_fs,
                color=status_fg, weight="bold",
                bbox=dict(boxstyle="round,pad=0.35", facecolor=status_bg, edgecolor="none", alpha=0.95)
            )

            ret = _safe_float(r.get("ret"), 0.0)
            if abs(ret) > 1e-12:
                ret_text = _fmt_ret_pct(ret)
                ax.text(
                    x_tag, y2, ret_text,
                    ha="right", va="center",
                    fontsize=row_tag_fs,
                    color="#0b0d10" if is_dark else "#ffffff",
                    weight="bold",
                    bbox=dict(
                        boxstyle="round,pad=0.30",
                        facecolor=get_ret_color(ret, theme),
                        edgecolor="none",
                        alpha=0.95
                    )
                )

            if i < min(len(L), int(rows_per_page)) - 1:
                ax.plot(
                    [sep_x0, sep_x1],
                    [y_center - row_h_top * 0.44, y_center - row_h_top * 0.44],
                    color=line, lw=1, alpha=0.6
                )

    # ================= BOTTOM =================
    if not P:
        _draw_empty(bot_y0, bot_y1, "(No peers)")
    else:
        for i, r in enumerate(P[: int(rows_per_page) + 1]):
            y_center = y_start_bot - i * row_h_bot
            y1 = y_center + row_h_bot * 0.22
            y2 = y_center - row_h_bot * 0.22

            line1 = _safe_str(r.get("line1") or "")
            line2 = _safe_str(r.get("line2") or "")

            if not line2:
                line2 = _default_line2_peer(r)

            ret = _safe_float(r.get("ret"), 0.0)
            ret_text = _fmt_ret_pct(ret) if abs(ret) > 1e-12 else ""
            ret_fs = 24

            x_ret = x_tag
            ret_w_px = text_width_px(ax, fig, ret_text, x=x_ret, y=y1, fontsize=ret_fs, weight="bold") if ret_text else 0.0
            x_ret_left = x_ret - px_to_data_dx(ax, ret_w_px + 14, y_data=y1) if ret_text else x_ret

            pct = _limit_pct_optional(r)
            pill_text = limit_label(pct) if pct is not None else ""
            pill_w_px = text_width_px(ax, fig, pill_text, x=x_ret, y=y1, fontsize=ret_fs, weight="bold") if pill_text else 0.0

            reserve_px = (ret_w_px + 18) + ((pill_w_px + 26 + 18) if pill_text else 0.0)
            safe_right = x_tag - px_to_data_dx(ax, reserve_px, y_data=y1)
            safe_right = max(x_name + 0.10, safe_right)

            line1_fit = _ellipsize_px(
                ax, fig, line1,
                x_left=x_name, x_right=safe_right, y=y1,
                fontsize=row_name_fs, weight="bold"
            )
            line2_fit = _ellipsize_px(
                ax, fig, line2,
                x_left=x_name, x_right=x_tag - 0.08, y=y2,
                fontsize=row_line2_fs, weight="regular"
            )

            ax.text(x_name, y1, line1_fit, ha="left", va="center", fontsize=row_name_fs, color=fg, weight="bold")
            if line2_fit:
                ax.text(
                    x_name, y2, line2_fit,
                    ha="left", va="center",
                    fontsize=row_line2_fs,
                    color=line2_color,
                    weight="regular",
                    alpha=0.95
                )

            if ret_text:
                ax.text(
                    x_ret, y1, ret_text,
                    ha="right", va="center",
                    fontsize=ret_fs,
                    color=get_ret_color(ret, theme),
                    weight="bold"
                )

            if pct is not None:
                pill_bg, pill_fg = limit_colors(pct, theme)
                draw_pill_after_text(
                    ax, fig,
                    text_x=x_name,
                    text_y=y1,
                    text_str=line1_fit,
                    text_fontsize=row_name_fs,
                    pill_text=pill_text,
                    pill_fontsize=ret_fs,
                    pill_fg=pill_fg,
                    pill_bg=pill_bg,
                    x_right_limit=x_ret_left,
                    gap_px=10,
                    measure_text_width_px_fn=text_width_px,
                    px_to_data_dx_fn=px_to_data_dx,
                    fallback_y=y2,
                )

            if i < min(len(P), int(rows_per_page) + 1) - 1:
                ax.plot(
                    [sep_x0, sep_x1],
                    [y_center - row_h_bot * 0.44, y_center - row_h_bot * 0.44],
                    color=line, lw=1, alpha=0.6
                )

        if has_more_peers:
            ax.text(
                0.5, bot_y1 + 0.02,
                "(More items not shown)",
                ha="center", va="bottom",
                fontsize=max(20, row_line2_fs),
                color=sub, alpha=0.85,
                weight="bold"
            )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100, facecolor=bg)
    plt.close(fig)
    return out_path
