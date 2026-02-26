# scripts/render_images_ca/sector_blocks/draw_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from datetime import datetime, timedelta, timezone

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
# Optional ZoneInfo (py>=3.9)
# =============================================================================
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# =============================================================================
# Font (keep CJK-safe because you may have mixed chars)
# =============================================================================
def setup_chinese_font() -> str | None:
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
    # ✅ CA: display effective trading day first
    ymd = str(payload.get("ymd_effective") or payload.get("ymd") or payload.get("bar_date") or "").strip()
    return ymd or ""


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _format_utc_label_from_offset_minutes(mins: int) -> str:
    """
    mins: e.g. -300 -> UTC-05, 330 -> UTC+05:30
    """
    sign = "+" if mins >= 0 else "-"
    m = abs(int(mins))
    hh = m // 60
    mm = m % 60
    if mm == 0:
        return f"UTC{sign}{hh:02d}"
    return f"UTC{sign}{hh:02d}:{mm:02d}"


def _format_utc_label(offset: str) -> str:
    """
    Convert "-05:00" -> "UTC-05"
            "+09:00" -> "UTC+09"
            "-05:30" -> "UTC-05:30"
            "-0500"  -> "UTC-05"
            "UTC-05:00" -> "UTC-05"
    """
    off = _safe_str(offset)
    if not off:
        return ""
    s = off.strip()
    if s.upper().startswith("UTC"):
        s = s[3:].strip()

    # "-0500"
    if len(s) == 5 and s[0] in "+-" and s[1:].isdigit():
        hh = s[:3]
        mm = s[3:]
        if mm == "00":
            return f"UTC{hh}"
        return f"UTC{hh}:{mm}"

    # "-05:00"
    if ":" in s and (s.startswith("+") or s.startswith("-")):
        hh, mm = s.split(":", 1)
        hh = (hh or "").strip()
        mm = (mm or "").strip()
        if mm == "00":
            return f"UTC{hh}"
        return f"UTC{hh}:{mm}"

    # "-05"
    if (s.startswith("+") or s.startswith("-")) and s[1:].isdigit():
        return f"UTC{s}"

    # already formatted-ish
    if s.upper().startswith("UTC"):
        return s

    return f"UTC{s}"


def _guess_city_label_from_tz(tz_name: str) -> str:
    tz = _safe_str(tz_name)
    if "Toronto" in tz or tz.endswith("/Toronto"):
        return "Toronto Time"
    if "Vancouver" in tz or tz.endswith("/Vancouver"):
        return "Vancouver Time"
    if "New_York" in tz or tz.endswith("/New_York"):
        return "New York Time"
    return "Local Time"


def _parse_dt_any(s: str) -> Optional[datetime]:
    """
    Best-effort parse for:
      - "2026-02-22T08:37:00Z"
      - "2026-02-22T08:37:00+00:00"
      - "2026-02-22 08:37:00"
      - "2026-02-22 08:37"
      - "2026-02-22T08:37"
    """
    txt = _safe_str(s)
    if not txt:
        return None

    t = txt.replace("Z", "+00:00")

    try:
        return datetime.fromisoformat(t)
    except Exception:
        pass

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]
    for f in fmts:
        try:
            return datetime.strptime(t, f)
        except Exception:
            continue
    return None


def _get_payload_time_dict(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = payload.get("meta") or {}
    if isinstance(meta, dict):
        t = meta.get("time") or {}
        if isinstance(t, dict):
            return t
    return {}


def _is_iana_tz_name(tz_name: str) -> bool:
    """
    Very small heuristic:
    - IANA tz usually has a '/', e.g. America/Toronto
    - Reject things like 'UTC-05:00'
    """
    s = _safe_str(tz_name)
    return bool(s) and ("/" in s) and (not s.upper().startswith("UTC"))


def _parse_utc_offset_to_tzinfo(offset: str) -> Optional[timezone]:
    """
    Accept:
      - "UTC-05:00", "-05:00", "+09:00", "-0500", "+0930", "UTC+08"
    Return datetime.timezone(timedelta(...)) or None.
    """
    s = _safe_str(offset)
    if not s:
        return None
    t = s.strip()
    if t.upper().startswith("UTC"):
        t = t[3:].strip()

    # "+09" / "-05"
    if len(t) == 3 and t[0] in "+-" and t[1:].isdigit():
        hh = int(t[1:])
        mins = hh * 60
        if t[0] == "-":
            mins = -mins
        return timezone(timedelta(minutes=mins))

    # "+09:30" / "-05:00"
    if ":" in t and t[0] in "+-":
        hh_s, mm_s = t.split(":", 1)
        try:
            hh = int(hh_s[1:])
            mm = int(mm_s)
            mins = hh * 60 + mm
            if hh_s[0] == "-":
                mins = -mins
            return timezone(timedelta(minutes=mins))
        except Exception:
            return None

    # "+0930" / "-0500"
    if len(t) == 5 and t[0] in "+-" and t[1:].isdigit():
        try:
            hh = int(t[1:3])
            mm = int(t[3:5])
            mins = hh * 60 + mm
            if t[0] == "-":
                mins = -mins
            return timezone(timedelta(minutes=mins))
        except Exception:
            return None

    return None


def _resolve_market_tz(payload: Dict[str, Any]) -> str:
    """
    Prefer pipeline-provided market_tz (IANA only).
    If it's an offset string (e.g. UTC-05:00), do NOT return it here.
    """
    mt = _get_payload_time_dict(payload)
    tz = _safe_str(mt.get("market_tz") or mt.get("tz") or mt.get("timezone") or "")
    if _is_iana_tz_name(tz):
        return tz
    # default: Toronto (TSX default)
    return "America/Toronto"


def _resolve_updated_dt_local(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return:
      updated_local_str: "YYYY-MM-DD HH:MM"
      utc_label: "UTC-05" / "UTC-04" / ...

    Priority:
      1) meta.time.market_finished_at (parseable) + tz
      2) payload.generated_at (assume UTC if naive) -> tz
      3) meta.time.market_finished_hm only -> today in tz (best-effort)

    IMPORTANT:
    - If payload tz is NOT IANA (e.g. 'UTC-05:00'), we convert using fixed offset tzinfo.
    """
    mt = _get_payload_time_dict(payload)

    # Determine tzinfo:
    #   - IANA tz -> ZoneInfo
    #   - else try offset -> fixed timezone
    #   - else fallback Toronto IANA
    tz_name_raw = _safe_str(mt.get("market_tz") or mt.get("tz") or mt.get("timezone") or "")
    tz_offset_raw = _safe_str(mt.get("market_utc_offset") or mt.get("market_tz_offset") or tz_name_raw or "")

    tzinfo = None
    tz_label = None

    # 1) IANA
    if ZoneInfo is not None and _is_iana_tz_name(tz_name_raw):
        try:
            tzinfo = ZoneInfo(tz_name_raw)
        except Exception:
            tzinfo = None

    # 2) fixed offset
    if tzinfo is None:
        fixed = _parse_utc_offset_to_tzinfo(tz_offset_raw)
        if fixed is not None:
            tzinfo = fixed
            # label from minutes
            try:
                mins = int(fixed.utcoffset(datetime.now()) .total_seconds() // 60)  # type: ignore
                tz_label = _format_utc_label_from_offset_minutes(mins)
            except Exception:
                tz_label = _format_utc_label(tz_offset_raw) or None

    # 3) fallback Toronto
    if tzinfo is None and ZoneInfo is not None:
        try:
            tzinfo = ZoneInfo("America/Toronto")
        except Exception:
            tzinfo = None

    # If still None (no tzdata), we cannot convert; just print raw
    if tzinfo is None:
        updated_at = _safe_str(mt.get("market_finished_at") or payload.get("generated_at") or "")
        updated_at = updated_at.replace("T", " ")[:16] if updated_at else ""
        utc_label = _format_utc_label(tz_offset_raw) or None
        return (updated_at or None), utc_label

    # Helper to compute UTC label if we didn't already
    def _utc_label_from_dt(dt: datetime) -> Optional[str]:
        if tz_label:
            return tz_label
        try:
            off = dt.utcoffset()
            if off is None:
                return None
            mins = int(off.total_seconds() // 60)
            return _format_utc_label_from_offset_minutes(mins)
        except Exception:
            return _format_utc_label(tz_offset_raw) or None

    # 1) market_finished_at
    mfa = _safe_str(mt.get("market_finished_at") or "")
    dt = _parse_dt_any(mfa) if mfa else None
    if dt is not None:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tzinfo)
        else:
            dt = dt.astimezone(tzinfo)
        updated_local = dt.strftime("%Y-%m-%d %H:%M")
        return updated_local, _utc_label_from_dt(dt)

    # 2) generated_at
    ga = _safe_str(payload.get("generated_at") or mt.get("generated_at") or "")
    dt2 = _parse_dt_any(ga) if ga else None
    if dt2 is not None:
        if dt2.tzinfo is None:
            dt2 = dt2.replace(tzinfo=timezone.utc)
        dt2 = dt2.astimezone(tzinfo)
        updated_local = dt2.strftime("%Y-%m-%d %H:%M")
        return updated_local, _utc_label_from_dt(dt2)

    # 3) market_finished_hm
    hm = _safe_str(mt.get("market_finished_hm") or "")
    if hm and len(hm) >= 4:
        now_local = datetime.now(tzinfo)
        updated_local = f"{now_local:%Y-%m-%d} {hm[:5]}"
        return updated_local, _utc_label_from_dt(now_local)

    return None, None


def get_market_time_info(payload: Dict[str, Any]) -> Tuple[str, str]:
    """
    Sector page subtitle (2 lines):
      line1: Canada (Toronto Time) YYYY-MM-DD Intraday
      line2: Updated YYYY-MM-DD HH:MM (UTC-05)
    """
    ymd = parse_cutoff(payload)

    # Try shared builder first, but only accept if line2 includes a date.
    if _build_time_note is not None:
        try:
            ymd2, note = _build_time_note(payload, market="CA", lang="en")
            ymd2 = (ymd2 or ymd or "").strip()
            note = (note or "").strip()

            if note:
                lines = [ln.strip() for ln in note.split("\n") if ln.strip()]
                if len(lines) == 1 and " | " in note and "Updated" in note:
                    left, right = note.split(" | ", 1)
                    lines = [left.strip(), right.strip()]

                if len(lines) >= 2:
                    l2 = lines[1]
                    has_date = len(l2) >= 10 and l2[7:8] == "-" and l2[4:5] == "-"
                    if has_date:
                        return (ymd2 or ymd), "\n".join(lines[:2])
        except Exception:
            pass

    slot = _safe_str(payload.get("slot") or "")
    if slot == "close":
        session = "Close"
    elif slot == "midday":
        session = "Intraday"
    elif slot == "open":
        session = "Open"
    else:
        session = slot.upper() if slot else "Data"

    # display label uses tz name if IANA, else assume Toronto label if offset-only
    mt = _get_payload_time_dict(payload)
    tz_name_raw = _safe_str(mt.get("market_tz") or mt.get("tz") or mt.get("timezone") or "")
    tz_for_label = tz_name_raw if _is_iana_tz_name(tz_name_raw) else "America/Toronto"
    city_label = _guess_city_label_from_tz(tz_for_label)
    market_label = f"Canada ({city_label})"

    updated_local, utc_label = _resolve_updated_dt_local(payload)
    offset_part = f" ({utc_label})" if utc_label else ""

    line1 = f"{market_label} {ymd} {session}".strip() if ymd else f"{market_label} {session}".strip()
    line2 = f"Updated {updated_local}{offset_part}".strip() if updated_local else f"Updated{offset_part}".strip()

    return (ymd or ""), f"{line1}\n{line2}".strip()


# =============================================================================
# Color helpers
# =============================================================================
def get_ret_color(ret: float, theme: str = "light") -> str:
    if theme == "dark":
        return "#40c057" if ret >= 0 else "#ff6b6b"
    return "#2f9e44" if ret >= 0 else "#c92a2a"

def pick_big_tag(ret_decimal: float) -> Tuple[str, str]:
    """
    6 tiers with clearly distinct color families (same as AU/UK):

    10–20%  : MOVER  (Blue)
    20–30%  : JUMP   (Green)
    30–40%  : SURGE  (Purple)
    40–50%  : RALLY  (Orange)
    50–100% : ROCKET (Red)
    100%+   : MOON   (Gold)
    """
    if ret_decimal >= 1.00:
        return ("MOON", "#f59f00")        # Gold
    if ret_decimal >= 0.50:
        return ("ROCKET", "#e03131")      # Red
    if ret_decimal >= 0.40:
        return ("RALLY", "#f76707")       # Orange
    if ret_decimal >= 0.30:
        return ("SURGE", "#7048e8")       # Purple
    if ret_decimal >= 0.20:
        return ("JUMP", "#2f9e44")        # Green
    return ("MOVER", "#4dabf7")           # Blue


def _infer_threshold_from_rows(limitup_rows: List[Dict[str, Any]], default: float = 0.10) -> float:
    for r in (limitup_rows or []):
        for k in ("touch_th", "ret_th"):
            try:
                v = r.get(k)
                if v is None:
                    continue
                th = float(v)
                if th > 0:
                    return th
            except Exception:
                pass
    return float(default)


def _pick_touch_tag_text(row: Dict[str, Any], *, default_th: float = 0.10) -> str:
    for k in ("touch_tag", "touch_label"):
        v = _safe_str(row.get(k))
        if v:
            return v

    for k in ("touch_th", "ret_th"):
        try:
            th = float(row.get(k))
            if th > 0:
                return f"Touched {th*100:.0f}%"
        except Exception:
            pass

    bt = _safe_str(row.get("badge_text"))
    if bt:
        return bt

    return f"Touched {default_th*100:.0f}%"


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
        t = ax.text(x, y, s, ha="center", va="top", fontsize=fontsize, color=fg, weight=weight, alpha=0.0)
        bb = t.get_window_extent(renderer=renderer)
        t.remove()
        return float(bb.width)

    def _avail_width_px(x0: float, x1: float, y: float) -> float:
        p0 = ax.transData.transform((x0, y))
        p1 = ax.transData.transform((x1, y))
        return max(1.0, (p1[0] - p0[0]))

    def _fit_center_ellipsis(text: str, x0: float, x1: float, y: float, fontsize: int) -> str:
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
        s = (text or "").strip()
        if not s:
            return fontsize
        fs = int(fontsize)
        avail = _avail_width_px(x0, x1, y)
        while fs > min_fs and _center_text_width_px(s, 0.5, y, fontsize=fs, weight="bold") > avail:
            fs -= 2
        return fs

    ret_th = _infer_threshold_from_rows(limitup_rows, default=0.10)
    th_pct = f"{ret_th*100:.0f}%"

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

    title_x0 = 0.06
    title_x1 = 0.90 if page_total > 1 else 0.94
    title_y = layout.header_title_y
    title_txt = _fit_center_ellipsis(sector, title_x0, title_x1, title_y, fontsize=layout.title_fs)
    ax.text(0.5, title_y, title_txt, ha="center", va="top", fontsize=layout.title_fs, color=fg, weight="bold")

    # Subtitle (2 lines)
    sub_block = (time_note or "").strip()
    if sub_block:
        lines = [ln.strip() for ln in sub_block.split("\n") if ln.strip()]
        sub_x0, sub_x1 = 0.06, 0.94

        if len(lines) >= 1:
            y1 = layout.header_subtitle_y
            fs1 = _fit_center_shrink(lines[0], sub_x0, sub_x1, y1, fontsize=layout.subtitle_fs, min_fs=16)
            ax.text(0.5, y1, lines[0], ha="center", va="top", fontsize=fs1, color=sub, weight="bold", alpha=0.90)

        if len(lines) >= 2:
            y2 = getattr(layout, "header_subtitle_line2_y", layout.header_subtitle_y - 0.040)
            fs2_base = getattr(layout, "subtitle_line2_fs", max(16, layout.subtitle_fs - 4))
            fs2 = _fit_center_shrink(lines[1], sub_x0, sub_x1, y2, fontsize=fs2_base, min_fs=14)
            ax.text(0.5, y2, lines[1], ha="center", va="top", fontsize=fs2, color=sub, weight="bold", alpha=0.90)

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

    top_y0, top_y1 = layout.top_box_y0, layout.top_box_y1
    bot_y0, bot_y1 = layout.bot_box_y0, layout.bot_box_y1

    ax.add_patch(plt.Rectangle((0.05, top_y1), 0.90, (top_y0 - top_y1), facecolor=box, edgecolor=line, linewidth=2, alpha=0.98))
    ax.add_patch(plt.Rectangle((0.05, bot_y1), 0.90, (bot_y0 - bot_y1), facecolor=box, edgecolor=line, linewidth=2, alpha=0.98))

    top_span = (top_y0 - top_y1)
    bot_span = (bot_y0 - bot_y1)
    top_title_y = top_y0 - top_span * 0.035
    bot_title_y = bot_y0 - bot_span * 0.035

    ax.text(
        0.08,
        bot_title_y,
        f"Peers (not Big +{th_pct})",
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
                ax.text(x_name, y1, fit1, ha="left", va="center", fontsize=layout.row_name_fs, color=fg, weight="medium")

                fit2 = _ellipsis_fit(line2, x_name, safe_right, y2, layout.row_line2_fs, weight="normal")
                ax.text(x_name, y2, fit2, ha="left", va="center", fontsize=layout.row_line2_fs, color=line2_color, weight="normal", alpha=0.95)

                if kind == "limitup":
                    is_touch = bool(r.get("touched_only")) or (str(r.get("limitup_status") or "") == "touch")
                    if is_touch:
                        tag_text = _pick_touch_tag_text(r, default_th=ret_th)
                        tag_bg = tag_theme_touch
                    else:
                        ret_pct = float(r.get("ret_pct", 0.0) or 0.0)
                        ret_decimal = ret_pct / 100.0
                        tag_text, tag_bg = pick_big_tag(ret_decimal)

                    ax.text(
                        x_tag,
                        y1,
                        tag_text,
                        ha="right",
                        va="center",
                        fontsize=layout.row_tag_fs,
                        color="white",
                        weight="bold",
                        bbox=dict(boxstyle=f"round,pad={layout.badge_pad_limitup}", facecolor=tag_bg, alpha=0.92, edgecolor="none"),
                    )

                    ret_pct = float(r.get("ret_pct", 0.0) or 0.0)
                    ret_decimal = ret_pct / 100.0
                    ret_color = get_ret_color(ret_decimal, theme)

                    if ret_decimal >= 1.00:
                        tag_text2 = f"+{ret_pct:.0f}%"
                        tag_fontsize = layout.row_tag_fs - 2
                    elif ret_decimal >= ret_th:
                        tag_text2 = f"+{ret_pct:.1f}%"
                        tag_fontsize = layout.row_tag_fs
                    else:
                        tag_text2 = f"+{ret_pct:.2f}%"
                        tag_fontsize = layout.row_tag_fs

                    if ret_pct < 0:
                        tag_text2 = f"{ret_pct:.1f}%"

                    ax.text(
                        x_tag,
                        y2,
                        tag_text2,
                        ha="right",
                        va="center",
                        fontsize=tag_fontsize,
                        color="white",
                        weight="bold",
                        bbox=dict(boxstyle=f"round,pad={layout.badge_pad_peer}", facecolor=ret_color, alpha=0.9, edgecolor="none"),
                    )

                else:
                    ret = float(r.get("ret", 0.0) or 0.0)
                    ret_pct = ret * 100.0
                    ret_color = get_ret_color(ret, theme)

                    if ret >= 1.00:
                        tag_text2 = f"+{ret_pct:.0f}%"
                        tag_fontsize = layout.row_tag_fs - 2
                    elif ret >= ret_th:
                        tag_text2 = f"+{ret_pct:.1f}%"
                        tag_fontsize = layout.row_tag_fs
                    else:
                        tag_text2 = f"+{ret_pct:.2f}%"
                        tag_fontsize = layout.row_tag_fs

                    if ret_pct < 0:
                        tag_text2 = f"{ret_pct:.1f}%"

                    ax.text(
                        x_tag,
                        y1,
                        tag_text2,
                        ha="right",
                        va="center",
                        fontsize=tag_fontsize,
                        color="white",
                        weight="bold",
                        bbox=dict(boxstyle=f"round,pad={layout.badge_pad_peer}", facecolor=ret_color, alpha=0.9, edgecolor="none"),
                    )

            else:
                fit1 = _ellipsis_fit(line1 or str(r.get("name", "")), x_name, safe_right, y, layout.row_name_fs)
                ax.text(x_name, y, fit1, ha="left", va="center", fontsize=layout.row_name_fs, color=fg, weight="medium")

            if i < n - 1:
                ax.plot([0.08, 0.91], [y - row_h * 0.50, y - row_h * 0.50], color=line, linewidth=1, alpha=0.5)

        if kind == "peer" and has_more_peers:
            hint_y = (y_start - (n - 1) * row_h) - row_h * 0.75
            ax.text(
                0.5,
                hint_y,
                "(More rows not shown)",
                ha="center",
                va="top",
                fontsize=max(layout.footer_fs_2 + 6, 26),
                color=sub,
                alpha=0.85,
                weight="bold",
            )

    draw_rows(limitup_rows, y_start_top, row_h_top, "limitup")
    draw_rows(peer_rows, y_start_bot, row_h_bot, "peer")

    use_precise = hit_total is not None and touch_total is not None and hit_shown is not None and touch_shown is not None
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

    top_title = f"Big +{th_pct} {big_n}  /  Touched {th_pct} {touch_n}{pct_part}"

    fs = int(layout.box_title_fs)
    _ensure_renderer()
    x_left, x_right, y = 0.08, 0.94, top_title_y
    avail_px = abs(ax.transData.transform((x_right, y))[0] - ax.transData.transform((x_left, y))[0])

    def _left_text_width_px(s: str, x: float, y: float, fontsize: int, weight: str = "bold") -> float:
        _ensure_renderer()
        t = ax.text(x, y, s, ha="left", va="center", fontsize=fontsize, color=fg, weight=weight, alpha=0.0)
        bb = t.get_window_extent(renderer=renderer)
        t.remove()
        return float(bb.width)

    while fs > 18 and _left_text_width_px(top_title, x_left, y, fontsize=fs, weight="bold") > avail_px:
        fs -= 2

    ax.text(x_left, top_title_y, top_title, ha="left", va="center", fontsize=fs, color=fg, weight="bold")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100, facecolor=bg)
    plt.close(fig)
