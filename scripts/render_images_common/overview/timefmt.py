# scripts/render_images_common/overview/timefmt.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Optional, Tuple


# =============================================================================
# Small helpers
# =============================================================================
def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def date_for_display(payload: Dict[str, Any]) -> str:
    """
    Prefer trading day (ymd_effective), fallback to ymd.
    This is what we display as the "trade date".
    """
    return _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")


def _get_time_meta(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    meta.time is optional. Structure example:

    meta: {
      time: {
        market_finished_hm: "16:00",
        market_tz: "JST",
        market_finished_at: "2026-02-13 16:00",
        market_tz_offset: "+07:00"   # (optional)
        market_finished_at_iso: "2026-02-22T13:59+11:00"  # (optional, preferred)
        market_finished_at_utc: "2026-02-22T02:59:00Z"    # (optional)
      }
    }
    """
    meta = payload.get("meta") or {}
    t = (meta.get("time") or {}) if isinstance(meta, dict) else {}
    return t if isinstance(t, dict) else {}


def _split_ymd_from_dt(s: str) -> str:
    """
    Extract YYYY-MM-DD from:
      - "YYYY-MM-DD HH:MM"
      - "YYYY-MM-DDTHH:MM:SS"
      - ISO strings like "YYYY-MM-DDTHH:MM+11:00"
    """
    s = _safe_str(s)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return ""


def _parse_iso_dt(s: str) -> Optional[datetime]:
    """
    Parse ISO datetime string.
    Supports trailing 'Z' -> '+00:00'.
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


def _extract_from_iso(iso_s: str) -> Tuple[str, str, str]:
    """
    From ISO dt -> (ymd, hm, tz_offset_str)
    tz_offset_str in "+11:00" / "-05:30" form if available.
    """
    dt = _parse_iso_dt(iso_s)
    if dt is None:
        return "", "", ""
    ymd = dt.strftime("%Y-%m-%d")
    hm = dt.strftime("%H:%M")
    off = ""
    try:
        td = dt.utcoffset()
        if td is not None:
            total = int(td.total_seconds())
            sign = "+" if total >= 0 else "-"
            total = abs(total)
            hh = total // 3600
            mm = (total % 3600) // 60
            off = f"{sign}{hh:02d}:{mm:02d}"
    except Exception:
        off = ""
    return ymd, hm, off


# =============================================================================
# Tiny i18n labels (subtitle only)
# =============================================================================
def _label(key: str, lang: str) -> str:
    """
    Localized short labels for subtitle line.
    Keep it minimal (retail-friendly).
    """
    k = (key or "").strip().lower()

    if lang == "th":
        if k == "trade_date":
            return "วันที่ข้อมูล"
        if k == "updated":
            return "อัปเดต"
        return key

    if lang == "ja":
        if k == "trade_date":
            return "データ日"
        if k == "updated":
            return "更新"
        return key

    if lang == "ko":
        if k == "trade_date":
            return "데이터일"
        if k == "updated":
            return "업데이트"
        return key

    if lang == "zh-cn":
        if k == "trade_date":
            return "数据日"
        if k == "updated":
            return "更新"
        return key

    if lang == "zh-tw":
        if k == "trade_date":
            return "資料日"
        if k == "updated":
            return "更新"
        return key

    # English default
    if k == "trade_date":
        return "Data date"
    if k == "updated":
        return "Updated"
    return key


def _tz_display(lang: str, tmeta: Dict[str, Any]) -> str:
    """
    Build timezone display.

    Goals:
    - Prefer "(UTC+10)" over "(UTC+10:00)" when minutes are :00
    - If tz itself looks like "UTC+08:00", normalize it too
    - Thai keeps special ICT preference
    """
    tz = _safe_str(tmeta.get("market_tz") or "")
    off = _safe_str(tmeta.get("market_tz_offset") or tmeta.get("tz_offset") or tmeta.get("market_utc_offset") or "")

    def _compact_utc_offset(s: str) -> str:
        """
        Normalize offset:
          "+10:00" -> "+10"
          "-05:00" -> "-05"
          "+07:30" -> "+07:30"  (keep minutes if not 00)
        """
        s = _safe_str(s)
        if not s:
            return ""
        m = re.match(r"^([+-])(\d{1,2})(?::?(\d{2}))?$", s)
        if not m:
            return s
        sign, hh, mm = m.group(1), m.group(2), m.group(3)
        hh2 = hh.zfill(2)
        if not mm or mm == "00":
            return f"{sign}{hh2}"
        return f"{sign}{hh2}:{mm}"

    # If tz looks like "UTC+08:00" / "UTC-05:00", extract offset from it
    tz_up = tz.upper().strip()
    tz_off = ""
    m2 = re.match(r"^UTC\s*([+-]\d{1,2})(?::?(\d{2}))?$", tz_up)
    if m2:
        sign_h = m2.group(1)  # like "+10" or "-5"
        mm = m2.group(2)      # maybe "00"
        tz_off = sign_h + (f":{mm}" if mm else "")

    # Choose offset source priority: explicit off > tz-derived offset
    off_eff = _compact_utc_offset(off or tz_off)

    # If no tz/offset at all → nothing
    if not tz and not off_eff:
        return ""

    if lang == "th":
        # Common Thai time notations:
        # - ICT (Indochina Time) = UTC+07:00
        if tz_up in {"ICT", "BANGKOK", "ASIA/BANGKOK", "TH", "THA", "UTC+7", "UTC+07"}:
            return "(ICT)"
        if off_eff:
            return f"(UTC{off_eff})" if off_eff.startswith(("+", "-")) else f"(UTC+{off_eff})"
        return f"({tz})"

    # Non-Thai: always prefer UTC offset if available; otherwise fall back to tz name
    if off_eff:
        return f"(UTC{off_eff})"
    return f"({tz})" if tz else ""


# =============================================================================
# Public API
# =============================================================================
def subtitle_one_line(
    payload: Dict[str, Any],
    *,
    market: str,
    asof: str,
    lang: str,
    normalize_market,
) -> str:
    """
    Subtitle formatter (fixed 2 lines):

      "{Data date label} YYYY-MM-DD"
      "{Updated label} YYYY-MM-DD HH:MM (UTC+XX)"

    Notes:
    - Fully localized labels for JP/KR/CN/TW/TH.
    - Dates remain numeric (universal).
    - Always shows update date to avoid cross-day confusion.

    ✅ Backward-compatible improvement:
    - If meta.time.market_finished_at_iso exists, prefer it to derive:
        update date, hm, and tz_offset (if missing).
      This fixes markets like AU where sector pages use ISO/UTC conversion.
    """
    _ = normalize_market
    _ = market
    _ = asof

    trade_ymd = date_for_display(payload)
    tmeta = _get_time_meta(payload)

    trade_lbl = _label("trade_date", lang)
    upd_lbl = _label("updated", lang)

    # ------------------------------------------------------------
    # Prefer ISO (if present) to avoid hm/date drift across systems
    # ------------------------------------------------------------
    iso = _safe_str(tmeta.get("market_finished_at_iso") or "")
    hm = _safe_str(tmeta.get("market_finished_hm") or "")
    finished_at = _safe_str(tmeta.get("market_finished_at") or "")

    update_ymd = _split_ymd_from_dt(finished_at) or trade_ymd

    # If ISO exists, use it to fill missing / correct parts
    if iso:
        iso_ymd, iso_hm, iso_off = _extract_from_iso(iso)

        # Fill or override ymd/hm if missing
        if iso_ymd:
            update_ymd = iso_ymd
        if iso_hm:
            hm = iso_hm

        # If tz_offset missing, inject from ISO offset (keeps _tz_display unchanged)
        if iso_off and not _safe_str(tmeta.get("market_tz_offset") or tmeta.get("market_utc_offset") or ""):
            tmeta = dict(tmeta)
            tmeta["market_tz_offset"] = iso_off

    # Fallback: no time info → only show data date (keep previous behavior)
    if not hm:
        return f"{trade_lbl} {trade_ymd}".strip()

    tz_disp = _tz_display(lang, tmeta)

    # ✅ Always 2 lines, always show update date + time
    return f"{trade_lbl} {trade_ymd}\n{upd_lbl} {update_ymd} {hm} {tz_disp}".strip()