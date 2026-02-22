# markets/common/time_builders.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _parse_utc_offset(s: str) -> Optional[timedelta]:
    """
    Parse "+11:00" / "+10" / "-05:30" -> timedelta.
    """
    ss = str(s or "").strip()
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


def _format_offset(td: timedelta) -> str:
    """
    "+11:00" style.
    """
    total = int(td.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hh = total // 3600
    mm = (total % 3600) // 60
    return f"{sign}{hh:02d}:{mm:02d}"


def _build_meta_time_by_tz(
    dt_utc: datetime,
    *,
    tz_name: str,
    fallback_offset: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build meta.time in ONE standard schema used by renderers / overview:

      {
        market_tz,
        market_tz_offset,
        market_utc_offset,
        market_finished_at,        # "YYYY-MM-DD HH:MM"
        market_finished_hm,        # "HH:MM"
        market_finished_at_iso,    # ISO with offset, minutes precision
        market_finished_at_utc     # ISO Z
      }

    Priority:
      1) ZoneInfo(tz_name) (DST correct)
      2) fallback_offset if provided (e.g. "+07:00")
      3) UTC as last resort
    """
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)

    # 1) ZoneInfo (best)
    if ZoneInfo is not None:
        try:
            tzinfo = ZoneInfo(tz_name)
            dt_local = dt_utc.astimezone(tzinfo)
            off = dt_local.utcoffset() or timedelta(0)

            return {
                "market_tz": tz_name,
                "market_tz_offset": _format_offset(off),
                "market_utc_offset": _format_offset(off),  # alias, MUST match
                "market_finished_at": dt_local.strftime("%Y-%m-%d %H:%M"),
                "market_finished_hm": dt_local.strftime("%H:%M"),
                "market_finished_at_iso": dt_local.isoformat(timespec="minutes"),
                "market_finished_at_utc": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
            }
        except Exception:
            pass

    # 2) Fallback fixed offset
    td = _parse_utc_offset(fallback_offset or "")
    if td is not None:
        tzinfo = timezone(td)
        dt_local = dt_utc.astimezone(tzinfo)
        return {
            "market_tz": tz_name,
            "market_tz_offset": _format_offset(td),
            "market_utc_offset": _format_offset(td),
            "market_finished_at": dt_local.strftime("%Y-%m-%d %H:%M"),
            "market_finished_hm": dt_local.strftime("%H:%M"),
            "market_finished_at_iso": dt_local.isoformat(timespec="minutes"),
            "market_finished_at_utc": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        }

    # 3) Last resort: UTC
    dt_local = dt_utc.astimezone(timezone.utc)
    return {
        "market_tz": tz_name or "UTC",
        "market_tz_offset": "+00:00",
        "market_utc_offset": "+00:00",
        "market_finished_at": dt_local.strftime("%Y-%m-%d %H:%M"),
        "market_finished_hm": dt_local.strftime("%H:%M"),
        "market_finished_at_iso": dt_local.isoformat(timespec="minutes").replace("+00:00", "Z"),
        "market_finished_at_utc": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
    }


# -----------------------------------------------------------------------------
# Public builders (what your snapshots should call)
# -----------------------------------------------------------------------------
def build_meta_time_america(
    dt_utc: datetime,
    *,
    tz_name: str,
    fallback_offset: Optional[str] = None,
) -> Dict[str, Any]:
    """
    North America unified builder.
    Examples:
      - "America/New_York"
      - "America/Toronto"
      - "America/Vancouver"
    """
    return _build_meta_time_by_tz(dt_utc, tz_name=tz_name, fallback_offset=fallback_offset)


def build_meta_time_asia(
    dt_utc: datetime,
    *,
    tz_name: str,
    fallback_offset: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Asia unified builder.
    Examples:
      - "Asia/Tokyo"
      - "Asia/Seoul"
      - "Asia/Bangkok"
    """
    return _build_meta_time_by_tz(dt_utc, tz_name=tz_name, fallback_offset=fallback_offset)