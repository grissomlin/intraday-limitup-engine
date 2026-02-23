# markets/timekit.py
# -*- coding: utf-8 -*-
"""
Unified market time utilities (DST-aware when ZoneInfo works; safe fallback otherwise)

Design goals
- One shared logic for:
  - "market today" (ymd folder)
  - "asof" local time
  - meta.time payload fields used by renderers
- Works on:
  - Linux CI (usually has tzdb => DST correct)
  - Windows (tzdb may be missing => fallback to fixed UTC offset)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore


# -----------------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------------
DEFAULT_MARKET_TZ: Dict[str, str] = {
    "US": "America/New_York",
    "CA": "America/Toronto",
    "UK": "Europe/London",
    "AU": "Australia/Sydney",
    "TW": "Asia/Taipei",
    "CN": "Asia/Shanghai",
    "JP": "Asia/Tokyo",
    "KR": "Asia/Seoul",
    "HK": "Asia/Hong_Kong",
    "TH": "Asia/Bangkok",
    "IN": "Asia/Kolkata",
}

# hours may be fractional (e.g. India +5.5)
DEFAULT_TZ_OFFSETS: Dict[str, float] = {
    "US": -5.0,   # Eastern (DST handled by ZoneInfo if available)
    "CA": -5.0,   # Toronto
    "UK": 0.0,    # London
    "AU": +10.0,  # Sydney (DST handled by ZoneInfo if available)
    "TW": +8.0,
    "CN": +8.0,
    "JP": +9.0,
    "KR": +9.0,
    "HK": +8.0,
    "TH": +7.0,
    "IN": +5.5,
}


def _norm_market(market: str) -> str:
    return (market or "").strip().upper()


def _env_bool(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_str(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def _parse_float(s: str) -> Optional[float]:
    try:
        ss = str(s).strip()
        if not ss:
            return None
        return float(ss)
    except Exception:
        return None


def _tz_offset_str(hours: float) -> str:
    # hours may be fractional (e.g. 5.5)
    sign = "+" if hours >= 0 else "-"
    ah = abs(float(hours))
    hh = int(ah)
    mm = int(round((ah - hh) * 60))
    # normalize rounding edge case
    if mm >= 60:
        hh += 1
        mm -= 60
    return f"{sign}{hh:02d}:{mm:02d}"


def _hours_to_timedelta(hours: float) -> timedelta:
    sign = 1 if hours >= 0 else -1
    ah = abs(float(hours))
    hh = int(ah)
    mm = int(round((ah - hh) * 60))
    if mm >= 60:
        hh += 1
        mm -= 60
    return sign * timedelta(hours=hh, minutes=mm)


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def market_tz_name(market: str) -> str:
    """
    Resolve market tz name (IANA) for ZoneInfo.
    Override with env:
      INTRADAY_MARKET_TZ_<MARKET>=Australia/Sydney
    """
    m = _norm_market(market)
    env_name = f"INTRADAY_MARKET_TZ_{m}"
    v = _env_str(env_name)
    if v:
        return v
    return DEFAULT_MARKET_TZ.get(m, "UTC")


def market_offset_hours(market: str) -> float:
    """
    Resolve fallback offset hours (fixed offset) when ZoneInfo not usable.
    Override with env:
      INTRADAY_TZ_OFFSET_<MARKET>=10
      INTRADAY_TZ_OFFSET_<MARKET>=5.5
      INTRADAY_TZ_OFFSET_<MARKET>=-5
    """
    m = _norm_market(market)
    env_name = f"INTRADAY_TZ_OFFSET_{m}"
    v = _parse_float(_env_str(env_name))
    if v is not None:
        return float(v)
    return float(DEFAULT_TZ_OFFSETS.get(m, 0.0))


def get_market_tzinfo(market: str, *, dt_utc: Optional[datetime] = None) -> timezone | Any:
    """
    Prefer ZoneInfo(IANA name) for DST correctness.
    If ZoneInfo missing or tzdb missing, fallback to fixed UTC offset.
    """
    tz_name = market_tz_name(market)
    if ZoneInfo is not None:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            pass

    off_h = market_offset_hours(market)
    return timezone(_hours_to_timedelta(off_h))


def market_now(market: str, *, dt_utc: Optional[datetime] = None) -> datetime:
    if dt_utc is None:
        dt_utc = datetime.now(timezone.utc)
    tzinfo = get_market_tzinfo(market, dt_utc=dt_utc)
    try:
        return dt_utc.astimezone(tzinfo)
    except Exception:
        return dt_utc


def market_today_ymd(market: str, *, dt_utc: Optional[datetime] = None) -> str:
    return market_now(market, dt_utc=dt_utc).strftime("%Y-%m-%d")


def market_now_hhmm(market: str, *, dt_utc: Optional[datetime] = None) -> str:
    return market_now(market, dt_utc=dt_utc).strftime("%H:%M")


def build_market_time_meta(
    market: str,
    *,
    started_utc: datetime,
    finished_utc: datetime,
) -> Dict[str, Any]:
    """
    Unified meta.time.
    - Uses ZoneInfo when available, else fixed offset.
    - Provides both an ISO timestamp with offset and an explicit offset string.
    """
    tzinfo = get_market_tzinfo(market, dt_utc=finished_utc)
    tz_name = market_tz_name(market)

    try:
        finished_local = finished_utc.astimezone(tzinfo)
    except Exception:
        finished_local = finished_utc

    try:
        started_local = started_utc.astimezone(tzinfo)
    except Exception:
        started_local = started_utc

    off_td = None
    try:
        off_td = finished_local.utcoffset()
    except Exception:
        off_td = None

    # If tzinfo doesn't yield offset (rare), fallback to configured offset hours
    if off_td is None:
        off_td = _hours_to_timedelta(market_offset_hours(market))

    off_hours = off_td.total_seconds() / 3600.0
    off_str = _tz_offset_str(float(off_hours))

    duration = max(0.0, (finished_utc - started_utc).total_seconds())

    def _iso_z(dt: datetime) -> str:
        return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

    return {
        # UTC anchors (always)
        "started_at_utc": _iso_z(started_utc),
        "finished_at_utc": _iso_z(finished_utc),
        "duration_seconds": int(duration),

        # Market timezone identity
        "market_tz": tz_name,                # IANA if provided, else "UTC"
        "market_tz_offset": off_str,         # "+11:00"
        "market_utc_offset": off_str,        # alias (renderers use either)

        # Human-friendly local strings
        "market_started_at": started_local.strftime("%Y-%m-%d %H:%M"),
        "market_finished_at": finished_local.strftime("%Y-%m-%d %H:%M"),
        "market_finished_hm": finished_local.strftime("%H:%M"),

        # ISO local timestamp with offset (best for robust parsing)
        "market_finished_at_iso": finished_local.isoformat(timespec="minutes"),
    }
