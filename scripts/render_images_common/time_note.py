# scripts/render_images_common/time_note.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Tuple, Optional

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _parse_iso_dt(s: str) -> Optional[datetime]:
    """
    Parse ISO datetime string.
    Supports trailing 'Z'.
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


def normalize_market(m: str) -> str:
    m = (m or "").strip().upper()
    alias = {
        "TWN": "TW", "TAIWAN": "TW",
        "HKG": "HK", "HKEX": "HK",
        "CHN": "CN", "CHINA": "CN",
        "USA": "US", "NASDAQ": "US", "NYSE": "US",
        "JPN": "JP", "JAPAN": "JP",
        "KOR": "KR", "KOREA": "KR",
        "CAN": "CA", "CANADA": "CA", "TSX": "CA", "TSXV": "CA",
        "AUS": "AU", "AUSTRALIA": "AU", "ASX": "AU",
        "GBR": "UK", "GB": "UK", "UNITED KINGDOM": "UK", "LSE": "UK",
        "DEU": "DE", "DE": "DE", "GERMANY": "DE",
        "IND": "IN", "INDIA": "IN", "NSE": "IN", "BSE": "IN",
        "THA": "TH", "THAILAND": "TH", "SET": "TH",
        "PHL": "PH", "PHILIPPINES": "PH",
    }
    return alias.get(m, m or "")


def _default_market_tz(market: str) -> str:
    m = normalize_market(market)
    # ✅ defaults (DST handled by ZoneInfo)
    return {
        "US": "America/New_York",
        "CA": "America/Toronto",
        "UK": "Europe/London",
        "DE": "Europe/Berlin",
        "AU": "Australia/Sydney",
        "CN": "Asia/Shanghai",
        "TW": "Asia/Taipei",
        "JP": "Asia/Tokyo",
        "KR": "Asia/Seoul",
        "TH": "Asia/Bangkok",
        "IN": "Asia/Kolkata",
        "PH": "Asia/Manila",
        "HK": "Asia/Hong_Kong",
    }.get(m, "UTC")


def _market_label(market: str, lang: str) -> str:
    m = normalize_market(market)
    lang = (lang or "").lower()
    is_en = lang.startswith("en")

    if m == "US":
        return "US Eastern Time" if is_en else "美東時間"
    if m == "CA":
        return "Canada Time" if is_en else "加拿大時間"
    if m == "UK":
        return "UK Time" if is_en else "英國時間"
    if m == "DE":
        return "Germany Time" if is_en else "德國時間"
    if m == "AU":
        return "Australia Time" if is_en else "澳洲時間"
    if m == "CN":
        return "China Time" if is_en else "北京時間"
    if m == "TW":
        return "Taiwan Time" if is_en else "台灣時間"
    if m == "JP":
        return "Japan Time" if is_en else "日本時間"
    if m == "KR":
        return "Korea Time" if is_en else "韓國時間"
    if m == "TH":
        return "Thailand Time" if is_en else "泰國時間"
    if m == "IN":
        return "India Time" if is_en else "印度時間"
    if m == "PH":
        return "Philippines Time" if is_en else "菲律賓時間"
    if m == "HK":
        return "Hong Kong Time" if is_en else "香港時間"

    return "Market Time" if is_en else "市場時間"


def _session_label(slot: str, lang: str) -> str:
    s = (slot or "").strip().lower()
    lang = (lang or "").lower()
    is_en = lang.startswith("en")

    if is_en:
        if s == "close":
            return "Close"
        if s == "midday":
            return "Intraday"
        if s == "open":
            return "Open"
        return s.upper() if s else "Data"
    else:
        if s == "close":
            return "收盤"
        if s == "midday":
            return "盤中"
        if s == "open":
            return "開盤"
        return s or "數據"


def _to_market_dt(dt_any: str, *, market_tz: str) -> Optional[datetime]:
    """
    Convert dt_any (ISO string) to market timezone.
    - If dt is naive -> assume UTC (avoid local machine TZ leakage)
    """
    if not dt_any:
        return None
    dt = _parse_iso_dt(dt_any)
    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    if ZoneInfo is None:
        # can't convert; at least keep it UTC-aware
        return dt

    try:
        return dt.astimezone(ZoneInfo(market_tz))
    except Exception:
        return dt


def _fmt_offset(dt_mkt: datetime, lang: str) -> str:
    """
    Return like '(UTC-05:00)' for en, '（UTC-05:00）' for zh.
    """
    try:
        off = dt_mkt.utcoffset()
        if off is None:
            return ""
        total = int(off.total_seconds())
        sign = "+" if total >= 0 else "-"
        total = abs(total)
        hh = total // 3600
        mm = (total % 3600) // 60
        s = f"UTC{sign}{hh:02d}:{mm:02d}"
    except Exception:
        return ""

    is_en = (lang or "").lower().startswith("en")
    return f" ({s})" if is_en else f"（{s}）"


def build_time_note(payload: Dict[str, Any], *, market: str, lang: str) -> Tuple[str, str]:
    """
    Return (ymd, time_note)

    ✅ New behavior (stable):
    - Always convert "Updated" time into *market timezone*.
    - If payload does NOT already provide market-local time, it will still be correct.
    - If ymd/ymd_effective missing or wrong-day, it can fallback to market-local updated date.

    Priority of timestamps:
      1) meta.time.market_finished_at_utc / meta.pipeline_finished_at_utc  (UTC -> convert)
      2) meta.time.market_finished_at / meta.pipeline_finished_at_market  (convert defensively)
      3) payload.generated_at (convert)
      4) now_utc (convert)  [last resort only]

    market_tz source:
      meta.time.market_tz / meta.market_tz / meta.tz / default mapping
    """
    mkt = normalize_market(market)
    lang = (lang or "").strip()

    slot = _safe_str(payload.get("slot") or "")
    meta = payload.get("meta") or {}
    meta_time = (meta.get("time") or {}) if isinstance(meta, dict) else {}

    market_tz = (
        _safe_str(meta_time.get("market_tz") or "")
        or _safe_str(meta.get("market_tz") or meta.get("tz") or "")
        or _default_market_tz(mkt)
    )

    # -------------------------
    # Pick best timestamp
    # -------------------------
    ts = (
        _safe_str(meta_time.get("market_finished_at_utc") or "")
        or _safe_str(meta.get("pipeline_finished_at_utc") or "")
    )
    if not ts:
        ts = (
            _safe_str(meta_time.get("market_finished_at") or "")
            or _safe_str(meta.get("pipeline_finished_at_market") or "")
            or _safe_str(payload.get("generated_at") or "")
        )

    dt_mkt = _to_market_dt(ts, market_tz=market_tz) if ts else None

    if dt_mkt is None:
        # last resort: now (UTC -> market)
        now_utc = datetime.now(timezone.utc)
        if ZoneInfo is not None:
            try:
                dt_mkt = now_utc.astimezone(ZoneInfo(market_tz))
            except Exception:
                dt_mkt = now_utc
        else:
            dt_mkt = now_utc

    hhmm = dt_mkt.strftime("%H:%M")
    offset_part = _fmt_offset(dt_mkt, lang)

    # -------------------------
    # ymd (prefer effective first; else fallback to market-local date)
    # -------------------------
    ymd = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    if not ymd:
        ymd = dt_mkt.strftime("%Y-%m-%d")

    market_label = _market_label(mkt, lang)
    session = _session_label(slot, lang)

    is_en = (lang or "").lower().startswith("en")
    if is_en:
        if ymd:
            note = f"{market_label} {ymd} {session} | Updated {hhmm}{offset_part}".strip()
        else:
            note = f"{market_label} {session} | Updated {hhmm}{offset_part}".strip()
    else:
        if ymd:
            note = f"{market_label} {ymd} {session}｜更新 {hhmm}{offset_part}".strip()
        else:
            note = f"{market_label} {session}｜更新 {hhmm}{offset_part}".strip()

    return ymd, note