# scripts/render_images_us/sector_blocks/_time.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone

# Optional shared time note builder
try:
    from scripts.render_images_common.time_note import build_time_note as _build_time_note  # type: ignore
except Exception:
    _build_time_note = None  # type: ignore

# Optional unified subtitle template (overview/timefmt.py)
try:
    from scripts.render_images_common.overview.timefmt import subtitle_one_line as _subtitle_one_line  # type: ignore
except Exception:
    _subtitle_one_line = None  # type: ignore

# ZoneInfo (DST-aware)
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def parse_cutoff(payload: Dict[str, Any]) -> str:
    # US: display effective trading day first (align with UK)
    ymd = str(payload.get("ymd_effective") or payload.get("ymd") or payload.get("bar_date") or "").strip()
    return ymd or ""


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


def _to_market_hhmm(dt_any: str, *, market_tz: str) -> str:
    """
    Convert dt_any (ISO string) to market timezone and return HH:MM.
    If conversion fails, fallback to raw HH:MM slicing.
    """
    if not dt_any:
        return ""
    if ZoneInfo is None:
        return _parse_hhmm_from_iso(dt_any)

    dt = _parse_iso_dt(dt_any)
    if dt is None:
        return _parse_hhmm_from_iso(dt_any)

    # If dt is naive, assume it's UTC (safer than local machine TZ)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    try:
        tz = ZoneInfo(market_tz)
        dt2 = dt.astimezone(tz)
        return dt2.strftime("%H:%M")
    except Exception:
        return _parse_hhmm_from_iso(dt_any)


def _session_name(slot: str) -> str:
    s = _safe_str(slot).lower()
    if s == "close":
        return "Close"
    if s == "midday":
        return "Intraday"
    if s == "open":
        return "Open"
    return slot.upper() if slot else "Data"


def get_market_time_info(payload: Dict[str, Any]) -> Tuple[str, str]:
    """
    US sector blocks subtitle builder.

    ✅ Priority (US-specific):
    0) Use payload.meta.time from snapshot builder (authoritative):
       - market_finished_ymd
       - market_finished_hm
       - market_tz_offset
       This fixes:
         - wrong timezone (printing UTC)
         - missing date after "Updated"
         - wrong (UTC+00:00) suffix

    1) fallback: legacy conversion logic (kept)
    2) last resort: shared builders (they may not match US formatting)
    """
    ymd = parse_cutoff(payload)
    slot = _safe_str(payload.get("slot") or "")
    session = _session_name(slot)

    market_label = "US Eastern Time"

    meta = payload.get("meta") or {}
    meta_time = (meta.get("time") or {}) if isinstance(meta, dict) else {}

    # ---------------------------------------------------------------------
    # 0) ✅ Authoritative meta.time (from markets/us/us_snapshot.py)
    # ---------------------------------------------------------------------
    if isinstance(meta_time, dict) and meta_time:
        upd_ymd = _safe_str(meta_time.get("market_finished_ymd") or "")
        upd_hm = _safe_str(meta_time.get("market_finished_hm") or "")
        tz_off = _safe_str(meta_time.get("market_tz_offset") or "")

        # allow fallback: some builds might only have "market_finished_at_market" or "market_finished_at"
        if (not upd_ymd or not upd_hm) and meta_time.get("market_finished_at_market"):
            s = _safe_str(meta_time.get("market_finished_at_market"))
            # expects "YYYY-MM-DD HH:MM"
            if len(s) >= 16 and " " in s:
                upd_ymd = upd_ymd or s[:10]
                upd_hm = upd_hm or s[11:16]
        if (not upd_ymd or not upd_hm) and meta_time.get("market_finished_at"):
            s = _safe_str(meta_time.get("market_finished_at"))
            if len(s) >= 16 and " " in s:
                upd_ymd = upd_ymd or s[:10]
                upd_hm = upd_hm or s[11:16]

        if upd_ymd and upd_hm and tz_off:
            line1 = f"{market_label} {ymd} {session}".strip() if ymd else f"{market_label} {session}".strip()
            line2 = f"Updated {upd_ymd} {upd_hm} (UTC{tz_off})"
            return (ymd, f"{line1} | {line2}")

        # if we at least have date+time but no offset, still print date+time (better than UTC+00:00)
        if upd_ymd and upd_hm and not tz_off:
            line1 = f"{market_label} {ymd} {session}".strip() if ymd else f"{market_label} {session}".strip()
            line2 = f"Updated {upd_ymd} {upd_hm}"
            return (ymd, f"{line1} | {line2}")

    # ---------------------------------------------------------------------
    # 1) Legacy fallback (kept)
    # ---------------------------------------------------------------------
    market_tz = "America/New_York"

    updated_market_str = _safe_str(meta.get("pipeline_finished_at_market") or "")
    if updated_market_str:
        hhmm = _to_market_hhmm(updated_market_str, market_tz=market_tz)
        upd_ymd = _safe_str(updated_market_str)[:10] if len(_safe_str(updated_market_str)) >= 10 else ""
    else:
        updated_utc = (
            _safe_str(meta.get("pipeline_finished_at_utc") or "")
            or _safe_str(meta_time.get("market_finished_at_utc") or "")
        )
        if updated_utc:
            hhmm = _to_market_hhmm(updated_utc, market_tz=market_tz)
            # try parse UTC -> NY date
            upd_ymd = ""
            dt = _parse_iso_dt(updated_utc)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if ZoneInfo is not None:
                    try:
                        dt2 = dt.astimezone(ZoneInfo(market_tz))
                        upd_ymd = dt2.strftime("%Y-%m-%d")
                    except Exception:
                        upd_ymd = ""
        else:
            updated_any = (
                _safe_str(meta_time.get("market_finished_at") or "")
                or _safe_str(payload.get("generated_at") or "")
                or _safe_str(meta.get("pipeline_finished_at") or "")
            )
            if updated_any:
                hhmm = _to_market_hhmm(updated_any, market_tz=market_tz)
                upd_ymd = _safe_str(updated_any)[:10] if len(_safe_str(updated_any)) >= 10 else ""
            else:
                hhmm = _safe_str(payload.get("asof") or "")
                hhmm = hhmm[:5] if hhmm else ""
                upd_ymd = ""

    # If we can, include date after Updated
    if upd_ymd and hhmm:
        line1 = f"{market_label} {ymd} {session}".strip() if ymd else f"{market_label} {session}".strip()
        line2 = f"Updated {upd_ymd} {hhmm}"
        return (ymd, f"{line1} | {line2}")

    if ymd:
        time_note = f"{market_label} {ymd} {session} | Updated {hhmm}".strip()
    else:
        time_note = f"{market_label} {session} | Updated {hhmm}".strip()

    # ---------------------------------------------------------------------
    # 2) last resort (shared builders) — ONLY if we got nothing useful above
    # ---------------------------------------------------------------------
    # NOTE: kept for completeness, but they may output formats you don't want.
    if _build_time_note is not None:
        try:
            ymd2, note = _build_time_note(payload, market="US", lang="en")
            if _safe_str(note):
                return (_safe_str(ymd2) or ymd, _safe_str(note))
        except Exception:
            pass

    if _subtitle_one_line is not None:
        try:
            note = _subtitle_one_line(
                payload,
                market="US",
                asof=_safe_str(payload.get("asof") or ""),
                lang="en",
                normalize_market=lambda x: x,
            )
            if _safe_str(note):
                return (ymd, _safe_str(note))
        except Exception:
            pass

    return ymd, time_note