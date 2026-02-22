# scripts/render_images_tw/sector_blocks/_time.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Tuple


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _parse_ymd_hhmm_from_iso(dt_iso: str) -> Tuple[str, str]:
    """
    Try parse ISO-ish datetime string and return (YYYY-MM-DD, HH:MM).
    Accepts:
      - 2026-02-11T14:49:00+08:00
      - 2026-02-11 14:49:00
      - 2026-02-11T14:49:00Z
    If missing/invalid -> ("", "")
    """
    s = _safe_str(dt_iso)
    if not s:
        return "", ""

    try:
        # split date/time
        if "T" in s:
            d, t = s.split("T", 1)
        elif " " in s:
            d, t = s.split(" ", 1)
        else:
            # maybe only time
            return "", s[:5]

        ymd = d[:10] if len(d) >= 10 else ""
        hhmm = t[:5] if len(t) >= 5 else ""
        return ymd, hhmm
    except Exception:
        return "", ""


def parse_cutoff(payload: Dict[str, Any]) -> str:
    # trading date (data date)
    ymd = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    return ymd or ""


def get_market_time_info(payload: Dict[str, Any]) -> Tuple[str, str]:
    """
    Returns (ymd_data, time_note)

    time_note will be TWO lines if possible:
      line1: "{market_label} {ymd_data} {session}"
      line2: "更新 {update_ymd} {hhmm}（UTC+08:00）"

    If update_ymd not available, fallback to:
      - asof hh:mm (no date) -> use line2: "更新 {hhmm}（UTC+08:00）"
    """
    ymd_data = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    slot = _safe_str(payload.get("slot") or "")
    meta = payload.get("meta") or {}

    if slot == "close":
        session = "收盤"
    elif slot == "midday":
        session = "盤中"
    elif slot == "open":
        session = "開盤"
    else:
        session = slot or "資料"

    market_label = _safe_str(meta.get("market_label") or "") or "台灣交易日"

    updated_market = _safe_str(meta.get("pipeline_finished_at_market") or "")
    if not updated_market:
        updated_market = _safe_str(payload.get("generated_at") or "")

    upd_ymd, hhmm = _parse_ymd_hhmm_from_iso(updated_market)

    # final fallback: payload.asof may be "14:49" or "2026-02-11 14:49"
    if not hhmm:
        asof = _safe_str(payload.get("asof") or "")
        a_ymd, a_hhmm = _parse_ymd_hhmm_from_iso(asof)
        hhmm = a_hhmm or asof[:5]
        if not upd_ymd:
            upd_ymd = a_ymd

    offset = _safe_str(meta.get("market_utc_offset") or "") or "UTC+08:00"
    offset_part = f"（{offset}）" if offset else ""

    # line1: market + data date + session
    if ymd_data:
        line1 = f"{market_label} {ymd_data} {session}".strip()
    else:
        line1 = f"{market_label} {session}".strip()

    # line2: update date+time preferred
    if upd_ymd and hhmm:
        line2 = f"更新 {upd_ymd} {hhmm}{offset_part}".strip()
    elif hhmm:
        line2 = f"更新 {hhmm}{offset_part}".strip()
    else:
        # very last fallback
        line2 = f"更新 {offset_part}".strip() if offset_part else "更新"

    time_note = f"{line1}\n{line2}".strip()
    return ymd_data, time_note