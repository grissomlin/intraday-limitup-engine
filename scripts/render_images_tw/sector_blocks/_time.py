# scripts/render_images_tw/sector_blocks/_time.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Tuple


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def parse_cutoff(payload: Dict[str, Any]) -> str:
    # trade date for display (prefer ymd_effective)
    return _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")


def _slot_zh(slot: str) -> str:
    s = (slot or "").strip().lower()
    if s == "open":
        return "開盤"
    if s == "close":
        return "收盤"
    if s == "midday":
        return "盤中"
    # fallback
    return "盤中"


def _format_trade_line(payload: Dict[str, Any]) -> str:
    ymd = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    slot = _slot_zh(_safe_str(payload.get("slot") or "midday"))
    if ymd:
        return f"台灣交易日 {ymd} {slot}"
    return f"台灣交易日 {slot}"


def _format_update_line(payload: Dict[str, Any]) -> str:
    """
    ✅ Correct rule (match overview):
    - Prefer meta.time.market_finished_at (already market-local "YYYY-MM-DD HH:MM")
    - Prefer meta.time.market_tz_offset / market_utc_offset (e.g. "+08:00") -> show as "UTC+08:00"
    Fallback:
    - payload.generated_at (best effort); if it looks like UTC ISO "....Z", label as "(UTC)".
    """
    meta = payload.get("meta") or {}
    t = meta.get("time") or {}

    produced_at = _safe_str(t.get("market_finished_at"))
    off = _safe_str(t.get("market_tz_offset") or t.get("market_utc_offset"))

    # If we have market-local produced_at, we must NOT accidentally show UTC string.
    if produced_at:
        tz_label = f"UTC{off}" if off else ""
        return f"更新 {produced_at}" + (f" ({tz_label})" if tz_label else "")

    # -----------------------
    # Fallback: generated_at
    # -----------------------
    ga = _safe_str(payload.get("generated_at"))
    if not ga:
        return ""

    # common shapes:
    #  - 2026-02-23T05:15:12Z   (UTC)
    #  - 2026-02-23T05:15:12+00:00
    #  - 2026-02-23 13:15
    show = ga
    label = ""

    if "T" in ga and len(ga) >= 16:
        # keep "YYYY-MM-DD HH:MM"
        show = ga.replace("T", " ", 1)[:16]
        if ga.endswith("Z") or "+00:00" in ga:
            label = "UTC"
    elif " " in ga and len(ga) >= 16:
        show = ga[:16]

    if label:
        return f"更新 {show} ({label})"
    return f"更新 {show}"


def get_market_time_info(payload: Dict[str, Any]) -> Tuple[str, str]:
    """
    Returns:
      (ymd_for_folder_or_display, time_note_two_lines)

    time_note is two lines:
      line1: 台灣交易日 YYYY-MM-DD 盤中
      line2: 更新 YYYY-MM-DD HH:MM (UTC+08:00)
    """
    ymd = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    line1 = _format_trade_line(payload)
    line2 = _format_update_line(payload)

    note = line1
    if line2:
        note = f"{line1}\n{line2}"

    return ymd, note
