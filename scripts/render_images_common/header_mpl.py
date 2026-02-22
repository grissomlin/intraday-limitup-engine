# scripts/render_images_common/header_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Tuple


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _parse_hhmm_from_iso(dt_iso: str) -> str:
    """
    Accept 'YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DD HH:MM:SS'
    Return 'HH:MM' if possible.
    """
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


def get_market_time_info(payload: Dict[str, Any], *, market: str = "CN") -> Tuple[str, str, str, str]:
    """
    Return: (market_label, ymd, session_zh, hhmm)

    CN desired output usage:
      time_note = f"{market_label}{ymd} 截止 {hhmm}"
    """
    meta = payload.get("meta") or {}
    ymd = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    slot = _safe_str(payload.get("slot") or "")

    # session label
    if slot == "close":
        session = "收盘"
    elif slot == "midday":
        session = "盘中"
    elif slot == "open":
        session = "开盘"
    else:
        session = "盘中" if slot else "盘中"

    # prefer pipeline_finished_at_market, fallback to generated_at/asof
    updated_market = _safe_str(meta.get("pipeline_finished_at_market") or "")
    if not updated_market:
        updated_market = _safe_str(payload.get("generated_at") or "")
    hhmm = _parse_hhmm_from_iso(updated_market) or _safe_str(payload.get("asof") or "")

    if market.upper() == "CN":
        market_label = _safe_str(meta.get("market_label") or "") or "中国时间"
        # CN: usually no need to show UTC offset on image
        return market_label, ymd, session, hhmm

    # generic fallback for other markets
    market_label = _safe_str(meta.get("market_label") or "") or "当地时间"
    return market_label, ymd, session, hhmm
