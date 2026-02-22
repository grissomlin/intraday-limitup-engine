# markets/_calendar_cache.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import yfinance as yf


def _safe_ticker(t: str) -> str:
    return (t or "").replace("^", "").replace("=", "_").replace("/", "_").replace("\\", "_").strip() or "ticker"


def _calendar_cache_path(*, cache_root: str, market: str, calendar_ticker: str, asof_ymd: str) -> str:
    os.makedirs(cache_root, exist_ok=True)
    fn = f"{market}_{_safe_ticker(calendar_ticker)}_{asof_ymd}.json"
    return os.path.join(cache_root, fn)


def _load_calendar_cache(path: str) -> Optional[dict]:
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_calendar_cache(path: str, payload: dict) -> None:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _download_calendar_dates(
    *,
    ticker: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    timeout_sec: int = 30,
) -> List[pd.Timestamp]:
    df = yf.download(
        ticker,
        start=start_dt.strftime("%Y-%m-%d"),
        end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        progress=False,
        timeout=timeout_sec,
        auto_adjust=True,
        threads=False,
    )
    if df is None or df.empty:
        return []
    dates = pd.to_datetime(df.index).tz_localize(None).normalize()
    return sorted(pd.Index(dates).unique().tolist())


def _fallback_payload(
    *,
    cache_path: str,
    calendar_ticker: str,
    asof_ymd: str,
    fallback_rolling_cal_days: int,
    error: str,
    latest_ymd: Optional[str] = None,
) -> Dict[str, Any]:
    latest = latest_ymd or asof_ymd
    start_ymd = (pd.to_datetime(latest) - timedelta(days=fallback_rolling_cal_days)).strftime("%Y-%m-%d")
    end_ymd = latest
    end_excl = (pd.to_datetime(end_ymd) + timedelta(days=1)).strftime("%Y-%m-%d")
    return {
        "asof_ymd": asof_ymd,
        "latest_ymd": latest,
        "start_ymd": start_ymd,
        "end_ymd": end_ymd,
        "end_excl_ymd": end_excl,
        "mode": "cal_days",
        "error": error,
        "calendar_ticker": calendar_ticker,
        "cache_path": cache_path,
    }


def _get_trading_window_cached(
    *,
    market: str,
    calendar_ticker: str,
    asof_ymd: str,
    n_trading_days: int,
    lookback_cal_days: int,
    fallback_rolling_cal_days: int,
    cache_root: str,
) -> Dict[str, Any]:
    """
    Returns dict:
      {
        "asof_ymd": "...",
        "latest_ymd": "...",
        "start_ymd": "...",
        "end_ymd": "...",
        "end_excl_ymd": "...",
        "mode": "trading_days"|"cal_days",
        "error": "... or None",
        "calendar_ticker": "...",
        "cache_path": "...",
      }
    """
    cache_path = _calendar_cache_path(
        cache_root=cache_root,
        market=market,
        calendar_ticker=calendar_ticker,
        asof_ymd=asof_ymd,
    )
    cached = _load_calendar_cache(cache_path)
    if cached and isinstance(cached, dict) and cached.get("asof_ymd") == asof_ymd:
        return cached

    end_dt = pd.to_datetime(asof_ymd)
    start_dt = end_dt - timedelta(days=int(max(30, lookback_cal_days)))

    err: Optional[str] = None
    try:
        dates_dt = _download_calendar_dates(ticker=calendar_ticker, start_dt=start_dt, end_dt=end_dt)
    except Exception as e:
        dates_dt = []
        err = f"calendar_exception: {e}"

    if not dates_dt:
        payload = _fallback_payload(
            cache_path=cache_path,
            calendar_ticker=calendar_ticker,
            asof_ymd=asof_ymd,
            fallback_rolling_cal_days=fallback_rolling_cal_days,
            error=err or "calendar_empty",
        )
        _save_calendar_cache(cache_path, payload)
        return payload

    # <= asof
    dates_dt = [d for d in dates_dt if d <= end_dt.normalize()]
    if not dates_dt:
        payload = _fallback_payload(
            cache_path=cache_path,
            calendar_ticker=calendar_ticker,
            asof_ymd=asof_ymd,
            fallback_rolling_cal_days=fallback_rolling_cal_days,
            error=err or "calendar_filtered_empty",
        )
        _save_calendar_cache(cache_path, payload)
        return payload

    latest_ymd = dates_dt[-1].strftime("%Y-%m-%d")

    if len(dates_dt) >= max(5, n_trading_days):
        end_incl = dates_dt[-1]
        start_incl = dates_dt[-n_trading_days]
        end_excl = end_incl + timedelta(days=1)
        payload = {
            "asof_ymd": asof_ymd,
            "latest_ymd": latest_ymd,
            "start_ymd": start_incl.strftime("%Y-%m-%d"),
            "end_ymd": end_incl.strftime("%Y-%m-%d"),
            "end_excl_ymd": end_excl.strftime("%Y-%m-%d"),
            "mode": "trading_days",
            "error": err,
            "calendar_ticker": calendar_ticker,
            "cache_path": cache_path,
        }
        _save_calendar_cache(cache_path, payload)
        return payload

    # insufficient dates -> fallback cal-days (but keep latest)
    payload = _fallback_payload(
        cache_path=cache_path,
        calendar_ticker=calendar_ticker,
        asof_ymd=asof_ymd,
        fallback_rolling_cal_days=fallback_rolling_cal_days,
        error=err or "calendar_insufficient_dates",
        latest_ymd=latest_ymd,
    )
    _save_calendar_cache(cache_path, payload)
    return payload
