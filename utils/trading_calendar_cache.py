# utils/trading_calendar_cache.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd
import yfinance as yf


@dataclass
class CalendarResult:
    ticker: str
    asof_ymd: str
    latest_ymd: Optional[str]
    dates: List[str]              # trading dates as YYYY-MM-DD, sorted
    start_ymd: Optional[str]      # last N trading days start (inclusive)
    end_ymd: Optional[str]        # end (inclusive)
    end_excl_ymd: Optional[str]   # end exclusive YYYY-MM-DD (end+1 day)
    mode: str                     # "trading_days" or "cal_days"
    error: Optional[str] = None


def _today_ymd() -> str:
    return pd.Timestamp.now().strftime("%Y-%m-%d")


def _cache_dir(root: str) -> str:
    os.makedirs(root, exist_ok=True)
    return root


def _cache_path(root: str, market: str, ticker: str, asof_ymd: str) -> str:
    # one cache per market+ticker+day
    safe_ticker = ticker.replace("^", "").replace("=", "_").replace("/", "_")
    return os.path.join(_cache_dir(root), f"{market}_{safe_ticker}_{asof_ymd}.json")


def _read_cache(path: str) -> Optional[dict]:
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_cache(path: str, payload: dict) -> None:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _download_calendar_dates(ticker: str, start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> List[pd.Timestamp]:
    df = yf.download(
        ticker,
        start=start_dt.strftime("%Y-%m-%d"),
        end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        progress=False,
        timeout=30,
        auto_adjust=True,
        threads=False,
    )
    if df is None or df.empty:
        return []
    dates = pd.to_datetime(df.index).tz_localize(None).normalize()
    dates = sorted(pd.Index(dates).unique().tolist())
    return dates


def get_trading_window_cached(
    *,
    market: str,
    calendar_ticker: str,
    n_trading_days: int = 30,
    lookback_cal_days: int = 180,
    cache_root: str = "data/cache/calendar",
    asof_ymd: Optional[str] = None,          # default today
    fallback_rolling_cal_days: int = 90,     # if calendar fails
) -> CalendarResult:
    """
    Returns latest trading day + last N trading days window using a cached calendar.

    Cache key: (market, calendar_ticker, asof_ymd)
    TTL: same-day (you can re-generate by changing asof_ymd or deleting cache file)
    """
    asof_ymd = asof_ymd or _today_ymd()
    cpath = _cache_path(cache_root, market, calendar_ticker, asof_ymd)

    cached = _read_cache(cpath)
    if cached:
        return CalendarResult(**cached)

    # build fresh
    end_dt = pd.to_datetime(asof_ymd)
    start_dt = end_dt - timedelta(days=lookback_cal_days)
    mode = "trading_days"
    err = None

    try:
        dates_dt = _download_calendar_dates(calendar_ticker, start_dt, end_dt)
    except Exception as e:
        dates_dt = []
        err = f"calendar_exception: {e}"

    if not dates_dt:
        # fallback cal-days window (no trading calendar)
        mode = "cal_days"
        latest = asof_ymd
        start_ymd = (pd.to_datetime(asof_ymd) - timedelta(days=fallback_rolling_cal_days)).strftime("%Y-%m-%d")
        end_ymd = asof_ymd
        end_excl = (pd.to_datetime(end_ymd) + timedelta(days=1)).strftime("%Y-%m-%d")
        res = CalendarResult(
            ticker=calendar_ticker,
            asof_ymd=asof_ymd,
            latest_ymd=latest,
            dates=[],
            start_ymd=start_ymd,
            end_ymd=end_ymd,
            end_excl_ymd=end_excl,
            mode=mode,
            error=err or "calendar_empty",
        )
        _write_cache(cpath, res.__dict__)
        return res

    # filter <= asof
    dates_dt = [d for d in dates_dt if d <= end_dt.normalize()]
    latest = dates_dt[-1].strftime("%Y-%m-%d")

    if len(dates_dt) >= max(5, n_trading_days):
        end_incl = dates_dt[-1]
        start_incl = dates_dt[-n_trading_days]
        end_excl = end_incl + timedelta(days=1)
        res = CalendarResult(
            ticker=calendar_ticker,
            asof_ymd=asof_ymd,
            latest_ymd=latest,
            dates=[d.strftime("%Y-%m-%d") for d in dates_dt],
            start_ymd=start_incl.strftime("%Y-%m-%d"),
            end_ymd=end_incl.strftime("%Y-%m-%d"),
            end_excl_ymd=end_excl.strftime("%Y-%m-%d"),
            mode=mode,
            error=err,
        )
    else:
        # insufficient dates -> fallback cal-days
        mode = "cal_days"
        start_ymd = (pd.to_datetime(latest) - timedelta(days=fallback_rolling_cal_days)).strftime("%Y-%m-%d")
        end_ymd = latest
        end_excl = (pd.to_datetime(end_ymd) + timedelta(days=1)).strftime("%Y-%m-%d")
        res = CalendarResult(
            ticker=calendar_ticker,
            asof_ymd=asof_ymd,
            latest_ymd=latest,
            dates=[d.strftime("%Y-%m-%d") for d in dates_dt],
            start_ymd=start_ymd,
            end_ymd=end_ymd,
            end_excl_ymd=end_excl,
            mode=mode,
            error="calendar_insufficient_dates",
        )

    _write_cache(cpath, res.__dict__)
    return res
