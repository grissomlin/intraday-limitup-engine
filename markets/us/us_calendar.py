# markets/us/us_calendar.py
# -*- coding: utf-8 -*-
"""Trading-days window helpers (split from downloader_us.py)."""

from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd
import yfinance as yf

def _latest_n_trading_days_window(
    n: int = 30,
    proxy_symbol: str = "SPY",
    lookback_cal_days: int = 180,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    回傳 (start_inclusive_ymd, end_inclusive_ymd, end_exclusive_ymd)
    - start_inclusive_ymd：最後 N 個交易日的第一天
    - end_inclusive_ymd：最後一個交易日
    - end_exclusive_ymd：end_inclusive + 1 day（給 yfinance end=exclusive）
    """
    try:
        end_dt = pd.Timestamp.today().normalize()
        start_dt = end_dt - pd.Timedelta(days=int(lookback_cal_days))

        df = yf.download(
            proxy_symbol,
            start=start_dt.strftime("%Y-%m-%d"),
            end=(end_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
            threads=False,
            timeout=25,
        )
        if df is None or df.empty:
            return None, None, None

        dates = pd.to_datetime(df.index).tz_localize(None).normalize()
        dates = dates.sort_values().unique()
        if len(dates) < max(5, n):
            return None, None, None

        end_incl = dates[-1]
        start_incl = dates[-n]
        end_excl = end_incl + pd.Timedelta(days=1)

        return (
            start_incl.strftime("%Y-%m-%d"),
            end_incl.strftime("%Y-%m-%d"),
            end_excl.strftime("%Y-%m-%d"),
        )
    except Exception:
        return None, None, None

