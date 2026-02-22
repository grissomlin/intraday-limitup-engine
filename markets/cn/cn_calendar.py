# markets/cn/cn_calendar.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import timedelta
from typing import Optional, Tuple

import pandas as pd
import yfinance as yf

from .cn_config import calendar_ticker, calendar_lookback_cal_days

def infer_window_by_trading_days(
    end_ymd: str,
    n_trading_days: int,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    用 yfinance proxy ticker 當交易日曆來源，推算最近 N 個交易日窗口。
    回傳 (start_ymd, end_ymd_inclusive, end_exclusive_ymd)
    """
    cal_ticker = calendar_ticker()
    lookback = calendar_lookback_cal_days()

    try:
        end_dt = pd.to_datetime(end_ymd).normalize()
        start_dt = end_dt - timedelta(days=lookback)

        df_cal = yf.download(
            cal_ticker,
            start=start_dt.strftime("%Y-%m-%d"),
            end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
            timeout=30,
            auto_adjust=True,
            threads=False,
        )
        if df_cal is None or df_cal.empty:
            return None, None, None

        dates = pd.to_datetime(df_cal.index).tz_localize(None).normalize()
        dates = dates.sort_values().unique()
        dates = [d for d in dates if d <= end_dt]

        if len(dates) < max(5, n_trading_days):
            return None, None, None

        end_incl = dates[-1]
        start_incl = dates[-n_trading_days]
        end_excl = end_incl + timedelta(days=1)

        return (
            start_incl.strftime("%Y-%m-%d"),
            end_incl.strftime("%Y-%m-%d"),
            end_excl.strftime("%Y-%m-%d"),
        )
    except Exception:
        return None, None, None
