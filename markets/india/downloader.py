# markets/india/downloader.py
# -*- coding: utf-8 -*-
"""
INDIA pipeline (rolling window, DB-based snapshot builder)

- run_sync(): refresh stock list from NSE master csv, download prices, write DB
- run_intraday(): build raw payload from DB
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

from markets._calendar_cache import _get_trading_window_cached

from .india_config import (
    _batch_size,
    _batch_sleep_sec,
    _cal_cache_root,
    _calendar_lookback_cal_days,
    _calendar_ticker,
    _db_path,
    _fallback_rolling_cal_days,
    _fallback_single_enabled,
    _master_csv_path,
    _rolling_trading_days,
    _single_sleep_sec,
    _yf_suffix,
    _yf_threads_enabled,
    log,
)
from .india_db import init_db
from .india_download import bulk_insert_errors, download_batch, download_one_india, insert_prices
from .india_list import get_india_stock_list, get_india_stock_list_from_db
from .india_snapshot import run_intraday


def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    refresh_list: bool = True,
) -> Dict[str, Any]:
    db_path = _db_path()
    init_db(db_path)

    asof_ymd = (end_date or datetime.now().strftime("%Y-%m-%d")).strip()
    n_days = _rolling_trading_days()

    cal = _get_trading_window_cached(
        market="india",
        calendar_ticker=_calendar_ticker(),
        asof_ymd=asof_ymd,
        n_trading_days=n_days,
        lookback_cal_days=_calendar_lookback_cal_days(),
        fallback_rolling_cal_days=_fallback_rolling_cal_days(),
        cache_root=_cal_cache_root(),
    )

    end_date = str(cal.get("latest_ymd") or asof_ymd)

    if cal.get("mode") == "trading_days" and cal.get("start_ymd") and cal.get("end_ymd") and cal.get("end_excl_ymd"):
        start_date = str(cal["start_ymd"])
        end_date = str(cal["end_ymd"])
        end_excl_date = str(cal["end_excl_ymd"])
        window_mode = "trading_days"
        log(f"📅 Trading-day window OK (cached) | last {n_days} trading days | {start_date} ~ {end_date} (end_excl={end_excl_date})")
    else:
        if not start_date:
            start_date = (datetime.now() - timedelta(days=_fallback_rolling_cal_days())).strftime("%Y-%m-%d")
        end_excl_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
        window_mode = "cal_days"
        log(f"⚠️ Trading-day window unavailable; cal-days | {start_date} ~ {end_date} (end_excl={end_excl_date})")

    log(f"📦 INDIA DB = {db_path}")
    log(f"🚀 INDIA run_sync | window: {start_date} ~ {end_date} | refresh_list={refresh_list}")
    log(f"🗓️ calendar: ticker={_calendar_ticker()} mode={window_mode} cache={cal.get('cache_path')} err={cal.get('error')}")
    log(f"⚙️ batch_size={_batch_size()} threads={_yf_threads_enabled()} fallback_single={_fallback_single_enabled()} yf_suffix={_yf_suffix()}")
    log(f"🧩 list: master_csv_path={_master_csv_path()}")

    # list
    if refresh_list:
        items = get_india_stock_list(db_path)
    else:
        items = get_india_stock_list_from_db(db_path)
        if not items:
            items = get_india_stock_list(db_path)

    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False}

    tickers = [yf_sym for (yf_sym, _local, _n, _ind, _sec, _md) in items]
    name_map = {yf_sym: (n or "Unknown") for (yf_sym, _local, n, _ind, _sec, _md) in items}

    # rolling window (no incremental)
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("DELETE FROM stock_prices WHERE date >= ?", (start_date,))
        conn.commit()
    finally:
        conn.close()

    status: Dict[str, str] = {}
    err_final: Dict[str, str] = {}
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    batches = [tickers[i : i + _batch_size()] for i in range(0, len(tickers), _batch_size())]
    pbar = tqdm(batches, desc="INDIA批次同步", unit="batch")

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        for batch in pbar:
            df_long, failed_batch, err_msg = download_batch(batch, start_date, end_excl_date)

            if err_msg:
                for sym in batch:
                    status[sym] = "fail"
                    err_final[sym] = f"batch_error: {err_msg}"
                time.sleep(_batch_sleep_sec())
                continue

            if df_long is not None and not df_long.empty:
                insert_prices(conn, df_long)
                conn.commit()

            failed_set = set(failed_batch)
            ok_set = set(batch) - failed_set

            for sym in ok_set:
                status[sym] = "ok"
                err_final.pop(sym, None)

            for sym in failed_set:
                status[sym] = "fail"
                err_final[sym] = "batch_missing_or_no_close"

            if failed_batch and _fallback_single_enabled():
                fb_frames: List[pd.DataFrame] = []
                for sym in failed_batch:
                    df_one, err_one = download_one_india(sym, start_date, end_excl_date)
                    if df_one is not None and not df_one.empty:
                        fb_frames.append(df_one)
                        status[sym] = "ok"
                        err_final.pop(sym, None)
                    else:
                        status[sym] = "fail"
                        err_final[sym] = f"single_error: {err_one}" if err_one else "single_empty"
                    time.sleep(_single_sleep_sec())

                if fb_frames:
                    df_fb = pd.concat(fb_frames, ignore_index=True)
                    insert_prices(conn, df_fb)
                    conn.commit()

            time.sleep(_batch_sleep_sec())

        err_rows: List[Tuple[str, str, str, str, str, str]] = []
        for sym, st in status.items():
            if st != "fail":
                continue
            err_rows.append(
                (
                    sym,
                    name_map.get(sym, "Unknown"),
                    start_date,
                    end_date,
                    err_final.get(sym, "unknown_error"),
                    created_at,
                )
            )
        if err_rows:
            bulk_insert_errors(conn, err_rows)
            conn.commit()

        conn.execute("VACUUM")
        conn.commit()

        # NOTE: requires india_list upsert to set stock_info.market='INDIA'
        total_in_db = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info WHERE market='INDIA'").fetchone()[0]
    finally:
        conn.close()

    success = sum(1 for v in status.values() if v == "ok")
    failed = sum(1 for v in status.values() if v == "fail")

    log(f"📊 INDIA 同步完成 | 成功:{success} 失敗:{failed}")
    return {
        "success": success,
        "total": int(total_in_db),
        "failed": failed,
        "has_changed": success > 0,
        "db_path": db_path,
        "window": {"start": start_date, "end": end_date, "end_excl": end_excl_date, "mode": window_mode},
        "calendar": {
            "ticker": _calendar_ticker(),
            "n_trading_days": n_days,
            "cache_path": cal.get("cache_path"),
            "cache_error": cal.get("error"),
        },
        "batch": {"size": _batch_size(), "threads": _yf_threads_enabled(), "fallback_single": _fallback_single_enabled()},
        "filters": {"yf_suffix": _yf_suffix(), "master_csv_path": _master_csv_path()},
    }


if __name__ == "__main__":
    run_sync()
