# markets/fr/fr_snapshot.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
from tqdm import tqdm

from .fr_calendar import infer_window_by_trading_days, latest_trading_day_from_calendar
from .fr_config import (
    batch_size,
    batch_sleep_sec,
    calendar_ticker,
    calendar_lookback_cal_days,
    db_path,
    fallback_rolling_cal_days,
    fallback_single_enabled,
    log,
    rolling_trading_days,
    single_sleep_sec,
    yf_threads_enabled,
)
from .fr_db import init_db, insert_prices
from .fr_download import download_batch, download_one
from .fr_master import refresh_stock_info_from_master
from .fr_intraday import run_intraday as _run_intraday


def _write_download_errors(conn: sqlite3.Connection, final_failed: Dict[str, str], name_map: Dict[str, str], start_date: str, end_date_inclusive: str) -> None:
    if not final_failed:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [(sym, name_map.get(sym, "Unknown"), start_date, end_date_inclusive, err, now) for sym, err in final_failed.items()]
    conn.executemany(
        "INSERT INTO download_errors (symbol, name, start_date, end_date, error, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,  # end_exclusive
    refresh_list: bool = True,
) -> Dict[str, Any]:
    dbp = db_path()
    init_db(dbp)

    # ---------- decide window ----------
    if end_date:
        end_excl_candidate = pd.to_datetime(end_date).strftime("%Y-%m-%d")
        end_inclusive = latest_trading_day_from_calendar(
            asof_ymd=(pd.to_datetime(end_excl_candidate) - timedelta(days=1)).strftime("%Y-%m-%d")
        ) or (pd.to_datetime(end_excl_candidate) - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        end_inclusive = latest_trading_day_from_calendar() or datetime.now().strftime("%Y-%m-%d")

    n_days = rolling_trading_days()
    start_td, end_td_incl, end_td_excl = infer_window_by_trading_days(end_inclusive, n_days)

    if start_td and end_td_incl and end_td_excl:
        start_ymd = start_td
        end_inclusive = end_td_incl
        end_excl_date = end_td_excl
        window_mode = "trading_days"
        log(f"📅 Trading-day window OK | last {n_days} trading days | {start_ymd} ~ {end_inclusive} (end_excl={end_excl_date})")
    else:
        window_mode = "cal_days"
        if not start_date:
            start_ymd = (datetime.now() - timedelta(days=fallback_rolling_cal_days())).strftime("%Y-%m-%d")
        else:
            start_ymd = str(start_date)[:10]
        end_excl_date = (pd.to_datetime(end_inclusive) + timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"⚠️ Trading-day window unavailable; fallback cal-days | {start_ymd} ~ {end_inclusive} (end_excl={end_excl_date})")

    # ---------- list ----------
    items = refresh_stock_info_from_master(dbp, refresh_list=refresh_list)
    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False, "db_path": dbp}

    tickers = [s for s, _ in items if s]
    name_map = {s: (n or "Unknown") for s, n in items if s}
    total = len(tickers)

    log(f"📦 FR DB = {dbp}")
    log(f"🚀 FR run_sync | window: {start_ymd} ~ {end_inclusive} | refresh_list={refresh_list}")
    log(f"⚙️ batch_size={batch_size()} threads={yf_threads_enabled()} fallback_single={fallback_single_enabled()} total={total}")

    # ---------- rolling window delete ----------
    conn = sqlite3.connect(dbp, timeout=120)
    try:
        conn.execute("DELETE FROM stock_prices WHERE date >= ?", (start_ymd,))
        conn.commit()
    finally:
        conn.close()

    # ---------- batch download ----------
    batches = [tickers[i : i + batch_size()] for i in range(0, len(tickers), batch_size())]
    pbar = tqdm(batches, desc="FR批次同步", unit="batch")

    ok_set: set[str] = set()
    final_failed: Dict[str, str] = {}

    conn = sqlite3.connect(dbp, timeout=120)
    try:
        for batch in pbar:
            df_long, failed_batch, err_msg = download_batch(batch, start_ymd, end_excl_date)

            if err_msg:
                for sym in batch:
                    final_failed[sym] = err_msg
                time.sleep(batch_sleep_sec())
                continue

            if df_long is not None and not df_long.empty:
                insert_prices(conn, df_long)
                conn.commit()

            failed_batch_set = set(failed_batch or [])
            for sym in batch:
                if sym in failed_batch_set:
                    final_failed[sym] = "batch_missing_or_no_close"
                else:
                    ok_set.add(sym)
                    final_failed.pop(sym, None)

            if fallback_single_enabled():
                need_fallback = [s for s in batch if s in final_failed]
                for sym in need_fallback:
                    df_one, err_one = download_one(sym, start_ymd, end_excl_date)
                    if df_one is not None and not df_one.empty:
                        insert_prices(conn, df_one)
                        conn.commit()
                        ok_set.add(sym)
                        final_failed.pop(sym, None)
                    else:
                        if err_one:
                            final_failed[sym] = err_one
                    time.sleep(single_sleep_sec())

            time.sleep(batch_sleep_sec())

        _write_download_errors(conn, final_failed, name_map, start_ymd, end_inclusive)
        conn.commit()

        try:
            maxd = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()[0]
            log(f"🔎 stock_prices MAX(date) = {maxd} (window end={end_inclusive})")
        except Exception:
            pass

        log("🧹 VACUUM...")
        conn.execute("VACUUM")
        conn.commit()
    finally:
        conn.close()

    success = len(ok_set)
    failed = len(final_failed)
    log(f"📊 FR 同步完成 | 成功:{success} 失敗:{failed} / {total}")

    return {
        "success": int(success),
        "total": int(total),
        "failed": int(failed),
        "has_changed": success > 0,
        "db_path": dbp,
        "window": {"start": start_ymd, "end": end_inclusive, "end_excl": end_excl_date, "mode": window_mode},
        "calendar": {"ticker": calendar_ticker(), "n_trading_days": int(n_days), "lookback_cal_days": int(calendar_lookback_cal_days())},
        "batch": {"size": int(batch_size()), "threads": bool(yf_threads_enabled()), "fallback_single": bool(fallback_single_enabled())},
    }


def run_intraday(slot: str, asof: str, ymd: str, db_path_override=None) -> Dict[str, Any]:
    return _run_intraday(slot=slot, asof=asof, ymd=ymd, db_path_override=db_path_override)
