# markets/india/india_download.py
# -*- coding: utf-8 -*-
"""
INDIA download helpers (Yahoo Finance)

Required by markets/india/downloader.py (import shim tries this first).

Exports:
- download_batch(batch, start_date, end_excl_date) -> (df_long, failed_symbols, err_msg)
- download_one_india(symbol, start_date, end_excl_date) -> (df_long_one, err_msg)
- insert_prices(conn, df_long) -> None
- bulk_insert_errors(conn, err_rows) -> None
"""

from __future__ import annotations

import sqlite3
from typing import List, Optional, Tuple

import pandas as pd

from .india_config import _yf_threads_enabled, log


# -----------------------------------------------------------------------------
# yfinance fetch
# -----------------------------------------------------------------------------
def _fetch_yf_one(symbol: str, start_date: str, end_excl_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Fetch daily OHLCV for one ticker from yfinance.

    Returns:
      (df, err)
    df columns: date, open, high, low, close, volume
    """
    try:
        import yfinance as yf  # type: ignore
    except Exception as e:
        return None, f"yfinance_import_error: {e}"

    try:
        df = yf.download(
            symbol,
            start=start_date,
            end=end_excl_date,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,  # we manage threading ourselves
            group_by="column",
        )
    except Exception as e:
        return None, f"yfinance_download_error: {e}"

    if df is None or df.empty:
        return None, "empty"

    df = df.copy()

    # yfinance sometimes returns MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # Ensure required columns exist
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c not in df.columns:
            df[c] = pd.NA

    df = df.reset_index()

    # Index column name can be "Date" or something else
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    else:
        df = df.rename(columns={df.columns[0]: "date"})

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    out = df[["date", "Open", "High", "Low", "Close", "Volume"]].copy()
    out = out.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    # Coerce numeric
    for c in ["open", "high", "low", "close", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out[out["close"].notna()].copy()
    if out.empty:
        return None, "no_close"

    return out, None


def download_one_india(symbol: str, start_date: str, end_excl_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Return long-form df with columns:
      symbol, date, open, high, low, close, volume
    """
    df, err = _fetch_yf_one(symbol, start_date, end_excl_date)
    if df is None or df.empty:
        return None, err

    df = df.copy()
    df.insert(0, "symbol", str(symbol))
    return df, None


def download_batch(
    symbols: List[str],
    start_date: str,
    end_excl_date: str,
) -> Tuple[Optional[pd.DataFrame], List[str], Optional[str]]:
    """
    Download a batch of symbols.

    Returns:
      (df_long, failed_symbols, err_msg)

    - err_msg is batch-level fatal error (e.g. yfinance not installed)
    - failed_symbols are per-symbol failures
    """
    if not symbols:
        return None, [], None

    # dependency check
    try:
        import yfinance as _  # noqa: F401  # type: ignore
    except Exception as e:
        return None, list(symbols), f"yfinance_import_error: {e}"

    failed: List[str] = []
    frames: List[pd.DataFrame] = []

    use_threads = bool(_yf_threads_enabled()) and len(symbols) >= 8

    if use_threads:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        max_workers = min(16, max(4, len(symbols) // 10))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(download_one_india, sym, start_date, end_excl_date): sym for sym in symbols}
            for fut in as_completed(futs):
                sym = futs[fut]
                try:
                    df_one, err = fut.result()
                except Exception as e:
                    df_one, err = None, f"exception: {e}"
                if df_one is None or df_one.empty:
                    failed.append(sym)
                else:
                    frames.append(df_one)
    else:
        for sym in symbols:
            df_one, err = download_one_india(sym, start_date, end_excl_date)
            if df_one is None or df_one.empty:
                failed.append(sym)
            else:
                frames.append(df_one)

    if not frames:
        return None, failed, None

    df_long = pd.concat(frames, ignore_index=True)
    return df_long, failed, None


# -----------------------------------------------------------------------------
# DB writers
# -----------------------------------------------------------------------------
def insert_prices(conn: sqlite3.Connection, df_long: pd.DataFrame) -> None:
    """
    Insert long-form OHLCV into stock_prices.

    Expected columns:
      symbol, date, open, high, low, close, volume
    """
    if df_long is None or df_long.empty:
        return

    need = ["symbol", "date", "open", "high", "low", "close", "volume"]
    for c in need:
        if c not in df_long.columns:
            raise ValueError(f"insert_prices: missing column {c}")

    df = df_long[need].copy()
    df["symbol"] = df["symbol"].astype(str)
    df["date"] = df["date"].astype(str)

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["volume"] = df["volume"].fillna(0)

    rows = [tuple(x) for x in df.itertuples(index=False, name=None)]
    conn.executemany(
        """
        INSERT OR REPLACE INTO stock_prices
        (symbol, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def bulk_insert_errors(conn: sqlite3.Connection, err_rows: List[Tuple[str, str, str, str, str, str]]) -> None:
    """
    err_rows tuple:
      (symbol, name, start_date, end_date, error, created_at)
    """
    if not err_rows:
        return
    conn.executemany(
        """
        INSERT INTO download_errors
        (symbol, name, start_date, end_date, error, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        err_rows,
    )
