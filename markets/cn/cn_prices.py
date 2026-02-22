# markets/cn/cn_prices.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

from .cn_config import sleep_sec


# =============================================================================
# Tunables (env)
# =============================================================================
def batch_size() -> int:
    return int(os.getenv("CN_DAILY_BATCH_SIZE", "200"))


def batch_sleep_sec() -> float:
    return float(os.getenv("CN_BATCH_SLEEP_SEC", "0.05"))


def fallback_single_enabled() -> bool:
    return str(os.getenv("CN_FALLBACK_SINGLE", "1")).strip().lower() in ("1", "true", "yes", "y", "on")


def yf_threads_enabled() -> bool:
    return str(os.getenv("CN_YF_THREADS", "1")).strip().lower() in ("1", "true", "yes", "y", "on")


def single_sleep_sec() -> float:
    return float(os.getenv("CN_SLEEP_SEC", str(sleep_sec())))


# =============================================================================
# Low-level download (single)
# =============================================================================
def download_one(symbol: str, start_date: str, end_date_exclusive: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    回傳 (df, err)
    df 欄位：symbol,date,open,high,low,close,volume
    """
    max_retries = 2
    last_err: Optional[str] = None

    for attempt in range(max_retries + 1):
        try:
            df = yf.download(
                symbol,
                start=start_date,
                end=end_date_exclusive,
                interval="1d",
                progress=False,
                timeout=30,
                auto_adjust=True,
                threads=False,
            )
            if df is None or df.empty:
                last_err = "empty"
                if attempt < max_retries:
                    time.sleep(1.5)
                    continue
                return None, last_err

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            tmp = df.reset_index()
            tmp.columns = [str(c).lower() for c in tmp.columns]

            if "date" not in tmp.columns:
                if "index" in tmp.columns:
                    tmp["date"] = tmp["index"]
                else:
                    return None, "no_date_col"

            tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.tz_localize(None).dt.strftime("%Y-%m-%d")

            for col in ["open", "high", "low", "close", "volume"]:
                if col not in tmp.columns:
                    tmp[col] = None

            if pd.to_numeric(tmp["close"], errors="coerce").notna().sum() == 0:
                return None, "no_close"

            out = tmp[["date", "open", "high", "low", "close", "volume"]].copy()
            out["symbol"] = symbol
            out = out[["symbol", "date", "open", "high", "low", "close", "volume"]]
            return out, None

        except Exception as e:
            last_err = f"exception: {e}"
            if attempt < max_retries:
                time.sleep(2.0)
                continue
            return None, last_err

    return None, last_err or "unknown"


# =============================================================================
# Batch download
# =============================================================================
def download_batch(
    tickers: List[str],
    start_date: str,
    end_date_exclusive: str,
) -> Tuple[pd.DataFrame, List[str], Optional[str]]:
    """
    回傳 (long_df, failed_tickers, err_msg)
    long_df 欄位：symbol,date,open,high,low,close,volume
    """
    if not tickers:
        empty = pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        return empty, [], None

    try:
        df = yf.download(
            tickers=" ".join(tickers),
            start=start_date,
            end=end_date_exclusive,
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            threads=yf_threads_enabled(),
            progress=False,
            timeout=60,
        )
    except Exception as e:
        empty = pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        return empty, tickers, f"yf.download exception: {e}"

    if df is None or df.empty:
        empty = pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        return empty, tickers, "yf.download empty"

    rows: List[Dict[str, Any]] = []
    failed: List[str] = []

    # single ticker returned non-multiindex
    if not isinstance(df.columns, pd.MultiIndex):
        tmp = df.reset_index()
        tmp.columns = [str(c).lower() for c in tmp.columns]
        if "date" not in tmp.columns:
            if "index" in tmp.columns:
                tmp["date"] = tmp["index"]
            else:
                return (
                    pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"]),
                    tickers,
                    "no_date_col",
                )

        tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.tz_localize(None).dt.strftime("%Y-%m-%d")
        sym = tickers[0]

        if "close" not in tmp.columns or pd.to_numeric(tmp["close"], errors="coerce").notna().sum() == 0:
            failed.append(sym)
        else:
            for _, r in tmp.iterrows():
                rows.append(
                    {
                        "symbol": sym,
                        "date": r.get("date"),
                        "open": r.get("open"),
                        "high": r.get("high"),
                        "low": r.get("low"),
                        "close": r.get("close"),
                        "volume": r.get("volume"),
                    }
                )
    else:
        # MultiIndex layout could be ('Open','AAPL') or ('AAPL','Open')
        level0 = set([c[0] for c in df.columns])
        level1 = set([c[1] for c in df.columns])
        use_level = 1 if any(s in level1 for s in tickers[: min(3, len(tickers))]) else 0

        for sym in tickers:
            try:
                sub = df.xs(sym, axis=1, level=use_level, drop_level=False)
                if sub is None or sub.empty:
                    failed.append(sym)
                    continue

                if use_level == 1:
                    sub.columns = [c[0] for c in sub.columns]
                else:
                    sub.columns = [c[1] for c in sub.columns]

                tmp = sub.reset_index()
                tmp.columns = [str(c).lower() for c in tmp.columns]
                if "date" not in tmp.columns:
                    if "index" in tmp.columns:
                        tmp["date"] = tmp["index"]
                    else:
                        failed.append(sym)
                        continue

                tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.tz_localize(None).dt.strftime("%Y-%m-%d")

                if "close" not in tmp.columns or pd.to_numeric(tmp["close"], errors="coerce").notna().sum() == 0:
                    failed.append(sym)
                    continue

                for _, r in tmp.iterrows():
                    rows.append(
                        {
                            "symbol": sym,
                            "date": r.get("date"),
                            "open": r.get("open"),
                            "high": r.get("high"),
                            "low": r.get("low"),
                            "close": r.get("close"),
                            "volume": r.get("volume"),
                        }
                    )
            except Exception:
                failed.append(sym)

    out = pd.DataFrame(rows)
    if out.empty:
        empty = pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        return empty, sorted(list(set(failed + tickers))), "batch produced no rows"

    out = out.dropna(subset=["symbol", "date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    return out, sorted(list(set(failed))), None


# =============================================================================
# DB helpers (fast insert / final error list)
# =============================================================================
def insert_prices(conn, df_long: pd.DataFrame) -> None:
    if df_long is None or df_long.empty:
        return

    dfw = df_long.copy()
    dfw["volume"] = pd.to_numeric(dfw["volume"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        dfw[col] = pd.to_numeric(dfw[col], errors="coerce")

    rows = [
        (
            str(r.symbol),
            str(r.date)[:10],
            None if pd.isna(r.open) else float(r.open),
            None if pd.isna(r.high) else float(r.high),
            None if pd.isna(r.low) else float(r.low),
            None if pd.isna(r.close) else float(r.close),
            None if pd.isna(r.volume) else int(r.volume),
        )
        for r in dfw.itertuples(index=False)
    ]

    conn.executemany(
        "INSERT OR REPLACE INTO stock_prices (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def record_error(conn, sym: str, name: str, start_date: str, end_date: str, err: str) -> None:
    try:
        conn.execute(
            """
            INSERT INTO download_errors
            (symbol, name, start_date, end_date, error, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (sym, name, start_date, end_date, err, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
    except Exception:
        pass


def write_final_errors(conn, final_failed: Dict[str, str], name_map: Dict[str, str], start_date: str, end_date: str) -> None:
    """只寫最終仍失敗的 ticker（乾淨、不重複）"""
    if not final_failed:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        (sym, name_map.get(sym, "Unknown"), start_date, end_date, err, now) for sym, err in final_failed.items()
    ]
    conn.executemany(
        "INSERT INTO download_errors (symbol, name, start_date, end_date, error, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


# =============================================================================
# Backward-compatible sleep helper
# =============================================================================
def sleep_between() -> None:
    time.sleep(sleep_sec())
