# markets/uk/uk_prices.py
# -*- coding: utf-8 -*-
"""
UK rolling-window price sync (DB-based) — Batch + Clean Stats
(Adapted from markets/us/us_prices.py)

對外 API：
✅ run_sync(start_date=None, end_date=None, refresh_list=True)

下載策略：
✅ batch：yf.download("VOD.L HSBA.L ...", group_by="ticker")
✅ 單檔 fallback：只救最終失敗者（可關）

環境變數（UK 版）：
- UK_DB_PATH
- UK_ROLLING_TRADING_DAYS        (default 30)
- UK_CALENDAR_TICKER             (default ^FTSE)
- UK_CAL_LOOKBACK_CAL_DAYS       (default 180)
- UK_ROLLING_CAL_DAYS            (default 90)   # calendar 失敗才用
- UK_DAILY_BATCH_SIZE            (default 200)
- UK_BATCH_SLEEP_SEC             (default 0.05)
- UK_FALLBACK_SINGLE             (default 1)
- UK_YF_THREADS                  (default 1)
- UK_SLEEP_SEC                   (default 0.02)  # 單檔 fallback sleep
"""

from __future__ import annotations

import os
import time
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Optional imports (repo 已拆模組；若不存在就走內建 fallback)
# -----------------------------------------------------------------------------
try:
    from .uk_config import log  # type: ignore
except Exception:

    def log(msg: str) -> None:
        print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


try:
    from .uk_calendar import (  # type: ignore
        latest_trading_day_from_calendar as _latest_td_ext,
        infer_window_by_trading_days as _infer_window_ext,
    )
except Exception:
    _latest_td_ext = None
    _infer_window_ext = None

try:
    from .uk_list import get_uk_stock_list  # type: ignore
except Exception:
    get_uk_stock_list = None  # type: ignore


# =============================================================================
# Config helpers
# =============================================================================
def _db_path() -> str:
    return os.getenv("UK_DB_PATH", os.path.join(os.path.dirname(__file__), "uk_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("UK_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    # FTSE 100 index
    return os.getenv("UK_CALENDAR_TICKER", "^FTSE")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("UK_CAL_LOOKBACK_CAL_DAYS", "180"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("UK_ROLLING_CAL_DAYS", "90"))


def _batch_size() -> int:
    return int(os.getenv("UK_DAILY_BATCH_SIZE", "200"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("UK_BATCH_SLEEP_SEC", "0.05"))


def _fallback_single_enabled() -> bool:
    return str(os.getenv("UK_FALLBACK_SINGLE", "1")).strip() == "1"


def _yf_threads_enabled() -> bool:
    return str(os.getenv("UK_YF_THREADS", "1")).strip() == "1"


def _single_sleep_sec() -> float:
    return float(os.getenv("UK_SLEEP_SEC", "0.02"))


# =============================================================================
# DB schema (safe: create if missing)
# =============================================================================
def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_prices (
                symbol TEXT,
                date   TEXT,
                open   REAL,
                high   REAL,
                low    REAL,
                close  REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_info (
                symbol TEXT PRIMARY KEY,
                name   TEXT,
                sector TEXT,
                market TEXT,
                market_detail TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS download_errors (
                symbol TEXT,
                name   TEXT,
                start_date TEXT,
                end_date   TEXT,
                error TEXT,
                created_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol ON stock_prices(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON stock_prices(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_info_market ON stock_info(market)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_err_symbol ON download_errors(symbol)")
        conn.commit()
    finally:
        conn.close()


# =============================================================================
# Calendar helpers (fallback internal)
# =============================================================================
def _latest_trading_day_from_calendar(asof_ymd: Optional[str] = None) -> Optional[str]:
    if _latest_td_ext is not None:
        try:
            return _latest_td_ext(asof_ymd=asof_ymd)
        except Exception:
            pass

    cal_ticker = _calendar_ticker()
    lookback = _calendar_lookback_cal_days()
    try:
        end_dt = pd.to_datetime(asof_ymd) if asof_ymd else pd.Timestamp.now()
        start_dt = end_dt - timedelta(days=lookback)
        df = yf.download(
            cal_ticker,
            start=start_dt.strftime("%Y-%m-%d"),
            end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
            timeout=30,
            auto_adjust=True,
            threads=False,
        )
        if df is None or df.empty:
            return None
        dates = pd.to_datetime(df.index).tz_localize(None).normalize()
        dates = dates.sort_values().unique()
        if asof_ymd:
            cutoff = pd.to_datetime(asof_ymd).normalize()
            dates = [d for d in dates if d <= cutoff]
            if not dates:
                return None
        return dates[-1].strftime("%Y-%m-%d")
    except Exception:
        return None


def _infer_window_by_trading_days(end_ymd: str, n_trading_days: int) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if _infer_window_ext is not None:
        try:
            return _infer_window_ext(end_ymd=end_ymd, n_trading_days=n_trading_days)
        except Exception:
            pass

    cal_ticker = _calendar_ticker()
    lookback = _calendar_lookback_cal_days()
    try:
        end_dt = pd.to_datetime(end_ymd)
        start_dt = end_dt - timedelta(days=lookback)
        df = yf.download(
            cal_ticker,
            start=start_dt.strftime("%Y-%m-%d"),
            end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
            timeout=30,
            auto_adjust=True,
            threads=False,
        )
        if df is None or df.empty:
            return None, None, None

        dates = pd.to_datetime(df.index).tz_localize(None).normalize()
        dates = dates.sort_values().unique()
        dates = [d for d in dates if d <= end_dt.normalize()]

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


# =============================================================================
# List helpers
# =============================================================================
def _get_uk_items(db_path: str, refresh_list: bool) -> List[Tuple[str, str]]:
    """
    回傳 [(symbol, name), ...]
    盡量用 repo 的 uk_list.get_uk_stock_list；若失敗就用 DB stock_info 退路。
    """
    items: List[Tuple[str, str]] = []

    if get_uk_stock_list is not None:
        try:
            raw = get_uk_stock_list(db_path=Path(db_path), refresh_list=refresh_list)
            # 兼容：[(sym,name)] 或 [{"symbol":...}]
            if isinstance(raw, list) and raw:
                if isinstance(raw[0], dict):
                    for d in raw:
                        sym = str(d.get("symbol", "")).strip()
                        if sym:
                            items.append((sym, str(d.get("name", "") or "Unknown")))
                elif isinstance(raw[0], (tuple, list)):
                    for t in raw:
                        if not t:
                            continue
                        sym = str(t[0]).strip()
                        name = str(t[1]).strip() if len(t) >= 2 else "Unknown"
                        if sym:
                            items.append((sym, name or "Unknown"))
        except Exception as e:
            log(f"⚠️ uk_list.get_uk_stock_list failed (fallback DB): {e}")

    if items:
        return items

    # DB fallback
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute("SELECT symbol, name FROM stock_info WHERE market='UK'").fetchall()
            for s, n in rows:
                if s:
                    items.append((str(s), str(n or "Unknown")))
        finally:
            conn.close()

    return items


# =============================================================================
# Download core (batch + single fallback)
# =============================================================================
def _download_one(symbol: str, start_date: str, end_date_exclusive: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    max_retries = 2
    last_err: Optional[str] = None
    for attempt in range(max_retries + 1):
        try:
            df = yf.download(
                symbol,
                start=start_date,
                end=end_date_exclusive,
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
            if "date" not in tmp.columns and "index" in tmp.columns:
                tmp["date"] = tmp["index"]
            if "date" not in tmp.columns:
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


def _download_batch(
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
            threads=_yf_threads_enabled(),
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

    # single ticker (non-multiindex)
    if not isinstance(df.columns, pd.MultiIndex):
        tmp = df.reset_index()
        tmp.columns = [str(c).lower() for c in tmp.columns]
        if "date" not in tmp.columns and "index" in tmp.columns:
            tmp["date"] = tmp["index"]
        if "date" not in tmp.columns:
            return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"]), tickers, "no_date_col"

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
        # MultiIndex layout could be ('Open','VOD.L') or ('VOD.L','Open')
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
                if "date" not in tmp.columns and "index" in tmp.columns:
                    tmp["date"] = tmp["index"]
                if "date" not in tmp.columns:
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


def _insert_prices(conn: sqlite3.Connection, df_long: pd.DataFrame) -> None:
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


def _write_download_errors(
    conn: sqlite3.Connection,
    final_failed: Dict[str, str],
    name_map: Dict[str, str],
    start_date: str,
    end_date_inclusive: str,
) -> None:
    """只寫最終仍失敗的 ticker（乾淨、不重複）"""
    if not final_failed:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        (sym, name_map.get(sym, "Unknown"), start_date, end_date_inclusive, err, now)
        for sym, err in final_failed.items()
    ]
    conn.executemany(
        "INSERT INTO download_errors (symbol, name, start_date, end_date, error, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


# =============================================================================
# Public API: run_sync
# =============================================================================
def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,  # end_exclusive in caller contract
    refresh_list: bool = True,
) -> Dict[str, Any]:
    """
    UK rolling-window sync:
    - end_date 若未給：用 calendar ticker 最新交易日（資料源）當 end_inclusive，再推 end_excl
    - window 預設：最新 N 個交易日
    - 不增量：先刪掉 window 起點之後的舊 price，再重寫入
    """
    db_path = _db_path()
    init_db(db_path)

    # ---------- decide window ----------
    end_inclusive: str

    if end_date:
        end_excl_candidate = pd.to_datetime(end_date).strftime("%Y-%m-%d")
        end_inclusive = _latest_trading_day_from_calendar(
            asof_ymd=(pd.to_datetime(end_excl_candidate) - timedelta(days=1)).strftime("%Y-%m-%d")
        ) or (pd.to_datetime(end_excl_candidate) - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        end_inclusive = _latest_trading_day_from_calendar() or datetime.now().strftime("%Y-%m-%d")

    n_days = _rolling_trading_days()
    start_td, end_td_incl, end_td_excl = _infer_window_by_trading_days(end_inclusive, n_days)

    if start_td and end_td_incl and end_td_excl:
        start_ymd = start_td
        end_inclusive = end_td_incl
        end_excl_date = end_td_excl
        window_mode = "trading_days"
        log(f"📅 Trading-day window OK | last {n_days} trading days | {start_ymd} ~ {end_inclusive} (end_excl={end_excl_date})")
    else:
        window_mode = "cal_days"
        if not start_date:
            start_ymd = (datetime.now() - timedelta(days=_fallback_rolling_cal_days())).strftime("%Y-%m-%d")
        else:
            start_ymd = str(start_date)[:10]
        end_excl_date = (pd.to_datetime(end_inclusive) + timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"⚠️ Trading-day window unavailable; fallback cal-days | {start_ymd} ~ {end_inclusive} (end_excl={end_excl_date})")

    # ---------- list ----------
    items = _get_uk_items(db_path, refresh_list=refresh_list)
    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False, "db_path": db_path}

    tickers = [s for s, _ in items if s]
    name_map = {s: (n or "Unknown") for s, n in items if s}
    total = len(tickers)

    log(f"📦 UK DB = {db_path}")
    log(f"🚀 UK run_sync | window: {start_ymd} ~ {end_inclusive} | refresh_list={refresh_list}")
    log(f"⚙️ batch_size={_batch_size()} threads={_yf_threads_enabled()} fallback_single={_fallback_single_enabled()} total={total}")

    # ---------- rolling window delete ----------
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("DELETE FROM stock_prices WHERE date >= ?", (start_ymd,))
        conn.commit()
    finally:
        conn.close()

    # ---------- batch download ----------
    batches = [tickers[i : i + _batch_size()] for i in range(0, len(tickers), _batch_size())]
    pbar = tqdm(batches, desc="UK批次同步", unit="batch")

    ok_set: set[str] = set()
    final_failed: Dict[str, str] = {}

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        for batch in pbar:
            df_long, failed_batch, err_msg = _download_batch(batch, start_ymd, end_excl_date)

            if err_msg:
                for sym in batch:
                    final_failed[sym] = err_msg
                time.sleep(_batch_sleep_sec())
                continue

            if df_long is not None and not df_long.empty:
                _insert_prices(conn, df_long)
                conn.commit()

            failed_batch_set = set(failed_batch or [])
            for sym in batch:
                if sym in failed_batch_set:
                    final_failed[sym] = "batch_missing_or_no_close"
                else:
                    ok_set.add(sym)
                    if sym in final_failed:
                        final_failed.pop(sym, None)

            if _fallback_single_enabled():
                need_fallback = [s for s in batch if s in final_failed]
                for sym in need_fallback:
                    df_one, err_one = _download_one(sym, start_ymd, end_excl_date)
                    if df_one is not None and not df_one.empty:
                        _insert_prices(conn, df_one)
                        conn.commit()
                        ok_set.add(sym)
                        final_failed.pop(sym, None)
                    else:
                        if err_one:
                            final_failed[sym] = err_one
                    time.sleep(_single_sleep_sec())

            time.sleep(_batch_sleep_sec())

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

    log(f"📊 UK 同步完成 | 成功:{success} 失敗:{failed} / {total}")

    return {
        "success": int(success),
        "total": int(total),
        "failed": int(failed),
        "has_changed": success > 0,
        "db_path": db_path,
        "window": {"start": start_ymd, "end": end_inclusive, "end_excl": end_excl_date, "mode": window_mode},
        "calendar": {
            "ticker": _calendar_ticker(),
            "n_trading_days": int(n_days),
            "lookback_cal_days": int(_calendar_lookback_cal_days()),
        },
        "batch": {
            "size": int(_batch_size()),
            "threads": bool(_yf_threads_enabled()),
            "fallback_single": bool(_fallback_single_enabled()),
        },
    }


if __name__ == "__main__":
    run_sync(refresh_list=True)
