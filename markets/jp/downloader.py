# markets/jp/downloader.py
# -*- coding: utf-8 -*-
"""
JP pipeline (rolling window, DB-based snapshot builder)

- run_sync(): refresh JPX list, download prices (rolling window), write DB
- run_intraday(): build raw payload from DB

Enhancements:
- Exclude TOKYO PRO Market by default (env JP_INCLUDE_TOKYO_PRO=1 to include)
- Calendar trading-days window is cached (shared markets/_calendar_cache.py)

Fixes:
- DO NOT write "empty rows" into DB (close NaN/None or OHLC all NaN)
- When picking ymd_effective, ignore rows with close IS NULL
- When building snapshot_main, exclude close IS NULL

Added (2026-02):
- ✅ meta.time standardized (Asia/Tokyo) for 2-line subtitle:
    line1 = trading day (ymd_effective)
    line2 = updated date+time + UTC offset (renderer uses meta.time)
"""

from __future__ import annotations

import io
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
from tqdm import tqdm

from markets._calendar_cache import _get_trading_window_cached

# ✅ unified meta.time builder
from markets.common.time_builders import build_meta_time_asia


# =============================================================================
# Env
# =============================================================================
def _db_path() -> str:
    return os.getenv("JP_DB_PATH", os.path.join(os.path.dirname(__file__), "jp_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("JP_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    return os.getenv("JP_CALENDAR_TICKER", "^N225")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("JP_CAL_LOOKBACK_CAL_DAYS", "180"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("JP_ROLLING_CAL_DAYS", "90"))


def _list_url() -> str:
    return os.getenv(
        "JP_LIST_URL",
        "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls",
    )


def _batch_size() -> int:
    return int(os.getenv("JP_DAILY_BATCH_SIZE", "200"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("JP_BATCH_SLEEP_SEC", "0.05"))


def _fallback_single_enabled() -> bool:
    return str(os.getenv("JP_FALLBACK_SINGLE", "1")).strip() == "1"


def _yf_threads_enabled() -> bool:
    return str(os.getenv("JP_YF_THREADS", "1")).strip() == "1"


def _single_sleep_sec() -> float:
    return float(os.getenv("JP_SLEEP_SEC", "0.03"))


def _include_tokyo_pro() -> bool:
    return str(os.getenv("JP_INCLUDE_TOKYO_PRO", "0")).strip().lower() in ("1", "true", "yes", "y", "on")


def _cal_cache_root() -> str:
    return os.getenv("CAL_CACHE_ROOT", os.path.join("data", "cache", "calendar"))


def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


# =============================================================================
# DB schema
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
# JPX list
# =============================================================================
def _ensure_excel_tool():
    try:
        import xlrd  # noqa: F401
    except Exception:
        try:
            log("🔧 安裝 xlrd 以支援 JPX .xls ...")
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "xlrd"], check=False)
        except Exception:
            pass


def _is_tokyo_pro_market(product: str) -> bool:
    p = (product or "").strip().lower()
    return ("tokyo pro market" in p) or ("pro market" in p)


def get_jp_stock_list(db_path: str) -> List[Tuple[str, str, str, str]]:
    _ensure_excel_tool()

    url = _list_url()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html",
    }

    log(f"📡 正在從 JPX 官網同步最新股票名單... ({url})")
    log(f"🧩 include TOKYO PRO Market = {_include_tokyo_pro()} (JP_INCLUDE_TOKYO_PRO)")

    try:
        r = requests.get(url, headers=headers, timeout=45)
        r.raise_for_status()
        df = pd.read_excel(io.BytesIO(r.content))
    except Exception as e:
        log(f"❌ JPX 名單下載失敗: {e}")
        return []

    C_CODE = "Local Code"
    C_NAME = "Name (English)"
    C_PROD = "Section/Products"
    C_SECTOR = "33 Sector(name)"

    conn = sqlite3.connect(db_path)
    items: List[Tuple[str, str, str, str]] = []
    try:
        for _, row in df.iterrows():
            raw_code = row.get(C_CODE)
            if pd.isna(raw_code):
                continue

            code = str(raw_code).split(".")[0].strip()
            if not (len(code) == 4 and code.isdigit()):
                continue

            product = str(row.get(C_PROD, "")).strip()

            if product.lower().startswith("etfs"):
                continue
            if (not _include_tokyo_pro()) and _is_tokyo_pro_market(product):
                continue

            symbol = f"{code}.T"
            name = str(row.get(C_NAME, "")).strip() or "Unknown"
            sector = str(row.get(C_SECTOR, "")).strip() or "未分類"
            market = "JPX"
            market_detail = product or "unknown"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, name, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (symbol, name, sector, market, market_detail, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            items.append((symbol, name, sector, market_detail))
        conn.commit()
    finally:
        conn.close()

    log(f"✅ 日股名單同步完成：共 {len(items)} 檔")
    return items


def get_jp_stock_list_from_db(db_path: str) -> List[Tuple[str, str, str, str]]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT symbol, name, sector, market_detail FROM stock_info").fetchall()
        out = []
        for s, n, sec, md in rows:
            if not s:
                continue
            md_s = (md or "unknown")
            if (not _include_tokyo_pro()) and _is_tokyo_pro_market(str(md_s)):
                continue
            out.append((s, n or "Unknown", sec or "未分類", md_s))
        return out
    finally:
        conn.close()


# =============================================================================
# Download helpers
# =============================================================================
def download_one_jp(symbol: str, start_date: str, end_date_exclusive: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    max_retries = 2
    last_err: Optional[str] = None

    for attempt in range(max_retries + 1):
        try:
            df = yf.download(
                symbol,
                start=start_date,
                end=end_date_exclusive,
                progress=False,
                auto_adjust=True,
                threads=False,
                timeout=30,
            )

            if df is None or df.empty:
                last_err = "empty"
                if attempt < max_retries:
                    time.sleep(2.0)
                    continue
                return None, last_err

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            if "date" not in df.columns:
                if "index" in df.columns:
                    df["date"] = df["index"]
                else:
                    return None, "no_date_col"

            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
            for col in ["open", "high", "low", "close", "volume"]:
                if col not in df.columns:
                    df[col] = None

            out = df[["date", "open", "high", "low", "close", "volume"]].copy()
            out["symbol"] = symbol
            return out[["symbol", "date", "open", "high", "low", "close", "volume"]], None

        except Exception as e:
            last_err = f"exception: {e}"
            if attempt < max_retries:
                time.sleep(3.0)
                continue
            return None, last_err

    return None, last_err or "unknown"


def _download_batch(
    tickers: List[str],
    start_date: str,
    end_date_exclusive: str,
) -> Tuple[pd.DataFrame, List[str], Optional[str]]:
    if not tickers:
        empty = pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        return empty, [], None

    tickers_str = " ".join(tickers)
    try:
        df = yf.download(
            tickers=tickers_str,
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

    if not isinstance(df.columns, pd.MultiIndex):
        tmp = df.copy().reset_index()
        tmp.columns = [str(c).lower() for c in tmp.columns]
        if "date" not in tmp.columns and "index" in tmp.columns:
            tmp["date"] = tmp["index"]
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

                tmp = sub.copy().reset_index()
                tmp.columns = [str(c).lower() for c in tmp.columns]
                if "date" not in tmp.columns and "index" in tmp.columns:
                    tmp["date"] = tmp["index"]
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
    """
    ✅ FIX: do NOT write empty rows.
    - drop rows where close is NaN
    - drop rows where open/high/low/close are all NaN
    """
    if df_long is None or df_long.empty:
        return

    dfw = df_long.copy()
    dfw["volume"] = pd.to_numeric(dfw["volume"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        dfw[col] = pd.to_numeric(dfw[col], errors="coerce")

    # ---- critical filters ----
    dfw = dfw.dropna(subset=["symbol", "date"])
    # yfinance sometimes returns date index but OHLCV all NaN -> DO NOT store
    dfw = dfw[dfw["close"].notna()]
    dfw = dfw.dropna(subset=["open", "high", "low", "close"], how="all")

    if dfw.empty:
        return

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


def _bulk_insert_errors(conn: sqlite3.Connection, rows: List[Tuple[str, str, str, str, str, str]]) -> None:
    if not rows:
        return
    conn.executemany(
        "INSERT INTO download_errors (symbol, name, start_date, end_date, error, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


# =============================================================================
# Public API: run_sync
# =============================================================================
def run_sync(start_date: Optional[str] = None, end_date: Optional[str] = None, *, refresh_list: bool = True) -> Dict[str, Any]:
    db_path = _db_path()
    init_db(db_path)

    asof_ymd = (end_date or datetime.now().strftime("%Y-%m-%d")).strip()
    n_days = _rolling_trading_days()

    cal = _get_trading_window_cached(
        market="jp",
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
        log(f"⚠️ Trading-day window unavailable (cached/fallback); cal-days | {start_date} ~ {end_date} (end_excl={end_excl_date})")

    log(f"📦 JP DB = {db_path}")
    log(f"🚀 JP run_sync | window: {start_date} ~ {end_date} | refresh_list={refresh_list}")
    log(f"🗓️ calendar: ticker={_calendar_ticker()} mode={window_mode} cache={cal.get('cache_path')} err={cal.get('error')}")
    log(f"⚙️ batch_size={_batch_size()} threads={_yf_threads_enabled()} fallback_single={_fallback_single_enabled()}")

    # list
    if refresh_list:
        items = get_jp_stock_list(db_path)
    else:
        items = get_jp_stock_list_from_db(db_path)
        if not items:
            items = get_jp_stock_list(db_path)

    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False}

    tickers = [s for s, _, _, _ in items]
    name_map = {s: (n or "Unknown") for s, n, _, _ in items}

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
    pbar = tqdm(batches, desc="JP批次同步", unit="batch")

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        for batch in pbar:
            df_long, failed_batch, err_msg = _download_batch(batch, start_date, end_excl_date)

            if err_msg:
                for sym in batch:
                    status[sym] = "fail"
                    err_final[sym] = f"batch_error: {err_msg}"
                time.sleep(_batch_sleep_sec())
                continue

            if df_long is not None and not df_long.empty:
                _insert_prices(conn, df_long)
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
                fallback_frames: List[pd.DataFrame] = []
                for sym in failed_batch:
                    df_one, err_one = download_one_jp(sym, start_date, end_excl_date)
                    if df_one is not None and not df_one.empty:
                        fallback_frames.append(df_one)
                        status[sym] = "ok"
                        err_final.pop(sym, None)
                    else:
                        status[sym] = "fail"
                        err_final[sym] = f"single_error: {err_one}" if err_one else "single_empty"
                    time.sleep(_single_sleep_sec())

                if fallback_frames:
                    df_fb = pd.concat(fallback_frames, ignore_index=True)
                    _insert_prices(conn, df_fb)
                    conn.commit()

            time.sleep(_batch_sleep_sec())

        try:
            maxd = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()[0]
            log(f"🔎 stock_prices MAX(date) = {maxd} (window end={end_date})")
        except Exception:
            pass

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
            _bulk_insert_errors(conn, err_rows)
            conn.commit()

        log("🧹 VACUUM...")
        conn.execute("VACUUM")
        conn.commit()

        total_in_db = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    finally:
        conn.close()

    success = sum(1 for v in status.values() if v == "ok")
    failed = sum(1 for v in status.values() if v == "fail")

    log(f"📊 JP 同步完成 | 成功:{success} 失敗:{failed}")
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
            "lookback_cal_days": _calendar_lookback_cal_days(),
            "cache_path": cal.get("cache_path"),
            "cache_error": cal.get("error"),
        },
        "batch": {"size": _batch_size(), "threads": _yf_threads_enabled(), "fallback_single": _fallback_single_enabled()},
        "filters": {"include_tokyo_pro_market": _include_tokyo_pro()},
    }


# =============================================================================
# Snapshot builder: run_intraday (DB -> raw_payload)
# =============================================================================
def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    """
    ✅ FIX: ignore empty rows where close is NULL
    """
    row = conn.execute(
        "SELECT MAX(date) FROM stock_prices WHERE date <= ? AND close IS NOT NULL",
        (ymd,),
    ).fetchone()
    return row[0] if row and row[0] else None


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    db_path = _db_path()
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"JP DB not found: {db_path} (set JP_DB_PATH to override)")

    conn = sqlite3.connect(db_path)
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

        sql = """
        WITH p AS (
          SELECT
            symbol,
            date,
            open, high, low, close, volume,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
          FROM stock_prices
        )
        SELECT
          p.symbol,
          p.date AS ymd,
          p.open, p.high, p.low, p.close, p.volume,
          p.last_close,
          i.name,
          i.sector,
          i.market,
          i.market_detail
        FROM p
        LEFT JOIN stock_info i ON i.symbol = p.symbol
        WHERE p.date = ?
          AND p.close IS NOT NULL
        """
        df = pd.read_sql_query(sql, conn, params=(ymd_effective,))

        if df.empty:
            snapshot_main: List[Dict[str, Any]] = []
        else:
            df["name"] = df["name"].fillna("Unknown")
            df["sector"] = df["sector"].fillna("未分類")
            df.loc[df["sector"].isin(["", "—", "-", "--", "－", "–"]), "sector"] = "未分類"

            df["last_close"] = pd.to_numeric(df["last_close"], errors="coerce")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")

            df["ret"] = 0.0
            m = df["last_close"].notna() & (df["last_close"] > 0) & df["close"].notna()
            df.loc[m, "ret"] = (df.loc[m, "close"] / df.loc[m, "last_close"]) - 1.0

            df["streak"] = 1

            snapshot_main = df[
                [
                    "symbol",
                    "name",
                    "sector",
                    "ymd",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "last_close",
                    "ret",
                    "streak",
                    "market",
                    "market_detail",
                ]
            ].to_dict(orient="records")

        # ✅ unified meta.time for renderers (2-line subtitle)
        now_utc = datetime.now(timezone.utc)
        meta_time = build_meta_time_asia(now_utc, tz_name="Asia/Tokyo", fallback_offset="+09:00")

        return {
            "market": "jp",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "snapshot_main": snapshot_main,
            "snapshot_open": [],
            "stats": {"snapshot_main_count": int(len(snapshot_main)), "snapshot_open_count": 0},
            "meta": {
                "db_path": db_path,
                "ymd_effective": ymd_effective,
                "time": meta_time,
            },
        }
    finally:
        conn.close()


if __name__ == "__main__":
    run_sync()