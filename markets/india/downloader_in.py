# markets/india/downloader_in.py
# -*- coding: utf-8 -*-
"""
India pipeline (DB-based rolling window snapshot builder)

Goal:
- run_sync():
    1) refresh universe
    2) upsert stock_info (symbol/name/sector/industry/band/limit_pct...)
    3) download last N trading days daily bars (yfinance 1d) -> stock_prices
- run_intraday():
    1) use DB stock_info (NOT universe df) as the source-of-truth for metadata
    2) use DB stock_prices to compute:
         - ymd_effective (latest trading day <= requested ymd)
         - prev_close per symbol (LAG(close))
         - streak per symbol (locked streak on daily closes, using limit_pct)
    3) download today's intraday (yfinance 1m) and merge into snapshot_main

Key outputs in snapshot_main:
- prev_close (previous trading day's close, from DB via LAG)
- streak (locked streak computed from daily closes in DB)
- band, limit_pct, sector, industry (from DB stock_info)
- intraday open/high/low/close/volume (from yfinance 1m)

Notes:
- sector/industry currently scaffolded by universe.py (often "Unclassified"), but we keep DB fields so later you can enrich and it will flow through.
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from tqdm import tqdm

from markets._calendar_cache import _get_trading_window_cached
from markets.india.universe import load_universe_df


# =============================================================================
# Env knobs
# =============================================================================
def _db_path() -> str:
    return os.getenv("IN_DB_PATH", os.path.join(os.path.dirname(__file__), "in_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("IN_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    # NIFTY 50 index on Yahoo
    return os.getenv("IN_CALENDAR_TICKER", "^NSEI")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("IN_CAL_LOOKBACK_CAL_DAYS", "240"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("IN_ROLLING_CAL_DAYS", "120"))


def _cal_cache_root() -> str:
    return os.getenv("CAL_CACHE_ROOT", os.path.join("data", "cache", "calendar"))


def _daily_batch_size() -> int:
    return int(os.getenv("IN_DAILY_BATCH_SIZE", "120"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("IN_BATCH_SLEEP_SEC", "1.2"))


def _yf_threads_enabled() -> bool:
    return str(os.getenv("IN_YF_THREADS", "1")).strip() == "1"


def _intraday_chunk_size() -> int:
    return int(os.getenv("IN_YF_CHUNK_SIZE", "80"))


def _intraday_chunk_sleep() -> float:
    return float(os.getenv("IN_YF_CHUNK_SLEEP", "2.0"))


def _intraday_interval() -> str:
    return os.getenv("IN_YF_INTRADAY_INTERVAL", "1m").strip() or "1m"


def _intraday_period() -> str:
    # 1d is enough for 1m
    return os.getenv("IN_YF_INTRADAY_PERIOD", "1d").strip() or "1d"


def log(msg: str) -> None:
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
                symbol TEXT PRIMARY KEY,            -- yf_symbol, e.g. RELIANCE.NS
                local_symbol TEXT,                  -- exchange symbol
                name   TEXT,
                sector TEXT,
                industry TEXT,
                band   TEXT,
                limit_pct REAL,
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
# Universe -> stock_info
# =============================================================================
def _is_blankish(x: Any) -> bool:
    s = ("" if x is None else str(x)).strip()
    if not s or s.lower() in ("nan", "none"):
        return True
    if s in ("-", "—", "--", "－", "–"):
        return True
    return False


def _merge_keep_old_if_new_blank(new_val: Any, old_val: Any, default_if_both_blank: str) -> str:
    if not _is_blankish(new_val):
        return str(new_val).strip()
    if not _is_blankish(old_val):
        return str(old_val).strip()
    return default_if_both_blank


def _load_existing_stock_info_map(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    try:
        rows = conn.execute(
            "SELECT symbol, local_symbol, name, sector, industry, band, limit_pct, market, market_detail FROM stock_info"
        ).fetchall()
    except Exception:
        rows = []

    for sym, local_sym, name, sector, industry, band, limit_pct, market, md in rows:
        if not sym:
            continue
        out[str(sym)] = {
            "local_symbol": str(local_sym or ""),
            "name": str(name or ""),
            "sector": str(sector or ""),
            "industry": str(industry or ""),
            "band": str(band or ""),
            "limit_pct": limit_pct,
            "market": str(market or ""),
            "market_detail": str(md or ""),
        }
    return out


def sync_stock_info_from_universe(db_path: str) -> pd.DataFrame:
    """
    Read universe and upsert into stock_info.

    Universe columns expected (from markets/india/universe.py):
      Symbol, yf_symbol, name, sector, industry, band, limit_pct, ...
    """
    df_uni = load_universe_df()
    if df_uni is None or df_uni.empty:
        raise RuntimeError("India universe is empty. Check Drive CSVs or local env paths.")

    # ensure required cols exist
    for c in ["Symbol", "yf_symbol", "name", "sector", "industry", "band", "limit_pct"]:
        if c not in df_uni.columns:
            df_uni[c] = None

    conn = sqlite3.connect(db_path)
    try:
        existing = _load_existing_stock_info_map(conn)
        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for _, r in df_uni.iterrows():
            yf_symbol = ("" if r.get("yf_symbol") is None else str(r.get("yf_symbol"))).strip()
            if not yf_symbol:
                continue

            local_symbol = ("" if r.get("Symbol") is None else str(r.get("Symbol"))).strip()
            name_new = r.get("name")
            sector_new = r.get("sector")
            industry_new = r.get("industry")
            band_new = r.get("band")
            limit_pct_new = r.get("limit_pct")

            old = existing.get(yf_symbol, {})

            name_final = _merge_keep_old_if_new_blank(name_new, old.get("name"), "Unknown")
            sector_final = _merge_keep_old_if_new_blank(sector_new, old.get("sector"), "Unclassified")
            industry_final = _merge_keep_old_if_new_blank(industry_new, old.get("industry"), "Unclassified")
            band_final = _merge_keep_old_if_new_blank(band_new, old.get("band"), "")

            # limit_pct keep old numeric if new is missing
            try:
                limit_pct_final = (
                    float(limit_pct_new)
                    if limit_pct_new is not None and str(limit_pct_new).strip() != ""
                    else None
                )
            except Exception:
                limit_pct_final = None
            if limit_pct_final is None:
                lp_old = old.get("limit_pct")
                if lp_old is not None:
                    try:
                        limit_pct_final = float(lp_old)
                    except Exception:
                        limit_pct_final = None

            local_symbol_final = _merge_keep_old_if_new_blank(local_symbol, old.get("local_symbol"), local_symbol)

            market_detail = "NSE|universe"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, local_symbol, name, sector, industry, band, limit_pct, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    yf_symbol,
                    local_symbol_final,
                    name_final,
                    sector_final,
                    industry_final,
                    band_final,
                    limit_pct_final,
                    "IN",
                    market_detail,
                    now_s,
                ),
            )

            existing[yf_symbol] = {
                "local_symbol": local_symbol_final,
                "name": name_final,
                "sector": sector_final,
                "industry": industry_final,
                "band": band_final,
                "limit_pct": limit_pct_final,
                "market": "IN",
                "market_detail": market_detail,
            }

        conn.commit()
    finally:
        conn.close()

    return df_uni


# =============================================================================
# Download helpers (daily)
# =============================================================================
def _download_daily_batch(
    tickers: List[str], start_date: str, end_date_exclusive: str
) -> Tuple[pd.DataFrame, List[str], Optional[str]]:
    """
    Returns:
      - df_long: columns [symbol,date,open,high,low,close,volume]
      - failed: tickers that appear missing or no close
      - err_msg: batch exception text (if total failure)
    """
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
            threads=_yf_threads_enabled(),
            auto_adjust=True,
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
        # single ticker
        sym = tickers[0]
        tmp = df.copy().reset_index()
        tmp.columns = [str(c).lower() for c in tmp.columns]
        if "date" not in tmp.columns and "index" in tmp.columns:
            tmp["date"] = tmp["index"]
        tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.tz_localize(None).dt.strftime("%Y-%m-%d")

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
        # MultiIndex: (field, ticker) or (ticker, field)
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
    if df_long is None or df_long.empty:
        return

    dfw = df_long.copy()
    dfw["volume"] = pd.to_numeric(dfw["volume"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        dfw[col] = pd.to_numeric(dfw[col], errors="coerce")

    dfw = dfw.dropna(subset=["symbol", "date"])
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
# Public API: run_sync (rolling window)
# =============================================================================
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
        market="in",
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
        log(f"📅 IN Trading-day window OK | last {n_days} trading days | {start_date} ~ {end_date} (end_excl={end_excl_date})")
    else:
        if not start_date:
            start_date = (datetime.now() - timedelta(days=_fallback_rolling_cal_days())).strftime("%Y-%m-%d")
        end_excl_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
        window_mode = "cal_days"
        log(f"⚠️ IN Trading-day window unavailable; fallback cal-days | {start_date} ~ {end_date} (end_excl={end_excl_date})")

    log(f"📦 IN DB = {db_path}")
    log(f"🚀 IN run_sync | window: {start_date} ~ {end_date} | refresh_list={refresh_list}")
    log(f"🗓️ calendar: ticker={_calendar_ticker()} mode={window_mode} cache={cal.get('cache_path')} err={cal.get('error')}")
    log(f"⚙️ daily_batch_size={_daily_batch_size()} threads={_yf_threads_enabled()}")

    # refresh stock_info from universe (required if repo is stateless)
    if refresh_list:
        df_uni = sync_stock_info_from_universe(db_path)
    else:
        df_uni = load_universe_df()
        if df_uni is None or df_uni.empty:
            df_uni = sync_stock_info_from_universe(db_path)

    tickers = df_uni["yf_symbol"].astype(str).tolist()
    name_map = {str(r["yf_symbol"]): str(r.get("name") or "Unknown") for _, r in df_uni.iterrows()}

    # rolling window: delete window then refill (so DB doesn't grow forever)
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("DELETE FROM stock_prices WHERE date >= ?", (start_date,))
        conn.commit()
    finally:
        conn.close()

    status: Dict[str, str] = {}
    err_final: Dict[str, str] = {}
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    batches = [tickers[i : i + _daily_batch_size()] for i in range(0, len(tickers), _daily_batch_size())]
    pbar = tqdm(batches, desc="IN daily sync", unit="batch")

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        for batch in pbar:
            df_long, failed_batch, err_msg = _download_daily_batch(batch, start_date, end_excl_date)

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
                err_final[sym] = "daily_missing_or_no_close"

            time.sleep(_batch_sleep_sec())

        # record errors
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

        try:
            maxd = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()[0]
            log(f"🔎 stock_prices MAX(date) = {maxd} (window end={end_date})")
        except Exception:
            pass

        log("🧹 VACUUM...")
        conn.execute("VACUUM")
        conn.commit()

        total_in_info = conn.execute("SELECT COUNT(*) FROM stock_info WHERE market='IN'").fetchone()[0]
    finally:
        conn.close()

    success = sum(1 for v in status.values() if v == "ok")
    failed = sum(1 for v in status.values() if v == "fail")

    log(f"📊 IN daily sync done | ok:{success} fail:{failed}")
    return {
        "success": success,
        "failed": failed,
        "total": int(total_in_info),
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
        "batch": {"size": _daily_batch_size(), "threads": _yf_threads_enabled()},
    }


# =============================================================================
# Snapshot builder: streak + intraday (DB + yfinance)
# =============================================================================
def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute(
        "SELECT MAX(date) FROM stock_prices WHERE date <= ? AND close IS NOT NULL",
        (ymd,),
    ).fetchone()
    return row[0] if row and row[0] else None


def _load_stock_info(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
          symbol,
          local_symbol,
          name,
          sector,
          industry,
          band,
          limit_pct,
          market,
          market_detail
        FROM stock_info
        WHERE market='IN'
        """,
        conn,
    )


def _fetch_prevclose_map(conn: sqlite3.Connection, ymd_effective: str) -> Dict[str, Optional[float]]:
    """
    prev_close = LAG(close) for each symbol at ymd_effective
    """
    sql = """
    WITH p AS (
      SELECT
        symbol,
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
      FROM stock_prices
      WHERE close IS NOT NULL
        AND date <= ?
    )
    SELECT symbol, prev_close
    FROM p
    WHERE date = ?
    """
    df = pd.read_sql_query(sql, conn, params=(ymd_effective, ymd_effective))
    out: Dict[str, Optional[float]] = {}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        sym = str(r["symbol"])
        out[sym] = None if pd.isna(r["prev_close"]) else float(r["prev_close"])
    return out


def _fetch_recent_closes(conn: sqlite3.Connection, ymd_effective: str, max_rows_per_symbol: int = 40) -> pd.DataFrame:
    """
    Fetch last N closes per symbol (<= ymd_effective) for streak calc.
    """
    sql = f"""
    WITH p AS (
      SELECT
        symbol,
        date,
        close,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
      FROM stock_prices
      WHERE date <= ?
        AND close IS NOT NULL
    )
    SELECT symbol, date, close
    FROM p
    WHERE rn <= {int(max_rows_per_symbol)}
    """
    return pd.read_sql_query(sql, conn, params=(ymd_effective,))


def _compute_locked_streak(closes: pd.Series, limit_pct: Optional[float]) -> int:
    """
    locked definition: c_today >= c_prev * (1 + limit_pct)
    - If limit_pct is None/0 => streak = 0 (No Band)
    """
    try:
        if limit_pct is None:
            return 0
        lp = float(limit_pct)
        if lp <= 0:
            return 0
    except Exception:
        return 0

    if closes is None or len(closes) < 2:
        return 0

    closes = pd.to_numeric(closes, errors="coerce").dropna()
    if len(closes) < 2:
        return 0

    streak = 0
    for i in range(len(closes) - 1, 0, -1):
        c_today = float(closes.iloc[i])
        c_prev = float(closes.iloc[i - 1])
        if c_prev <= 0:
            break
        if c_today >= c_prev * (1.0 + lp):
            streak += 1
        else:
            break
    return int(streak)


def _download_intraday_chunk(tickers: List[str]) -> pd.DataFrame:
    return yf.download(
        tickers=" ".join(tickers),
        period=_intraday_period(),
        interval=_intraday_interval(),
        group_by="ticker",
        threads=_yf_threads_enabled(),
        auto_adjust=True,
        progress=False,
        timeout=60,
    )


def _extract_intraday_ohlcv(df_intra: pd.DataFrame, sym: str) -> Optional[Dict[str, Any]]:
    """
    Returns dict: open/high/low/close/volume or None if missing.
    Handles both single-ticker and MultiIndex outputs.
    """
    if df_intra is None or df_intra.empty:
        return None

    # single ticker
    if not isinstance(df_intra.columns, pd.MultiIndex):
        sub = df_intra
    else:
        level1 = set([c[1] for c in df_intra.columns])
        use_level = 1 if sym in level1 else 0
        try:
            sub = df_intra.xs(sym, axis=1, level=use_level, drop_level=False)
        except Exception:
            return None
        if sub is None or sub.empty:
            return None
        if use_level == 1:
            sub.columns = [c[0] for c in sub.columns]
        else:
            sub.columns = [c[1] for c in sub.columns]

    cols = [str(c) for c in sub.columns]
    sub2 = sub.copy()
    sub2.columns = cols

    if "Close" not in sub2.columns or sub2["Close"].dropna().empty:
        return None

    try:
        close = float(sub2["Close"].dropna().iloc[-1])
        high = float(sub2["High"].dropna().max()) if "High" in sub2.columns and not sub2["High"].dropna().empty else close
        low = float(sub2["Low"].dropna().min()) if "Low" in sub2.columns and not sub2["Low"].dropna().empty else close
        open_ = float(sub2["Open"].dropna().iloc[0]) if "Open" in sub2.columns and not sub2["Open"].dropna().empty else close

        vol = None
        if "Volume" in sub2.columns and not sub2["Volume"].dropna().empty:
            vol = sub2["Volume"].dropna().iloc[-1]
        volume = int(vol) if vol is not None and str(vol) != "nan" else None

        return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}
    except Exception:
        return None


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    db_path = _db_path()
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"IN DB not found: {db_path} (set IN_DB_PATH to override)")

    conn = sqlite3.connect(db_path)
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd

        df_info = _load_stock_info(conn)
        if df_info is None or df_info.empty:
            raise RuntimeError("stock_info is empty. Run run_sync(refresh_list=True) first.")

        # Ensure types
        df_info["symbol"] = df_info["symbol"].astype(str)
        df_info["limit_pct"] = pd.to_numeric(df_info.get("limit_pct"), errors="coerce")

        tickers = df_info["symbol"].tolist()

        prev_close_map = _fetch_prevclose_map(conn, ymd_effective)
        df_recent = _fetch_recent_closes(conn, ymd_effective, max_rows_per_symbol=40)
    finally:
        conn.close()

    log(f"🕒 IN requested ymd={ymd} slot={slot} asof={asof}")
    log(f"📅 IN ymd_effective (from DB) = {ymd_effective}")
    log(f"📦 IN stock_info symbols = {len(tickers)}")

    # streak map
    streak_map: Dict[str, int] = {}
    if df_recent is not None and not df_recent.empty:
        df_recent["date"] = pd.to_datetime(df_recent["date"], errors="coerce")
        df_recent = df_recent.dropna(subset=["symbol", "date"])
        df_recent = df_recent.sort_values(["symbol", "date"])

        lp_map = {str(r.symbol): (None if pd.isna(r.limit_pct) else float(r.limit_pct)) for r in df_info.itertuples(index=False)}

        for sym, g in df_recent.groupby("symbol", sort=False):
            streak_map[str(sym)] = _compute_locked_streak(g["close"], lp_map.get(str(sym)))

    # intraday yfinance (chunked)
    snapshot_main: List[Dict[str, Any]] = []
    failed: List[str] = []

    chunks = [tickers[i : i + _intraday_chunk_size()] for i in range(0, len(tickers), _intraday_chunk_size())]
    pbar = tqdm(chunks, desc="IN intraday 1m", unit="chunk")

    # create a quick info lookup
    info_map: Dict[str, Dict[str, Any]] = {}
    for r in df_info.itertuples(index=False):
        info_map[str(r.symbol)] = {
            "local_symbol": getattr(r, "local_symbol", "") or "",
            "name": getattr(r, "name", None) or "Unknown",
            "sector": getattr(r, "sector", None) or "Unclassified",
            "industry": getattr(r, "industry", None) or "Unclassified",
            "band": getattr(r, "band", None),
            "limit_pct": None if pd.isna(getattr(r, "limit_pct", None)) else float(getattr(r, "limit_pct")),
            "market_detail": getattr(r, "market_detail", None) or "NSE|db",
        }

    for chunk in pbar:
        try:
            df_intra = _download_intraday_chunk(chunk)
        except Exception:
            for sym in chunk:
                failed.append(sym)
            time.sleep(_intraday_chunk_sleep())
            continue

        for sym in chunk:
            meta = info_map.get(sym)
            if not meta:
                failed.append(sym)
                continue

            ohlcv = _extract_intraday_ohlcv(df_intra, sym)
            if not ohlcv:
                failed.append(sym)
                continue

            snapshot_main.append(
                {
                    "symbol": sym,  # Yahoo symbol
                    "local_symbol": meta["local_symbol"],
                    "name": meta["name"],
                    "sector": meta["sector"],
                    "industry": meta["industry"],

                    # DB-derived
                    "prev_close": prev_close_map.get(sym),
                    "streak": int(streak_map.get(sym, 0)),

                    # intraday-derived
                    "open": ohlcv["open"],
                    "high": ohlcv["high"],
                    "low": ohlcv["low"],
                    "close": ohlcv["close"],
                    "volume": ohlcv["volume"],

                    # DB stock_info-derived (universe already merged into DB)
                    "band": meta["band"],
                    "limit_pct": meta["limit_pct"],

                    "market": "in",
                    "market_detail": "NSE|yfinance",
                }
            )

        time.sleep(_intraday_chunk_sleep())

    return {
        "market": "in",
        "slot": slot,
        "asof": asof,
        "ymd": ymd,
        "ymd_effective": ymd_effective,
        "snapshot_main": snapshot_main,
        "snapshot_open": [],
        "stats": {
            "snapshot_main_count": int(len(snapshot_main)),
            "failed_count": int(len(failed)),
            "universe_total": int(len(tickers)),
        },
        "meta": {
            "db_path": db_path,
            "ymd_effective": ymd_effective,
            "failed_sample": failed[:50],
        },
    }


if __name__ == "__main__":
    # default: sync rolling window daily bars
    run_sync()
