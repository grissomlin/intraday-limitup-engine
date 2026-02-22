# markets/ca/ca_prices.py
# -*- coding: utf-8 -*-
"""
Canada rolling-window price sync (DB-based) — Batch + Single fallback
(Adapted from markets/uk/uk_prices.py)

Enhancements in this revision (保持原功能 + 新增穩定性):
- ✅ Permanent skiplist for "never recover" symbols:
    - YFTzMissingError('no timezone found')
    - YFPricesMissingError('no price data found')
  These will be appended to a skip file and skipped in future runs.
- ✅ All controls via ENV (but defaults are ON):
    - CA_SKIP_SYMBOLS_PATH (default data/cache/ca/skip_symbols.txt)
    - CA_SKIP_TZ_MISSING   (default 1)
    - CA_SKIP_NO_PRICE     (default 1)
    - CA_YF_SLEEP_SEC      (optional override; else use CA_SLEEP_SEC)

Public API:
  - run_sync(start_date=None, end_date=None, refresh_list=True)

Env (CA):
- CA_DB_PATH
- CA_ROLLING_TRADING_DAYS        (default 30)
- CA_CALENDAR_TICKER             (default ^GSPTSE)
- CA_CAL_LOOKBACK_CAL_DAYS       (default 180)
- CA_ROLLING_CAL_DAYS            (default 90)
- CA_DAILY_BATCH_SIZE            (default 80)
- CA_BATCH_SLEEP_SEC             (default 0.15)
- CA_FALLBACK_SINGLE             (default 1)
- CA_YF_THREADS                  (default 0)   # 0 safer
- CA_SLEEP_SEC                   (default 0.05)
- CA_YF_SLEEP_SEC                (optional; if set, overrides CA_SLEEP_SEC)
- CA_SKIP_SYMBOLS_PATH           (default data/cache/ca/skip_symbols.txt)
- CA_SKIP_TZ_MISSING             (default 1)
- CA_SKIP_NO_PRICE               (default 1)
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

from .ca_list import get_ca_stock_list


def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


# ---------------------------------------------------------------------
# config
# ---------------------------------------------------------------------
def _db_path() -> str:
    return os.getenv("CA_DB_PATH", os.path.join(os.path.dirname(__file__), "ca_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("CA_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    return os.getenv("CA_CALENDAR_TICKER", "^GSPTSE")  # S&P/TSX Composite


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("CA_CAL_LOOKBACK_CAL_DAYS", "180"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("CA_ROLLING_CAL_DAYS", "90"))


def _batch_size() -> int:
    return int(os.getenv("CA_DAILY_BATCH_SIZE", "80"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("CA_BATCH_SLEEP_SEC", "0.15"))


def _fallback_single_enabled() -> bool:
    return str(os.getenv("CA_FALLBACK_SINGLE", "1")).strip() == "1"


def _yf_threads_enabled() -> bool:
    return str(os.getenv("CA_YF_THREADS", "0")).strip() == "1"


def _single_sleep_sec() -> float:
    """
    Keep backward compat:
      - if CA_YF_SLEEP_SEC is set, prefer it
      - else fallback to CA_SLEEP_SEC (existing env)
    """
    v = (os.getenv("CA_YF_SLEEP_SEC") or "").strip()
    if v:
        try:
            return float(v)
        except Exception:
            pass
    return float(os.getenv("CA_SLEEP_SEC", "0.05"))


# ---------------------------------------------------------------------
# Permanent skiplist (NEW)
# ---------------------------------------------------------------------
def _env_on(name: str, default: str = "1") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "y", "on"}


CA_SKIP_SYMBOLS_PATH = os.getenv("CA_SKIP_SYMBOLS_PATH", "data/cache/ca/skip_symbols.txt")
CA_SKIP_TZ_MISSING = _env_on("CA_SKIP_TZ_MISSING", "1")
CA_SKIP_NO_PRICE = _env_on("CA_SKIP_NO_PRICE", "1")


def _norm_sym(sym: str) -> str:
    return str(sym or "").strip().upper()


def _load_skipset() -> set[str]:
    try:
        p = Path(CA_SKIP_SYMBOLS_PATH)
        if not p.exists():
            return set()
        out: set[str] = set()
        for ln in p.read_text(encoding="utf-8").splitlines():
            s = ln.strip()
            if not s or s.startswith("#"):
                continue
            # allow: "CCS.TO" or "CCS.TO\treason"
            out.add(_norm_sym(s.split()[0]))
        return out
    except Exception:
        return set()


def _append_skip(sym: str, reason: str = "") -> None:
    try:
        p = Path(CA_SKIP_SYMBOLS_PATH)
        p.parent.mkdir(parents=True, exist_ok=True)
        s = _norm_sym(sym)
        # avoid duplicates
        existing = _load_skipset()
        if s in existing:
            return
        line = s if not reason else f"{s}\t{reason}"
        with p.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        log(f"🧷 added to skiplist: {s} ({reason})")
    except Exception:
        pass


def _classify_never_recover_error(err: str) -> Optional[str]:
    """
    Return a short reason if this error should be permanently skipped.
    """
    e = (err or "").lower()

    # tz missing: yfinance often raises YFTzMissingError('possibly delisted; no timezone found')
    if CA_SKIP_TZ_MISSING and ("yftzmissingerror" in e or "no timezone found" in e):
        return "tz_missing"

    # no price: yfinance raises YFPricesMissingError('possibly delisted; no price data found ...')
    if CA_SKIP_NO_PRICE and ("yfpricesmissingerror" in e or "no price data found" in e):
        return "no_price"

    return None


# ---------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# calendar helpers (yfinance)
# ---------------------------------------------------------------------
def _latest_trading_day_from_calendar(asof_ymd: Optional[str] = None) -> Optional[str]:
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
        return (start_incl.strftime("%Y-%m-%d"), end_incl.strftime("%Y-%m-%d"), end_excl.strftime("%Y-%m-%d"))
    except Exception:
        return None, None, None


# ---------------------------------------------------------------------
# list helpers
# ---------------------------------------------------------------------
def _get_ca_items(db_path: str, refresh_list: bool) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    try:
        raw = get_ca_stock_list(db_path=Path(db_path), refresh_list=refresh_list)
        if isinstance(raw, list) and raw:
            if isinstance(raw[0], dict):
                for d in raw:
                    sym = str(d.get("symbol", "")).strip()
                    if sym:
                        items.append((sym, str(d.get("name", "") or "Unknown")))
            else:
                for t in raw:
                    if not t:
                        continue
                    sym = str(t[0]).strip()
                    name = str(t[1]).strip() if len(t) >= 2 else "Unknown"
                    if sym:
                        items.append((sym, name or "Unknown"))
    except Exception as e:
        log(f"⚠️ ca_list.get_ca_stock_list failed (fallback DB): {e}")

    if items:
        return items

    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute("SELECT symbol, name FROM stock_info WHERE market='CA'").fetchall()
            for s, n in rows:
                if s:
                    items.append((str(s), str(n or "Unknown")))
        finally:
            conn.close()
    return items


# ---------------------------------------------------------------------
# download core
# ---------------------------------------------------------------------
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
                    time.sleep(1.2)
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
            err_text = f"{e}"
            last_err = f"exception: {err_text}"

            # ✅ NEW: classify and permanently skip never-recover errors
            reason = _classify_never_recover_error(err_text)
            if reason:
                _append_skip(symbol, reason)
                # treat as a special failure (won't keep retrying)
                return None, f"skip_{reason}"

            if attempt < max_retries:
                time.sleep(1.8)
                continue
            return None, last_err
    return None, last_err or "unknown"


def _download_batch(tickers: List[str], start_date: str, end_date_exclusive: str) -> Tuple[pd.DataFrame, List[str], Optional[str]]:
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
        # NOTE: batch exception is usually network/rate-limit; do NOT add to permanent skip here.
        empty = pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        return empty, tickers, f"yf.download exception: {e}"

    if df is None or df.empty:
        empty = pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        return empty, tickers, "yf.download empty"

    rows: List[Dict[str, Any]] = []
    failed: List[str] = []

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
                    {"symbol": sym, "date": r.get("date"), "open": r.get("open"), "high": r.get("high"), "low": r.get("low"), "close": r.get("close"), "volume": r.get("volume")}
                )
    else:
        level0 = set([c[0] for c in df.columns])
        level1 = set([c[1] for c in df.columns])
        use_level = 1 if any(s in level1 for s in tickers[: min(3, len(tickers))]) else 0

        for sym in tickers:
            try:
                sub = df.xs(sym, axis=1, level=use_level, drop_level=False)
                if sub is None or sub.empty:
                    failed.append(sym)
                    continue

                sub.columns = [c[0] for c in sub.columns] if use_level == 1 else [c[1] for c in sub.columns]

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
                        {"symbol": sym, "date": r.get("date"), "open": r.get("open"), "high": r.get("high"), "low": r.get("low"), "close": r.get("close"), "volume": r.get("volume")}
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
        (str(r.symbol), str(r.date)[:10],
         None if pd.isna(r.open) else float(r.open),
         None if pd.isna(r.high) else float(r.high),
         None if pd.isna(r.low) else float(r.low),
         None if pd.isna(r.close) else float(r.close),
         None if pd.isna(r.volume) else int(r.volume))
        for r in dfw.itertuples(index=False)
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO stock_prices (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _write_download_errors(conn: sqlite3.Connection, final_failed: Dict[str, str], name_map: Dict[str, str], start_date: str, end_date_inclusive: str) -> None:
    if not final_failed:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [(sym, name_map.get(sym, "Unknown"), start_date, end_date_inclusive, err, now) for sym, err in final_failed.items()]
    conn.executemany(
        "INSERT INTO download_errors (symbol, name, start_date, end_date, error, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def run_sync(start_date: Optional[str] = None, end_date: Optional[str] = None, refresh_list: bool = True) -> Dict[str, Any]:
    db_path = _db_path()
    init_db(db_path)

    # decide window
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
        start_ymd = str(start_date)[:10] if start_date else (datetime.now() - timedelta(days=_fallback_rolling_cal_days())).strftime("%Y-%m-%d")
        end_excl_date = (pd.to_datetime(end_inclusive) + timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"⚠️ Trading-day window unavailable; fallback cal-days | {start_ymd} ~ {end_inclusive} (end_excl={end_excl_date})")

    # list
    items = _get_ca_items(db_path, refresh_list=refresh_list)
    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False, "db_path": db_path}

    # ✅ NEW: filter permanent skipset
    skipset = _load_skipset()
    tickers_all = [s for s, _ in items if s]
    name_map_all = {s: (n or "Unknown") for s, n in items if s}

    tickers = [s for s in tickers_all if _norm_sym(s) not in skipset]
    name_map = {s: name_map_all.get(s, "Unknown") for s in tickers}
    total = len(tickers_all)
    skipped_perm = len(tickers_all) - len(tickers)

    log(f"📦 CA DB = {db_path}")
    log(f"🚀 CA run_sync | window: {start_ymd} ~ {end_inclusive} (end_excl={end_excl_date}) | refresh_list={refresh_list}")
    log(f"⚙️ batch_size={_batch_size()} threads={_yf_threads_enabled()} fallback_single={_fallback_single_enabled()} total={total} skipped_perm={skipped_perm}")
    log(f"🧷 skiplist_path={CA_SKIP_SYMBOLS_PATH} (tz_missing={CA_SKIP_TZ_MISSING}, no_price={CA_SKIP_NO_PRICE})")

    # rolling window delete
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("DELETE FROM stock_prices WHERE date >= ?", (start_ymd,))
        conn.commit()
    finally:
        conn.close()

    # batch download
    batches = [tickers[i : i + _batch_size()] for i in range(0, len(tickers), _batch_size())]
    pbar = tqdm(batches, desc="CA批次同步", unit="batch")

    ok_set: set[str] = set()
    final_failed: Dict[str, str] = {}

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        for batch in pbar:
            # batch could contain symbols appended to skiplist in previous loop, re-filter defensively
            if skipset:
                batch = [s for s in batch if _norm_sym(s) not in skipset]
                if not batch:
                    time.sleep(_batch_sleep_sec())
                    continue

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
                    final_failed.pop(sym, None)

            if _fallback_single_enabled():
                need_fallback = [s for s in batch if s in final_failed]
                for sym in need_fallback:
                    # if got added to skiplist during the run, skip now
                    if _norm_sym(sym) in _load_skipset():
                        final_failed.pop(sym, None)
                        continue

                    df_one, err_one = _download_one(sym, start_ymd, end_excl_date)
                    if df_one is not None and not df_one.empty:
                        _insert_prices(conn, df_one)
                        conn.commit()
                        ok_set.add(sym)
                        final_failed.pop(sym, None)
                    else:
                        if err_one:
                            # ✅ if it is a skip_* result, don't count as failed
                            if str(err_one).startswith("skip_"):
                                final_failed.pop(sym, None)
                            else:
                                final_failed[sym] = err_one

                    time.sleep(_single_sleep_sec())

            time.sleep(_batch_sleep_sec())

        _write_download_errors(conn, final_failed, name_map_all, start_ymd, end_inclusive)
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
    log(f"📊 CA 同步完成 | 成功:{success} 失敗:{failed} / {total} (skipped_perm={skipped_perm})")

    return {
        "success": int(success),
        "total": int(total),
        "failed": int(failed),
        "has_changed": success > 0,
        "db_path": db_path,
        "window": {"start": start_ymd, "end": end_inclusive, "end_excl": end_excl_date, "mode": window_mode},
        "calendar": {"ticker": _calendar_ticker(), "n_trading_days": int(n_days), "lookback_cal_days": int(_calendar_lookback_cal_days())},
        "batch": {"size": int(_batch_size()), "threads": bool(_yf_threads_enabled()), "fallback_single": bool(_fallback_single_enabled())},
        "skiplist": {"path": CA_SKIP_SYMBOLS_PATH, "skipped_perm": int(skipped_perm)},
    }