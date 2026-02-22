# markets/kr/downloader.py
# -*- coding: utf-8 -*-
"""
KR (Korea) downloader (DB-based, rolling window, NO incremental)
----------------------------------------------------------------
✅ 保留：DB schema / trading-day window 邏輯（你原本那套）
✅ 保留：stock list（KRX corpList -> Yahoo tickers）
✅ 保留：run_sync 下載流程（batch + fallback）完整不動

✅ 精簡：把「snapshot builder(run_intraday SQL + 計算)」移出 downloader.py
   -> run_intraday 改成委派到 markets/kr/snapshot_builder.py

新增/沿用環境變數：
- KR_DAILY_BATCH_SIZE        預設 200
- KR_BATCH_SLEEP_SEC         預設 0.05（每批 sleep）
- KR_FALLBACK_SINGLE         預設 1（失敗 ticker 用單檔補抓）
- KR_YF_THREADS              預設 1（batch yf.download threads=True）
- KR_SLEEP_SEC               （單檔 fallback sleep）
"""

from __future__ import annotations

import os
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from io import StringIO

import pandas as pd
import yfinance as yf
from tqdm import tqdm
import requests


# =============================================================================
# Config / Paths
# =============================================================================
def _default_db_path() -> str:
    return os.getenv("KR_DB_PATH", os.path.join(os.path.dirname(__file__), "kr_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("KR_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    return os.getenv("KR_CALENDAR_TICKER", "^KS11")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("KR_CAL_LOOKBACK_CAL_DAYS", "180"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("KR_ROLLING_CAL_DAYS", "90"))


def _list_url() -> str:
    return os.getenv(
        "KR_LIST_URL",
        "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13",
    )


def _sleep_sec() -> float:
    # 單檔 fallback sleep
    return float(os.getenv("KR_SLEEP_SEC", "0.03"))


def _batch_size() -> int:
    return int(os.getenv("KR_DAILY_BATCH_SIZE", "200"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("KR_BATCH_SLEEP_SEC", "0.05"))


def _fallback_single_enabled() -> bool:
    return str(os.getenv("KR_FALLBACK_SINGLE", "1")).strip() == "1"


def _yf_threads_enabled() -> bool:
    return str(os.getenv("KR_YF_THREADS", "1")).strip() == "1"


def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


# =============================================================================
# DB Schema (unchanged)
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
        conn.commit()
    finally:
        conn.close()


# =============================================================================
# Trading-day helpers (unchanged)
# =============================================================================
def _latest_trading_day_from_calendar(asof_ymd: Optional[str] = None) -> Optional[str]:
    cal_ticker = _calendar_ticker()
    lookback = _calendar_lookback_cal_days()

    try:
        end_dt = pd.to_datetime(asof_ymd) if asof_ymd else pd.Timestamp.now()
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
            return None

        dates = pd.to_datetime(df_cal.index).tz_localize(None).normalize()
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
# Stock list (KRX -> Yahoo tickers) (mostly unchanged)
# =============================================================================
def _normalize_code(v: Any) -> str:
    s = str(v).strip()
    s = s.split(".")[0].strip()
    s = s.upper()
    s = "".join(ch for ch in s if ch.isalnum())
    if len(s) > 6:
        s = s[-6:]
    return s.rjust(6, "0")


def _to_yahoo_symbol(code6: str, market: str) -> Optional[str]:
    m = (market or "").strip().upper()
    if m in ("KOSPI", "유가증권"):
        return f"{code6}.KS"
    if m in ("KOSDAQ", "코스닥"):
        return f"{code6}.KQ"
    if m in ("KONEX", "코넥스"):
        return None
    return f"{code6}.KS"


def _fetch_krx_corplist_html(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Referer": "https://kind.krx.co.kr/",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "close",
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    content = resp.content

    for enc in ("euc-kr", "cp949", "utf-8"):
        try:
            return content.decode(enc)
        except Exception:
            continue

    return content.decode("euc-kr", errors="replace")


def get_kr_stock_list(db_path: str, refresh_list: bool = True) -> List[Tuple[str, str]]:
    if not refresh_list and os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute("SELECT symbol, name FROM stock_info").fetchall()
            items = [(s, n or "Unknown") for s, n in rows if s]
            if items:
                log(f"✅ 使用 DB stock_info 既有清單: {len(items)} 檔")
                return items
        finally:
            conn.close()

    url = _list_url()
    log(f"📡 從 KRX KIND 同步股票清單: {url}")

    try:
        html = _fetch_krx_corplist_html(url)
        tables = pd.read_html(StringIO(html))
        if not tables:
            raise RuntimeError("no tables from KRX corpList")
        df = tables[0].copy()
    except Exception as e:
        log(f"⚠️ KRX 清單取得失敗（fallback DB）：{e}")
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute("SELECT symbol, name FROM stock_info").fetchall()
            items = [(s, n or "Unknown") for s, n in rows if s]
            if items:
                log(f"✅ 使用 DB stock_info 既有清單: {len(items)} 檔")
                return items
        finally:
            conn.close()
        log("❌ 無可用 KR 股票清單（KRX 失敗且 DB 無既有名單）")
        return []

    name_col = "회사명" if "회사명" in df.columns else ("Company Name" if "Company Name" in df.columns else None)
    code_col = "종목코드" if "종목코드" in df.columns else ("Stock Code" if "Stock Code" in df.columns else None)
    mkt_col = "시장구분" if "시장구분" in df.columns else ("Market" if "Market" in df.columns else None)
    업종_col = "업종" if "업종" in df.columns else ("Industry" if "Industry" in df.columns else None)

    if not name_col or not code_col or not mkt_col:
        raise RuntimeError(f"unexpected KRX columns: {list(df.columns)}")

    items: List[Tuple[str, str]] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db_path)
    try:
        for _, r in df.iterrows():
            code6 = _normalize_code(r.get(code_col, ""))
            if len(code6) != 6:
                continue

            name = str(r.get(name_col, "")).strip() or "Unknown"
            market_raw = str(r.get(mkt_col, "")).strip()
            업종 = str(r.get(업종_col, "")).strip() if 업종_col else ""

            sym = _to_yahoo_symbol(code6, market_raw)
            if not sym:
                continue

            sector = 업종 if 업종 else "미분류"

            m_upper = market_raw.strip().upper()
            if m_upper in ("KOSPI", "유가증권"):
                market_detail = "KOSPI"
            elif m_upper in ("KOSDAQ", "코스닥"):
                market_detail = "KOSDAQ"
            elif m_upper in ("KONEX", "코넥스"):
                market_detail = "KONEX"
            else:
                market_detail = "KOSDAQ" if sym.endswith(".KQ") else "KOSPI"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (sym, name, sector, market_raw, market_detail, now),
            )
            items.append((sym, name))

        conn.commit()
    finally:
        conn.close()

    log(f"✅ KR 股票清單導入完成: {len(items)} 檔")
    return items


# =============================================================================
# Download prices (batch) + fallback single
# =============================================================================
def _download_one(symbol: str, start_date: str, end_date_exclusive: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """單檔 fallback（保留你原本語意）"""
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
    ✅ 批次下載：回傳 (long_df, failed_tickers, err_msg)
    long_df 欄位：symbol,date,open,high,low,close,volume
    """
    empty = pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
    if not tickers:
        return empty, [], None

    tickers_str = " ".join(tickers)

    try:
        df = yf.download(
            tickers=tickers_str,
            start=start_date,
            end=end_date_exclusive,  # yfinance end exclusive
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            threads=_yf_threads_enabled(),
            progress=False,
            timeout=60,
        )
    except Exception as e:
        return empty, tickers, f"yf.download exception: {e}"

    if df is None or df.empty:
        return empty, tickers, "yf.download empty"

    rows: List[Dict[str, Any]] = []
    failed: List[str] = []

    # 單一 ticker：非 MultiIndex
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
        # MultiIndex：欄位常見 ('Open','005930.KS') 或 ('005930.KS','Open')
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
                continue

    out = pd.DataFrame(rows)
    if out.empty:
        return empty, sorted(list(set(failed + tickers))), "batch produced no rows"

    out = out.dropna(subset=["symbol", "date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    failed_unique = sorted(list(set(failed)))
    return out, failed_unique, None


def _df_to_db_rows(
    df_long: pd.DataFrame,
) -> List[Tuple[str, str, Optional[float], Optional[float], Optional[float], Optional[float], Optional[int]]]:
    """把 long df 轉成 executemany rows"""
    if df_long is None or df_long.empty:
        return []
    dfw = df_long.copy()
    dfw["volume"] = pd.to_numeric(dfw["volume"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        dfw[col] = pd.to_numeric(dfw[col], errors="coerce")

    rows: List[Tuple[str, str, Optional[float], Optional[float], Optional[float], Optional[float], Optional[int]]] = []
    for r in dfw.itertuples(index=False):
        rows.append(
            (
                str(getattr(r, "symbol")),
                str(getattr(r, "date"))[:10],
                None if pd.isna(getattr(r, "open")) else float(getattr(r, "open")),
                None if pd.isna(getattr(r, "high")) else float(getattr(r, "high")),
                None if pd.isna(getattr(r, "low")) else float(getattr(r, "low")),
                None if pd.isna(getattr(r, "close")) else float(getattr(r, "close")),
                None if pd.isna(getattr(r, "volume")) else int(getattr(r, "volume")),
            )
        )
    return rows


def _insert_prices_rows(conn: sqlite3.Connection, rows: List[Tuple]) -> None:
    if not rows:
        return
    conn.executemany(
        "INSERT OR REPLACE INTO stock_prices (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _insert_error(
    conn: sqlite3.Connection,
    symbol: str,
    name: str,
    start_date: str,
    end_date: str,
    error: str,
) -> None:
    conn.execute(
        "INSERT INTO download_errors (symbol, name, start_date, end_date, error, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (symbol, name, start_date, end_date, error, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )


# =============================================================================
# run_sync (window logic unchanged, download stage improved stats/errors/speed)
# =============================================================================
def run_sync(start_date=None, end_date=None, *, refresh_list: bool = True) -> Dict[str, Any]:
    db_path = _default_db_path()
    init_db(db_path)

    # end_date 預設用「資料源最新可用交易日」
    if not end_date:
        end_date = _latest_trading_day_from_calendar() or datetime.now().strftime("%Y-%m-%d")

    n_days = _rolling_trading_days()
    start_td, end_incl, _end_excl = _infer_window_by_trading_days(end_date, n_days)

    if start_td and end_incl:
        start_date = start_td
        end_date = end_incl
        end_excl_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"📅 Trading-day window OK | last {n_days} trading days | {start_date} ~ {end_date} (end_excl={end_excl_date})")
        window_mode = "trading_days"
    else:
        if not start_date:
            start_date = (datetime.now() - timedelta(days=_fallback_rolling_cal_days())).strftime("%Y-%m-%d")
        end_excl_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"⚠️ Trading-day window unavailable; fallback to cal-days | {start_date} ~ {end_date} (end_excl={end_excl_date})")
        window_mode = "cal_days"

    log(f"📦 KR DB = {db_path}")
    log(f"🚀 KR run_sync | window: {start_date} ~ {end_date} | refresh_list={refresh_list}")
    log(f"⚙️ batch_size={_batch_size()} threads={_yf_threads_enabled()} fallback_single={_fallback_single_enabled()}")

    items = get_kr_stock_list(db_path, refresh_list=refresh_list)
    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False}

    tickers = [s for s, _ in items]
    name_map = {s: (n or "Unknown") for s, n in items}

    # 先刪 window 起點之後的舊資料（不變）
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("DELETE FROM stock_prices WHERE date >= ?", (start_date,))
        conn.commit()
    finally:
        conn.close()

    # ✅ 新統計：用「實際成功寫入 DB 的 ticker」
    ok_tickers: set[str] = set()
    failed_tickers: Dict[str, str] = {}  # sym -> reason (final)

    batches = [tickers[i : i + _batch_size()] for i in range(0, len(tickers), _batch_size())]
    pbar = tqdm(batches, desc="KR批次同步", unit="batch")

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        for batch in pbar:
            df_long, failed_batch, err_msg = _download_batch(batch, start_date, end_excl_date)

            # --- case 1) 整批炸掉：只記 1 筆 error（乾淨）
            if err_msg:
                sample = ",".join(batch[: min(10, len(batch))])
                msg = f"[BATCH_ERROR] batch_size={len(batch)} sample={sample} | {err_msg}"
                try:
                    _insert_error(conn, "__BATCH__", f"KR_BATCH({len(batch)})", start_date, end_date, msg)
                    conn.commit()
                except Exception:
                    pass
                # 整批先標成 failed（之後如果 fallback 成功會覆蓋）
                for sym in batch:
                    if sym not in ok_tickers:
                        failed_tickers[sym] = "batch_error"
                time.sleep(_batch_sleep_sec())
                continue

            # --- case 2) batch 有資料：先寫入一次（快）
            batch_rows = _df_to_db_rows(df_long)
            if batch_rows:
                _insert_prices_rows(conn, batch_rows)
                conn.commit()

                # 以 df_long 真正包含到的 symbol 為成功（避免灌水）
                ok_in_df = set(df_long["symbol"].astype(str).unique().tolist())
                ok_tickers.update(ok_in_df)
                # 成功就把 failed 標記移除
                for sym in list(ok_in_df):
                    failed_tickers.pop(sym, None)

            # --- failed_batch：做 fallback（可選）
            if failed_batch:
                # 先標記 batch_failed（後續 fallback 成功會移除）
                for sym in failed_batch:
                    if sym not in ok_tickers:
                        failed_tickers[sym] = "batch_failed"

                if _fallback_single_enabled():
                    fallback_rows_all: List[Tuple] = []
                    fallback_ok: set[str] = set()

                    for sym in failed_batch:
                        df_one, err = _download_one(sym, start_date, end_excl_date)
                        if df_one is not None and not df_one.empty:
                            rows_one = _df_to_db_rows(df_one)
                            if rows_one:
                                fallback_rows_all.extend(rows_one)
                                fallback_ok.add(sym)
                        else:
                            # 只記「單檔失敗」的 error（不會爆量到不可控）
                            if err:
                                try:
                                    _insert_error(conn, sym, name_map.get(sym, "Unknown"), start_date, end_date, f"[SINGLE_FAIL] {err}")
                                except Exception:
                                    pass
                        time.sleep(_sleep_sec())

                    if fallback_rows_all:
                        _insert_prices_rows(conn, fallback_rows_all)
                        conn.commit()

                    if fallback_ok:
                        ok_tickers.update(fallback_ok)
                        for sym in fallback_ok:
                            failed_tickers.pop(sym, None)

            time.sleep(_batch_sleep_sec())

        # quick sanity
        try:
            maxd = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()[0]
            log(f"🔎 stock_prices MAX(date) = {maxd} (window end={end_date})")
        except Exception:
            pass

        log("🧹 VACUUM...")
        conn.execute("VACUUM")
        conn.commit()

        total = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    finally:
        conn.close()

    # ✅ 最終 success/failed：以 ticker 集合計算，且「不灌水」
    success = int(len(ok_tickers))
    failed = int(len(set(tickers) - ok_tickers))

    log(f"📊 KR 同步完成 | 成功(有寫入):{success} 失敗(仍無資料):{failed} / {len(items)}")

    # 額外：給 overview 用的統計（你要放到第一張圖很適合）
    stats = {
        "success": success,
        "failed": failed,
        "total_requested": int(len(tickers)),
        "ok_rate": (float(success) / float(len(tickers))) if tickers else 0.0,
        "failed_sample": sorted(list(set(tickers) - ok_tickers))[:30],
    }

    return {
        "success": success,
        "total": int(total),
        "failed": failed,
        "has_changed": success > 0,
        "window": {"start": start_date, "end": end_date, "end_excl": end_excl_date, "mode": window_mode},
        "db_path": db_path,
        "calendar": {"ticker": _calendar_ticker(), "n_trading_days": n_days, "lookback_cal_days": _calendar_lookback_cal_days()},
        "batch": {"size": _batch_size(), "threads": _yf_threads_enabled(), "fallback_single": _fallback_single_enabled()},
        "stats": stats,
    }


# =============================================================================
# Snapshot builder (delegated)
# =============================================================================
def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    """
    委派給 markets/kr/snapshot_builder.py
    - locked/touch + streak10/30 + prev 都在那邊算
    """
    from .snapshot_builder import run_intraday as _run_intraday
    return _run_intraday(slot=slot, asof=asof, ymd=ymd)
