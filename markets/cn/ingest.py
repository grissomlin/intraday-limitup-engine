# -*- coding: utf-8 -*-
"""
markets/cn/ingest.py
--------------------
CN (A-share) daily-K downloader -> SQLite warehouse

✅ processor.py compatible schema:
   - stock_prices(symbol,date,open,high,low,close,volume) PK(symbol,date)
   - stock_info(symbol,name,sector,market,market_detail,updated_at)
   - download_errors(symbol,name,start_date,end_date,error,created_at)

✅ NO incremental (rolling window):
   - 每次抓「最近 N 交易日窗口」（預設 30 交易日）
   - 用交易日 proxy ticker 推算窗口起點（預設 000001.SS）
   - 先刪掉 DB 裡 window 起點之後的舊資料，再寫入最新資料（避免 DB 越長越大）

✅ stock list via akshare:
   - prefer code_name list; fallback spot_em; final fallback DB

✅ board classification:
   - main: default
   - chinext: 300/301
   - star: 688

🧪 Debug friendly CLI:
  --days / --start / --end
  --sample-n / --sample-mode (mixed/main/chinext/star)
  --symbols (comma separated)
  --no-vacuum
  --fix-sector-missing (optional): unify A-Share/NULL/''/—/-/... -> 未分類

環境變數：
- CN_DB_PATH                     預設 markets/cn/cn_stock_warehouse.db
- CN_ROLLING_TRADING_DAYS        預設 30（精準交易日窗口）
- CN_CALENDAR_TICKER             預設 000001.SS（用這個 ticker 的日K日期推交易日）
- CN_CAL_LOOKBACK_CAL_DAYS       預設 180（拉日曆用）
- CN_ROLLING_CAL_DAYS            預設 90（fallback：交易日曆失敗才用）
- CN_SLEEP_SEC                   預設 0.03（避免打爆 yfinance）
"""

from __future__ import annotations

import argparse
import os
import random
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from tqdm import tqdm


# =============================================================================
# Env / Paths
# =============================================================================
MARKET_CODE = "cn-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "cn_stock_warehouse.db")


def _db_path() -> str:
    return os.getenv("CN_DB_PATH", DEFAULT_DB_PATH)


def _rolling_trading_days() -> int:
    return int(os.getenv("CN_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    # 上證綜指 (SSE Composite) as trading-day proxy
    return os.getenv("CN_CALENDAR_TICKER", "000001.SS")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("CN_CAL_LOOKBACK_CAL_DAYS", "180"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("CN_ROLLING_CAL_DAYS", "90"))


def _sleep_sec() -> float:
    return float(os.getenv("CN_SLEEP_SEC", "0.03"))


def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


# =============================================================================
# DB init
# =============================================================================
def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_prices (
                symbol TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, date)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_info (
                symbol TEXT PRIMARY KEY,
                name TEXT,
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
                name TEXT,
                start_date TEXT,
                end_date TEXT,
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
# Trading-day window helper (exact N trading days)
# =============================================================================
def _infer_window_by_trading_days(
    end_ymd: str,
    n_trading_days: int,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    用 yfinance 的 proxy ticker 當交易日曆來源，推算最近 N 個交易日窗口。
    回傳 (start_ymd, end_ymd_inclusive, end_exclusive_ymd)

    - end_ymd_inclusive：窗口最後一天（通常是最近交易日）
    - end_exclusive_ymd：yfinance end 是 exclusive，所以要 +1 天
    """
    cal_ticker = _calendar_ticker()
    lookback = _calendar_lookback_cal_days()

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


# =============================================================================
# board classification
# =============================================================================
def _classify_cn_market(symbol: str) -> Tuple[str, str]:
    """
    symbol: 600000.SS / 000001.SZ / 300001.SZ / 688001.SS
    return (market, market_detail)
    """
    code = str(symbol).split(".")[0].zfill(6)

    if code.startswith("688"):
        return "SSE", "star"
    if code.startswith(("300", "301")):
        return "SZSE", "chinext"
    if symbol.endswith(".SS"):
        return "SSE", "main"
    if symbol.endswith(".SZ"):
        return "SZSE", "main"
    return "CN", "unknown"


def _is_main(sym: str) -> bool:
    code = sym.split(".")[0]
    return not code.startswith(("300", "301", "688"))


def _is_chinext(sym: str) -> bool:
    code = sym.split(".")[0]
    return code.startswith(("300", "301"))


def _is_star(sym: str) -> bool:
    code = sym.split(".")[0]
    return code.startswith("688")


# =============================================================================
# stock list (akshare)
# =============================================================================
def _normalize_code_name_df(df: pd.DataFrame) -> Tuple[str, str]:
    code_col = "code" if "code" in df.columns else ("代码" if "代码" in df.columns else None)
    name_col = "name" if "name" in df.columns else ("名称" if "名称" in df.columns else None)
    if not code_col or not name_col:
        raise RuntimeError(f"unexpected columns: {list(df.columns)}")
    return code_col, name_col


def get_cn_stock_list(db_path: str) -> List[Tuple[str, str]]:
    """
    return [(symbol, name), ...], symbol uses Yahoo format: .SS / .SZ
    priority:
      1) ak.stock_info_a_code_name()
      2) ak.stock_zh_a_spot_em()
      3) existing stock_info in DB
    """
    log("📡 正在獲取 A 股清單...")

    valid_prefixes = (
        "000", "001", "002", "003",
        "300", "301",
        "600", "601", "603", "605",
        "688",
    )

    # --- 1) prefer code->name list ---
    try:
        import akshare as ak  # type: ignore

        df = ak.stock_info_a_code_name()
        code_col, name_col = _normalize_code_name_df(df)

        conn = sqlite3.connect(db_path)
        stock_list: List[Tuple[str, str]] = []
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for _, row in df.iterrows():
                code = str(row.get(code_col, "")).zfill(6)
                if not code.startswith(valid_prefixes):
                    continue

                symbol = f"{code}.SS" if code.startswith("6") else f"{code}.SZ"
                market, market_detail = _classify_cn_market(symbol)
                name = str(row.get(name_col, "Unknown")).strip() or "Unknown"
                sector = "A-Share"

                conn.execute(
                    """
                    INSERT OR REPLACE INTO stock_info
                    (symbol, name, sector, market, market_detail, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (symbol, name, sector, market, market_detail, now),
                )
                stock_list.append((symbol, name))

            conn.commit()
        finally:
            conn.close()

        log(f"✅ A 股清單導入成功(code_name): {len(stock_list)} 檔")
        return stock_list

    except Exception as e:
        log(f"⚠️ code_name 清單失敗，改用 spot_em：{e}")

    # --- 2) fallback spot_em ---
    try:
        import akshare as ak  # type: ignore

        df_spot = ak.stock_zh_a_spot_em()
        code_col = "代码" if "代码" in df_spot.columns else ("code" if "code" in df_spot.columns else None)
        name_col = "名称" if "名称" in df_spot.columns else ("name" if "name" in df_spot.columns else None)
        if not code_col or not name_col:
            raise RuntimeError(f"unexpected columns: {list(df_spot.columns)}")

        conn = sqlite3.connect(db_path)
        stock_list: List[Tuple[str, str]] = []
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for _, row in df_spot.iterrows():
                code = str(row.get(code_col, "")).zfill(6)
                if not code.startswith(valid_prefixes):
                    continue

                symbol = f"{code}.SS" if code.startswith("6") else f"{code}.SZ"
                market, market_detail = _classify_cn_market(symbol)
                name = str(row.get(name_col, "Unknown")).strip() or "Unknown"
                sector = "A-Share"

                conn.execute(
                    """
                    INSERT OR REPLACE INTO stock_info
                    (symbol, name, sector, market, market_detail, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (symbol, name, sector, market, market_detail, now),
                )
                stock_list.append((symbol, name))

            conn.commit()
        finally:
            conn.close()

        log(f"✅ A 股清單導入成功(spot_em): {len(stock_list)} 檔")
        return stock_list

    except Exception as e:
        log(f"⚠️ spot_em 也失敗（將改用 DB 既有 stock_info）: {e}")

    # --- 3) fallback: use existing DB list ---
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT symbol, name FROM stock_info").fetchall()
        items = [(s, (n or "Unknown")) for s, n in rows if s]
        if items:
            log(f"✅ 使用 stock_info 既有清單: {len(items)} 檔")
            return items
    finally:
        conn.close()

    log("❌ 無可用 A 股清單（akshare 失敗且 DB 無既有名單）")
    return []


# =============================================================================
# download one symbol (full window)
# =============================================================================
def download_one_cn(symbol: str, start_date: str, end_date_exclusive: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    returns (df, err)
    df columns: symbol,date,open,high,low,close,volume
    NOTE: yfinance end is exclusive.
    """
    max_retries = 2
    last_err: Optional[str] = None

    for attempt in range(max_retries + 1):
        try:
            df = yf.download(
                symbol,
                start=start_date,
                end=end_date_exclusive,
                progress=False,
                timeout=25,
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
            msg = str(e)
            last_err = f"exception: {msg}"
            if attempt < max_retries:
                time.sleep(2.0)
                continue
            return None, last_err

    return None, last_err or "unknown"


# =============================================================================
# optional: fix sector missing/bad -> 未分類
# =============================================================================
def fix_sector_missing(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            """
            UPDATE stock_info
            SET sector='未分類'
            WHERE sector IS NULL
               OR TRIM(sector)=''
               OR sector IN ('A-Share','—','-','--','－','–')
            """
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


# =============================================================================
# main ingest (rolling window, NO incremental)
# =============================================================================
def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,  # end_inclusive input; internally we pass end_exclusive to yfinance
    *,
    sample_n: int = 0,
    sample_mode: str = "mixed",
    symbols: Optional[List[str]] = None,
    vacuum: bool = True,
    fix_sector: bool = False,
) -> Dict[str, object]:
    """
    NO incremental: 每次抓 rolling window（預設近 30 交易日）
    - start_date / end_date 若都沒給：用交易日窗口自動推算
    - 若你硬給 start/end：就照你給的走（end 視為 inclusive，內部會轉成 exclusive）
    """
    t0 = time.time()
    db_path = _db_path()
    init_db(db_path)

    # 1) decide window
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    n_days = _rolling_trading_days()
    start_td, end_incl, end_excl = _infer_window_by_trading_days(end_date, n_days)

    if (not start_date) and start_td and end_incl and end_excl:
        start_date = start_td
        end_date = end_incl
        end_excl_date = end_excl
        mode = "trading_days"
        log(f"📅 Trading-day window OK | last {n_days} trading days | {start_date} ~ {end_date} (end_excl={end_excl_date})")
    else:
        # fallback: cal-days (or user-specified)
        if not start_date:
            start_date = (datetime.now() - timedelta(days=_fallback_rolling_cal_days())).strftime("%Y-%m-%d")
        end_excl_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
        mode = "cal_days" if (not start_td) else "manual"
        log(f"⚠️ Using cal-days/manual window | {start_date} ~ {end_date} (end_excl={end_excl_date})")

    log(f"📦 CN DB = {db_path}")
    log(f"🚀 啟動 A 股同步 | window({mode}): {start_date} ~ {end_date}")

    # 2) get list
    if symbols:
        items = [(s.strip(), "Unknown") for s in symbols if str(s).strip()]
        log(f"🧪 指定 symbols 模式：{len(items)} 檔")
    else:
        items = get_cn_stock_list(db_path)

    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False}

    # 3) sample
    if sample_n and sample_n > 0:
        mode_s = (sample_mode or "mixed").lower().strip()

        main_items = [(s, n) for s, n in items if _is_main(s)]
        chinext_items = [(s, n) for s, n in items if _is_chinext(s)]
        star_items = [(s, n) for s, n in items if _is_star(s)]

        if mode_s == "main":
            items = random.sample(main_items, min(sample_n, len(main_items)))
        elif mode_s == "chinext":
            items = random.sample(chinext_items, min(sample_n, len(chinext_items)))
        elif mode_s == "star":
            items = random.sample(star_items, min(sample_n, len(star_items)))
        else:
            k = max(1, sample_n // 3)
            pick_main = random.sample(main_items, min(k, len(main_items)))
            pick_chi = random.sample(chinext_items, min(k, len(chinext_items)))
            pick_star = random.sample(star_items, min(k, len(star_items)))
            items = pick_main + pick_chi + pick_star

        log(f"🧪 SAMPLE MODE: {mode_s} | symbols={len(items)}")

    # 4) rolling delete + download
    success_count = 0
    fail_count = 0

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        # ✅ rolling window：先刪掉 window 起點之後的舊資料（避免 DB 疊加）
        conn.execute("DELETE FROM stock_prices WHERE date >= ?", (start_date,))
        conn.commit()

        pbar = tqdm(items, desc="CN同步", unit="檔")
        for symbol, name in pbar:
            df_res, err = download_one_cn(symbol, start_date, end_excl_date)

            if df_res is not None and not df_res.empty:
                df_res.to_sql(
                    "stock_prices",
                    conn,
                    if_exists="append",
                    index=False,
                    method=lambda table, conn2, keys, data_iter: conn2.executemany(
                        f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})",
                        data_iter,
                    ),
                )
                success_count += 1
            else:
                fail_count += 1
                if err:
                    try:
                        conn.execute(
                            """
                            INSERT INTO download_errors
                            (symbol, name, start_date, end_date, error, created_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (symbol, name, start_date, end_date, err, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                        )
                    except Exception:
                        pass

            time.sleep(_sleep_sec())

        conn.commit()

        if vacuum:
            log("🧹 執行資料庫 VACUUM...")
            conn.execute("VACUUM")
            conn.commit()

        if fix_sector:
            affected = fix_sector_missing(db_path)
            log(f"🏷️ sector 缺失/壞值 → 未分類：{affected} 筆")

        db_info_cnt = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    finally:
        conn.close()

    mins = (time.time() - t0) / 60.0
    log(f"📊 A 股同步完成 | 成功:{success_count} 失敗:{fail_count} / {len(items)} | {mins:.1f} 分鐘")

    return {
        "success": success_count,
        "total": db_info_cnt,
        "failed": fail_count,
        "has_changed": success_count > 0,
        "window": {"start": start_date, "end": end_date, "end_excl": end_excl_date, "mode": mode},
        "db_path": db_path,
        "calendar": {"ticker": _calendar_ticker(), "n_trading_days": int(n_days), "lookback_cal_days": _calendar_lookback_cal_days()},
    }


def _parse_args():
    ap = argparse.ArgumentParser(description="CN ingest (yfinance -> sqlite) rolling window (NO incremental)")
    ap.add_argument("--start", default="", help="start date YYYY-MM-DD (override auto window)")
    ap.add_argument("--end", default="", help="end date YYYY-MM-DD (inclusive, override auto window)")
    ap.add_argument("--days", type=int, default=0, help="shortcut: last N calendar days (debug only)")
    ap.add_argument("--sample-n", type=int, default=0, help="sample N symbols only")
    ap.add_argument("--sample-mode", default="mixed", choices=["mixed", "main", "chinext", "star"], help="sample pool")
    ap.add_argument("--symbols", default="", help="comma separated symbols, e.g. 600000.SS,000001.SZ,300001.SZ,688001.SS")
    ap.add_argument("--no-vacuum", action="store_true", help="skip VACUUM (faster for testing)")
    ap.add_argument("--fix-sector-missing", action="store_true", help="set A-Share/NULL/''/—/-/... -> 未分類")
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    # CLI debug:
    # - If --days is given, use calendar-days (still fine; run_sync will treat as manual)
    if args.days and args.days > 0:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=int(args.days))).strftime("%Y-%m-%d")
    else:
        start = args.start.strip() or None
        end = args.end.strip() or None

    sym_list: List[str] = []
    if args.symbols.strip():
        sym_list = [s.strip() for s in args.symbols.split(",") if s.strip()]

    run_sync(
        start_date=start,
        end_date=end,
        sample_n=int(args.sample_n or 0),
        sample_mode=str(args.sample_mode or "mixed"),
        symbols=sym_list or None,
        vacuum=(not args.no_vacuum),
        fix_sector=bool(args.fix_sector_missing),
    )
