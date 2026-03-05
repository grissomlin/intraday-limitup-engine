# markets/fr/fr_snapshot.py
# -*- coding: utf-8 -*-
"""
France (Euronext Paris) DB -> raw snapshot builder (open movers, no daily limit)
(Adapted from markets/uk/uk_snapshot.py + markets/uk/uk_prices.py style)

Goals:
- Use a master CSV list (FR_STOCKLIST / FR_MASTER_CSV_PATH) to build stock_info
- Download rolling window prices into sqlite (run_sync)
- Build snapshot_open + peers_by_sector from local DB (run_intraday)
- Apply penny/illiquid/noise filters via ENV (in builders stage; snapshot keeps raw rows)

ENV (list):
- FR_STOCKLIST                (preferred) path to FR master CSV (e.g. FR_Stock_Master_Data.csv)
- FR_MASTER_CSV_PATH          (fallback)  same as above

Optional auto-fetch from Google Drive (like india_list.py):
- FR_MASTER_CSV_AUTO_FETCH          (default 1)
- token:  GDRIVE_TOKEN_B64 / GDRIVE_TOKEN_JSON_B64 / GDRIVE_TOKEN
- folder: GDRIVE_FOLDER_ID / FR_STOCKLIST / GDRIVE_ROOT_FOLDER_ID / GDRIVE_PARENT_ID
- drive filename: FR_MASTER_CSV_DRIVE_NAME / (default: FR_Stock_Master_Data.csv)

ENV (DB + sync):
- FR_DB_PATH                    (default: markets/fr/fr_stock_warehouse.db)
- FR_ROLLING_TRADING_DAYS       (default 30)
- FR_CALENDAR_TICKER            (default ^FCHI)
- FR_CAL_LOOKBACK_CAL_DAYS      (default 180)
- FR_ROLLING_CAL_DAYS           (default 90)
- FR_DAILY_BATCH_SIZE           (default 200)
- FR_BATCH_SLEEP_SEC            (default 0.05)
- FR_FALLBACK_SINGLE            (default 1)
- FR_YF_THREADS                 (default 1)
- FR_SLEEP_SEC                  (default 0.02)

ENV (snapshot thresholds):
- FR_RET_TH                     (default 0.10)
- FR_TOUCH_TH                   (default 0.10)
- FR_ROWS_PER_BOX               (default 6)
- FR_PEER_EXTRA_PAGES           (default 1)
- FR_STREAK_LOOKBACK_ROWS       (default 90)
- FR_BADGE_FALLBACK_LANG        (default en)

NOTE:
- Penny/volume/tick filtering is applied in builders_fr.py (watchlist builder),
  so snapshot remains complete for debug/peer pages if you want.
"""

from __future__ import annotations

import base64
import io
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Optional imports (render helpers)
# -----------------------------------------------------------------------------
try:
    from scripts.render_images_common.move_bands import move_badge  # (band, key)
except Exception:

    def move_badge(ret: float) -> Tuple[int, str]:
        try:
            r = float(ret)
        except Exception:
            return -1, ""
        if r >= 1.00:
            return 5, "move_band_5"
        if r >= 0.50:
            return 4, "move_band_4"
        if r >= 0.40:
            return 3, "move_band_3"
        if r >= 0.30:
            return 2, "move_band_2"
        if r >= 0.20:
            return 1, "move_band_1"
        if r >= 0.10:
            return 0, "move_band_0"
        return -1, ""


try:
    from scripts.render_images_common.i18n import t as _t  # type: ignore
except Exception:

    def _t(lang: str, key: str, default: str = "", **kwargs: Any) -> str:
        try:
            return (default or key).format(**kwargs)
        except Exception:
            return default or key


# =============================================================================
# Logging
# =============================================================================
def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


# =============================================================================
# ENV helpers
# =============================================================================
def _env_bool(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _parse_bool_env(name: str, default: bool) -> bool:
    v = (os.getenv(name, "").strip() or ("1" if default else "0")).lower()
    return v in ("1", "true", "yes", "y", "on")


def _get_first_env(*names: str) -> Tuple[str, str]:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v, n
    return "", ""


# =============================================================================
# Config (snapshot)
# =============================================================================
FR_RET_TH = float(os.getenv("FR_RET_TH", "0.10"))
FR_TOUCH_TH = float(os.getenv("FR_TOUCH_TH", "0.10"))
FR_ROWS_PER_BOX = int(os.getenv("FR_ROWS_PER_BOX", "6"))
FR_PEER_EXTRA_PAGES = int(os.getenv("FR_PEER_EXTRA_PAGES", "1"))
FR_STREAK_LOOKBACK_ROWS = int(os.getenv("FR_STREAK_LOOKBACK_ROWS", "90"))
FR_BADGE_FALLBACK_LANG = (os.getenv("FR_BADGE_FALLBACK_LANG", "en") or "en").strip().lower()


# =============================================================================
# Config (sync)
# =============================================================================
def _db_path() -> str:
    return os.getenv("FR_DB_PATH", os.path.join(os.path.dirname(__file__), "fr_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("FR_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    return os.getenv("FR_CALENDAR_TICKER", "^FCHI")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("FR_CAL_LOOKBACK_CAL_DAYS", "180"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("FR_ROLLING_CAL_DAYS", "90"))


def _batch_size() -> int:
    return int(os.getenv("FR_DAILY_BATCH_SIZE", "200"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("FR_BATCH_SLEEP_SEC", "0.05"))


def _fallback_single_enabled() -> bool:
    return str(os.getenv("FR_FALLBACK_SINGLE", "1")).strip() == "1"


def _yf_threads_enabled() -> bool:
    return str(os.getenv("FR_YF_THREADS", "1")).strip() == "1"


def _single_sleep_sec() -> float:
    return float(os.getenv("FR_SLEEP_SEC", "0.02"))


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
                local_symbol TEXT,
                name   TEXT,
                sector TEXT,
                industry TEXT,
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
# Master CSV (local + optional Drive fetch)
# =============================================================================
def _master_csv_path() -> str:
    # You said you already have FR_STOCKLIST used; keep it as preferred.
    return (
        (os.getenv("FR_STOCKLIST") or "").strip()
        or (os.getenv("FR_MASTER_CSV_PATH") or "").strip()
    )


def _get_drive_service_from_token_b64(token_b64: str):
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    decoded = base64.b64decode(token_b64).decode("utf-8")
    token_info = json.loads(decoded)

    creds = Credentials.from_authorized_user_info(token_info)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def _find_file_id_in_folder(service, folder_id: str, file_name: str) -> Optional[str]:
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    res = (
        service.files()
        .list(
            q=query,
            fields="files(id, name, modifiedTime)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = res.get("files", []) or []
    if not files:
        return None
    files.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return files[0]["id"]


def _download_drive_file(service, file_id: str, out_path: Path) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    out_path.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)

    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _status, done = downloader.next_chunk()


def _maybe_fetch_master_csv_from_drive(local_path: str) -> bool:
    """
    Download master CSV from Drive if missing.

    Env it will try:
      token:  GDRIVE_TOKEN_B64 / GDRIVE_TOKEN_JSON_B64 / GDRIVE_TOKEN
      folder: GDRIVE_FOLDER_ID / FR_STOCKLIST / GDRIVE_ROOT_FOLDER_ID / GDRIVE_PARENT_ID
      drive filename: FR_MASTER_CSV_DRIVE_NAME / (default: FR_Stock_Master_Data.csv)
    """
    if not _parse_bool_env("FR_MASTER_CSV_AUTO_FETCH", True):
        return False

    token_b64, token_key = _get_first_env("GDRIVE_TOKEN_B64", "GDRIVE_TOKEN_JSON_B64", "GDRIVE_TOKEN")
    folder_id, folder_key = _get_first_env("GDRIVE_FOLDER_ID", "FR_STOCKLIST", "GDRIVE_ROOT_FOLDER_ID", "GDRIVE_PARENT_ID")

    if not token_b64 or not folder_id:
        miss = []
        if not token_b64:
            miss.append("token(GDRIVE_TOKEN_B64/GDRIVE_TOKEN_JSON_B64/GDRIVE_TOKEN)")
        if not folder_id:
            miss.append("folder(GDRIVE_FOLDER_ID/FR_STOCKLIST/GDRIVE_ROOT_FOLDER_ID/GDRIVE_PARENT_ID)")
        log(f"⚠️ Drive fetch skipped: missing {', '.join(miss)}")
        return False

    drive_name = (os.getenv("FR_MASTER_CSV_DRIVE_NAME") or "FR_Stock_Master_Data.csv").strip() or "FR_Stock_Master_Data.csv"

    try:
        log(f"☁️ master CSV missing; try fetch from Drive | folder={folder_id}({folder_key}) name={drive_name} | token={token_key}")
        svc = _get_drive_service_from_token_b64(token_b64)

        file_id = _find_file_id_in_folder(svc, folder_id, drive_name)
        if not file_id:
            log(f"⚠️ Drive fetch skipped: file not found | folder={folder_id}({folder_key}) name={drive_name}")
            return False

        out_path = Path(local_path)
        _download_drive_file(svc, file_id, out_path)
        log(f"✅ Drive fetched: {drive_name} (fileId={file_id}) -> {out_path}")
        return True
    except Exception as e:
        log(f"⚠️ Drive fetch failed: {e}")
        return False


def _load_master_csv(path: str) -> pd.DataFrame:
    if not path:
        raise FileNotFoundError("FR master CSV path is empty. Set FR_STOCKLIST (preferred) or FR_MASTER_CSV_PATH.")

    if not os.path.exists(path):
        _maybe_fetch_master_csv_from_drive(path)

    if not os.path.exists(path):
        raise FileNotFoundError(f"FR master CSV not found: {path} (set FR_STOCKLIST or FR_MASTER_CSV_PATH)")

    df = pd.read_csv(path)
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _norm_col(df: pd.DataFrame, name: str, aliases: List[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    if name.lower() in cols:
        return cols[name.lower()]
    for a in aliases:
        if a.lower() in cols:
            return cols[a.lower()]
    return ""


def _coerce_symbol(x: Any) -> str:
    s = ("" if x is None else str(x)).strip().upper()
    if s in ("", "NAN", "NONE", "-", "—", "--"):
        return ""
    return s


def _coerce_text(x: Any, default: str = "") -> str:
    s = ("" if x is None else str(x)).strip()
    return s if s else default


# =============================================================================
# Calendar helpers (trading day window)
# =============================================================================
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
        return (
            start_incl.strftime("%Y-%m-%d"),
            end_incl.strftime("%Y-%m-%d"),
            end_excl.strftime("%Y-%m-%d"),
        )
    except Exception:
        return None, None, None


# =============================================================================
# List -> DB stock_info
# =============================================================================
def refresh_stock_info_from_master(db_path: str, refresh_list: bool = True) -> List[Tuple[str, str]]:
    """
    Returns [(yf_symbol, name), ...] and writes stock_info.
    If refresh_list=False and DB already has FR stock_info, use DB.
    """
    init_db(db_path)

    if (not refresh_list) and os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            df0 = pd.read_sql_query("SELECT symbol, name FROM stock_info WHERE market='FR'", conn)
            if not df0.empty:
                items = [(str(r["symbol"]), str(r["name"])) for _, r in df0.iterrows() if str(r["symbol"]).strip()]
                log(f"✅ 使用 DB stock_info 既有 FR 清單: {len(items)} 檔")
                return items
        finally:
            conn.close()

    path = _master_csv_path()
    log(f"📡 同步法國 FR 名單 (master_csv) path={path}")
    df = _load_master_csv(path)

    # Accept many column variants
    c_yf = _norm_col(df, "yf_symbol", ["yf_ticker", "yf", "ticker_yf"])
    c_sym = _norm_col(df, "symbol", ["local_symbol", "ticker", "code"])
    c_name = _norm_col(df, "company_name", ["name", "issuer name", "issuer_name", "company"])
    c_sector = _norm_col(df, "sector", ["Sector", "icb sector", "gics sector"])
    c_ind = _norm_col(df, "industry", ["Industry", "subindustry", "sub_industry", "icb industry"])

    if not c_yf:
        raise RuntimeError(f"FR master CSV missing yf symbol column. Columns={list(df.columns)}")

    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    items: List[Tuple[str, str]] = []

    conn = sqlite3.connect(db_path)
    try:
        for _, r in df.iterrows():
            yf_symbol = _coerce_symbol(r.get(c_yf))
            if not yf_symbol:
                continue

            local_symbol = _coerce_symbol(r.get(c_sym)) if c_sym else ""
            name = _coerce_text(r.get(c_name), default=yf_symbol) if c_name else yf_symbol
            sector = _coerce_text(r.get(c_sector), default="Unknown") if c_sector else "Unknown"
            industry = _coerce_text(r.get(c_ind), default="Unknown") if c_ind else "Unknown"

            md = "EURONEXT_PARIS|src=master_csv"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, local_symbol, name, sector, industry, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (yf_symbol, local_symbol, name, sector, industry, "FR", md, now_s),
            )
            items.append((yf_symbol, name))

        conn.commit()
    finally:
        conn.close()

    log(f"✅ FR 名單同步完成：共 {len(items)} 檔")
    return items


# =============================================================================
# Download core
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
        # MultiIndex layout could be ('Open','MC.PA') or ('MC.PA','Open')
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
    FR rolling-window sync:
    - end_date 若未給：用 calendar ticker 最新交易日當 end_inclusive，再推 end_excl
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
    items = refresh_stock_info_from_master(db_path, refresh_list=refresh_list)
    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False, "db_path": db_path}

    tickers = [s for s, _ in items if s]
    name_map = {s: (n or "Unknown") for s, n in items if s}
    total = len(tickers)

    log(f"📦 FR DB = {db_path}")
    log(f"🚀 FR run_sync | window: {start_ymd} ~ {end_inclusive} | refresh_list={refresh_list}")
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
    pbar = tqdm(batches, desc="FR批次同步", unit="batch")

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
    log(f"📊 FR 同步完成 | 成功:{success} 失敗:{failed} / {total}")

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


# =============================================================================
# Snapshot builder helpers
# =============================================================================
def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    return row[0] if row and row[0] else None


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b if b > 0 else 0


def _compute_streaks(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.sort_values(["symbol", "ymd"]).reset_index(drop=True)
    df["hit_10_close"] = df["hit_10_close"].fillna(False).astype(bool)

    df["g"] = (~df["hit_10_close"]).groupby(df["symbol"]).cumsum()

    df["streak"] = 0
    hit_rows = df["hit_10_close"]
    df.loc[hit_rows, "streak"] = (
        df.loc[hit_rows, "hit_10_close"]
        .astype(int)
        .groupby([df.loc[hit_rows, "symbol"], df.loc[hit_rows, "g"]])
        .cumsum()
    )

    df["hit_prev"] = df.groupby("symbol")["hit_10_close"].shift(1).fillna(False).astype(int)
    df["streak_prev"] = df.groupby("symbol")["streak"].shift(1).fillna(0).astype(int)

    return df.drop(columns=["g"], errors="ignore")


# =============================================================================
# Public API: run_intraday
# =============================================================================
def run_intraday(slot: str, asof: str, ymd: str, db_path: Optional[Path] = None) -> Dict[str, Any]:
    dbp = db_path or Path(_db_path())
    if isinstance(dbp, str):
        dbp = Path(dbp)

    if not dbp.exists():
        raise FileNotFoundError(f"FR DB not found: {dbp} (set FR_DB_PATH to override)")

    conn = sqlite3.connect(str(dbp))
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

        # Load recent rows per symbol (for streak), prev_close via LAG(close)
        sql = """
        WITH base AS (
          SELECT
            sp.symbol,
            sp.date AS ymd,
            sp.open, sp.high, sp.low, sp.close, sp.volume,
            LAG(sp.close) OVER (PARTITION BY sp.symbol ORDER BY sp.date) AS prev_close,
            i.name,
            i.sector,
            i.market_detail,
            ROW_NUMBER() OVER (PARTITION BY sp.symbol ORDER BY sp.date DESC) AS rn
          FROM stock_prices sp
          JOIN stock_info i ON i.symbol = sp.symbol
          WHERE i.market='FR' AND sp.date <= ?
        )
        SELECT
          symbol, ymd, open, high, low, close, volume, prev_close,
          name, sector, market_detail
        FROM base
        WHERE rn <= ?
        """
        df = pd.read_sql_query(sql, conn, params=(ymd_effective, int(FR_STREAK_LOOKBACK_ROWS)))
    finally:
        conn.close()

    # time meta: for FR we keep it simple; main.py already adds unified build_market_time_meta.
    time_meta: Dict[str, Any] = {}

    if df.empty:
        return {
            "market": "fr",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filters": {
                "enable_open_watchlist": True,
                "note": "FR treated as open_limit (no daily limit). Built from local DB.",
                "ret_th": FR_RET_TH,
                "touch_th": FR_TOUCH_TH,
                "rows_per_box": FR_ROWS_PER_BOX,
                "peer_extra_pages": FR_PEER_EXTRA_PAGES,
                "streak_lookback_rows": FR_STREAK_LOOKBACK_ROWS,
            },
            "stats": {"snapshot_main_count": 0, "snapshot_open_count": 0},
            "snapshot_main": [],
            "snapshot_open": [],
            "peers_by_sector": {},
            "peers_not_limitup": [],
            "errors": [{"reason": "no_rows_for_ymd_effective"}],
            "meta": {"db_path": str(dbp), "ymd_effective": ymd_effective, "time": time_meta},
        }

    # normalize fields
    df["name"] = df["name"].fillna("Unknown")
    df["sector"] = df["sector"].fillna("Unknown").replace("", "Unknown")
    df["market_detail"] = df["market_detail"].fillna("Unknown")

    for col in ("prev_close", "open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0).astype(int)

    # Drop rows with no prev_close or no close (cannot compute ret)
    m = df["prev_close"].notna() & (df["prev_close"] > 0) & df["close"].notna()
    skipped_no_prev = int((~m).sum())
    df = df[m].copy()
    if df.empty:
        return {
            "market": "fr",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filters": {
                "enable_open_watchlist": True,
                "note": "FR treated as open_limit (no daily limit). Built from local DB.",
                "ret_th": FR_RET_TH,
                "touch_th": FR_TOUCH_TH,
                "rows_per_box": FR_ROWS_PER_BOX,
                "peer_extra_pages": FR_PEER_EXTRA_PAGES,
                "streak_lookback_rows": FR_STREAK_LOOKBACK_ROWS,
            },
            "stats": {"snapshot_main_count": 0, "snapshot_open_count": 0, "snapshot_open_skipped_no_prev": skipped_no_prev},
            "snapshot_main": [],
            "snapshot_open": [],
            "peers_by_sector": {},
            "peers_not_limitup": [],
            "errors": [{"reason": "all_rows_missing_prev_close_or_close"}],
            "meta": {"db_path": str(dbp), "ymd_effective": ymd_effective, "time": time_meta},
        }

    # ret / touch_ret
    df["ret"] = (df["close"] / df["prev_close"]) - 1.0
    df["touch_ret"] = (df["high"] / df["prev_close"]) - 1.0

    df["touched_10"] = df["touch_ret"].notna() & (df["touch_ret"] >= FR_TOUCH_TH)
    df["hit_10_close"] = df["ret"].notna() & (df["ret"] >= FR_RET_TH)
    df["touched_only"] = df["touched_10"] & (~df["hit_10_close"])

    # streak
    df = _compute_streaks(df)

    # move band/key
    badges = df["ret"].fillna(0.0).apply(lambda x: move_badge(float(x)))
    df["move_band"] = badges.apply(lambda t: int(t[0]) if t and len(t) >= 1 else -1)
    df["move_key"] = badges.apply(lambda t: str(t[1]) if t and len(t) >= 2 else "")

    # backward compatible
    df["badge_level"] = df["move_band"].where(df["move_band"] >= 0, 0).astype(int)
    df["badge_text"] = df["move_key"].apply(lambda k: _t(FR_BADGE_FALLBACK_LANG, k, default="") if k else "")

    # select day rows
    df_day = df[df["ymd"].astype(str) == str(ymd_effective)].copy()
    if df_day.empty:
        return {
            "market": "fr",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filters": {
                "enable_open_watchlist": True,
                "note": "FR treated as open_limit (no daily limit). Built from local DB.",
                "ret_th": FR_RET_TH,
                "touch_th": FR_TOUCH_TH,
                "rows_per_box": FR_ROWS_PER_BOX,
                "peer_extra_pages": FR_PEER_EXTRA_PAGES,
                "streak_lookback_rows": FR_STREAK_LOOKBACK_ROWS,
            },
            "stats": {"snapshot_main_count": 0, "snapshot_open_count": 0, "snapshot_open_skipped_no_prev": skipped_no_prev},
            "snapshot_main": [],
            "snapshot_open": [],
            "peers_by_sector": {},
            "peers_not_limitup": [],
            "errors": [{"reason": "no_rows_for_ymd_effective_after_history_load"}],
            "meta": {"db_path": str(dbp), "ymd_effective": ymd_effective, "time": time_meta},
        }

    # snapshot_open rows
    snapshot_open: List[Dict[str, Any]] = []
    for _, r in df_day.iterrows():
        prev_close = float(r["prev_close"] or 0.0)
        close = float(r["close"] or 0.0)
        ret = float(r.get("ret") or 0.0)

        streak = int(r.get("streak") or 0)
        streak_prev = int(r.get("streak_prev") or 0)
        hit_prev = int(r.get("hit_prev") or 0)

        touched_only = bool(r.get("touched_only") or False)
        hit_today = bool(r.get("hit_10_close") or False)

        parts: List[str] = []
        if touched_only:
            parts.append(f"touched ≥{int(FR_TOUCH_TH * 100)}% (close < {int(FR_RET_TH * 100)}%)")
            parts.append(f"prev close < {int(FR_RET_TH * 100)}%")
        elif hit_today and hit_prev == 1:
            parts.append(f"{int(FR_RET_TH * 100)}%+ streak: {streak}")
            parts.append(f"prev streak: {streak_prev}")
        elif hit_today and hit_prev == 0:
            parts.append(f"close ≥{int(FR_RET_TH * 100)}%")
            parts.append("prev not hit")

        status_text = " | ".join(parts)

        snapshot_open.append(
            {
                "symbol": str(r["symbol"]),
                "name": str(r["name"]),
                "sector": str(r["sector"]),
                "market": "FR",
                "market_detail": str(r.get("market_detail") or "Unknown"),
                "market_label": str(r.get("market_detail") or "Unknown"),
                "bar_date": str(r["ymd"]),
                "prev_close": prev_close,
                "open": float(r.get("open") or 0.0),
                "high": float(r.get("high") or 0.0),
                "low": float(r.get("low") or 0.0),
                "close": close,
                "volume": int(r.get("volume") or 0),
                "ret": ret,
                "touch_ret": float(r.get("touch_ret") or 0.0),
                "touched_only": bool(touched_only),
                "streak": int(streak),
                "streak_prev": int(streak_prev),
                "hit_prev": int(hit_prev),
                # ✅ new preferred
                "move_band": int(r.get("move_band") if r.get("move_band") is not None else -1),
                "move_key": str(r.get("move_key") or ""),
                # ✅ old
                "badge_text": str(r.get("badge_text") or ""),
                "badge_level": int(r.get("badge_level") or 0),
                "limit_type": "open_limit",
                "status_text": status_text,
            }
        )

    # peers (same as UK open-limit style)
    df_sort = df_day.copy()
    df_sort["ret_sort"] = df_sort["ret"].fillna(-999.0)
    df_sort["touch_sort"] = df_sort["touch_ret"].fillna(-999.0)

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    movers_cnt = df_sort[df_sort["hit_10_close"]].groupby("sector").size().to_dict()

    df_peers = df_sort[~df_sort["hit_10_close"]].copy()

    for sector, g in df_peers.groupby("sector"):
        mover_n = int(movers_cnt.get(sector, 0))
        mover_pages = max(1, _ceil_div(mover_n, FR_ROWS_PER_BOX))
        peer_cap = FR_ROWS_PER_BOX * (mover_pages + max(0, FR_PEER_EXTRA_PAGES))

        g2 = g.sort_values(["ret_sort", "touch_sort"], ascending=[False, False]).head(peer_cap)

        rows: List[Dict[str, Any]] = []
        for _, rr in g2.iterrows():
            rows.append(
                {
                    "symbol": str(rr["symbol"]),
                    "name": str(rr["name"]),
                    "sector": str(rr["sector"]),
                    "market": "FR",
                    "market_detail": str(rr.get("market_detail") or "Unknown"),
                    "market_label": str(rr.get("market_detail") or "Unknown"),
                    "bar_date": str(rr["ymd"]),
                    "prev_close": float(rr["prev_close"] or 0.0),
                    "open": float(rr.get("open") or 0.0),
                    "high": float(rr.get("high") or 0.0),
                    "low": float(rr.get("low") or 0.0),
                    "close": float(rr.get("close") or 0.0),
                    "volume": int(rr.get("volume") or 0),
                    "ret": float(rr.get("ret") or 0.0),
                    "touch_ret": float(rr.get("touch_ret") or 0.0),
                    "touched_only": bool(rr.get("touched_only") or False),
                    "streak": int(rr.get("streak") or 0),
                    "streak_prev": int(rr.get("streak_prev") or 0),
                    "hit_prev": int(rr.get("hit_prev") or 0),
                    "move_band": int(rr.get("move_band") if rr.get("move_band") is not None else -1),
                    "move_key": str(rr.get("move_key") or ""),
                    "badge_text": str(rr.get("badge_text") or ""),
                    "badge_level": int(rr.get("badge_level") or 0),
                    "limit_type": "open_limit",
                    "status_text": "",
                }
            )

        peers_by_sector[str(sector)] = rows

    peers_not_limitup: List[Dict[str, Any]] = []
    for _, rows in peers_by_sector.items():
        peers_not_limitup.extend(rows)

    return {
        "market": "fr",
        "slot": slot,
        "asof": asof,
        "ymd": ymd,
        "ymd_effective": ymd_effective,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "filters": {
            "enable_open_watchlist": True,
            "note": "FR treated as open_limit (no daily limit). Built from local DB.",
            "ret_th": FR_RET_TH,
            "touch_th": FR_TOUCH_TH,
            "rows_per_box": FR_ROWS_PER_BOX,
            "peer_extra_pages": FR_PEER_EXTRA_PAGES,
            "badge_fallback_lang": FR_BADGE_FALLBACK_LANG,
            "streak_lookback_rows": int(FR_STREAK_LOOKBACK_ROWS),
        },
        "stats": {
            "snapshot_main_count": 0,
            "snapshot_open_count": len(snapshot_open),
            "snapshot_open_skipped_no_prev": skipped_no_prev,
            "peers_sectors": int(len(peers_by_sector)),
            "peers_flat_count": int(len(peers_not_limitup)),
        },
        "snapshot_main": [],
        "snapshot_open": snapshot_open,
        "peers_by_sector": peers_by_sector,
        "peers_not_limitup": peers_not_limitup,
        "errors": [],
        "meta": {"db_path": str(dbp), "ymd_effective": ymd_effective, "time": time_meta},
    }


if __name__ == "__main__":
    # quick local test
    res_sync = run_sync(refresh_list=True)
    res = run_intraday(slot="midday", asof=datetime.now().strftime("%H:%M"), ymd=datetime.now().strftime("%Y-%m-%d"))
    print("sync:", {k: res_sync.get(k) for k in ["success", "total", "failed", "db_path"]})
    print("snapshot_open =", len(res.get("snapshot_open") or []))
