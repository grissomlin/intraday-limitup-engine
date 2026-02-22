# markets/uk/uk_list.py
# -*- coding: utf-8 -*-
"""
UK stock list fetch (LSE Instrument list Excel) - adapted from markets/us/us_list.py

✅ 目標：GitHub / Server 跑，預設直接用 URL 下載，不依賴本地檔。

流程：
- 下載 LSE Instrument list_75.xlsx
- 預設讀取 "1.1 Shares"
- 自動偵測 header row（欄名在第 9 行附近）
- 過濾：預設只保留 MiFIR Identifier Code == "SHRS"（普通股）
- 寫入 DB 的 stock_info（沿用 US schema）
- 回傳 [(symbol, name), ...]

環境變數：
- UK_LIST_URL                (default: LSE Instrument list_75.xlsx)
- UK_SHEET_NAME              (default: "1.1 Shares")  # 也可設成 "1.0 All Equity"
- UK_HEADER_SCAN_ROWS        (default: 30)
- UK_HEADER_KEY              (default: "TIDM")
- UK_REQUIRE_MIFIR_SHRS      (default: 1)

Yahoo Finance / yfinance ticker mapping:
- UK_TICKER_SUFFIX           (default: ".L")
- UK_KEEP_RAW_TICKER         (default: 0)  # 1 = 不做任何轉換
- UK_MAP_DOT_CLASS_TO_DASH   (default: 1)  # BP.A -> BP-A.L
- UK_MAP_TRAILING_DOT        (default: 1)  # RE. -> RE.L
- UK_LIMIT_SYMBOLS           (default: 0)

可選：
- UK_LIST_XLSX_PATH          (optional，本地 Excel 路徑；若有設才會用，否則一律用 URL)
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests

from markets.us.us_db import init_db
from markets.us.us_config import TICKER_RE, EXCLUDE_NAME_RE, log


DEFAULT_UK_LIST_URL = "https://docs.londonstockexchange.com/sites/default/files/reports/Instrument%20list_75.xlsx"
DEFAULT_SHEET_NAME = "1.1 Shares"
DEFAULT_HEADER_KEY = "TIDM"


# =============================================================================
# Helpers
# =============================================================================
def _safe_str(x: object, default: str = "") -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
    except Exception:
        pass
    s = str(x).strip()
    return s if s else default


def _env_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "y", "on")


def _suffix() -> str:
    return (os.getenv("UK_TICKER_SUFFIX", ".L") or ".L").strip() or ".L"


def _normalize_for_yf(sym: str) -> str:
    """
    Convert LSE TIDM -> yfinance ticker:

    - "RE."  -> "RE.L" (strip trailing dot then add suffix)
    - "QQ."  -> "QQ.L"
    - "BP.A" -> "BP-A.L" (share class with dot -> dash, then add suffix)
    - already "XXX.L" -> keep
    """
    sym = (sym or "").strip().upper()
    if not sym:
        return sym

    if _env_bool("UK_KEEP_RAW_TICKER", "0"):
        return sym

    suf = _suffix()

    # already has yfinance suffix
    if sym.endswith(suf.upper()):
        return sym

    # case 1: trailing dot: "RE."
    if _env_bool("UK_MAP_TRAILING_DOT", "1") and sym.endswith("."):
        base = sym[:-1].strip()
        if base:
            return f"{base}{suf}"

    # case 2: share class "BP.A" / "BT.A" etc
    if _env_bool("UK_MAP_DOT_CLASS_TO_DASH", "1") and "." in sym:
        # Convert dot to dash, then suffix
        # BP.A -> BP-A.L
        base = sym.replace(".", "-").strip("-")
        if base:
            return f"{base}{suf}"

    # default: simple append suffix (even if it contains dot? we handled above)
    return f"{sym}{suf}"


def _looks_like_symbol(sym: str) -> bool:
    if not sym:
        return False
    if len(sym) > 12:
        return False
    if not TICKER_RE.match(sym):
        # allow trailing dot in raw TIDM, e.g. "RE."
        if sym.endswith(".") and TICKER_RE.match(sym[:-1]):
            return True
        return False
    return True


def _open_excel() -> pd.ExcelFile:
    local_path = (os.getenv("UK_LIST_XLSX_PATH") or "").strip()
    if local_path:
        p = Path(local_path)
        if p.exists():
            log(f"📄 Reading UK instrument list from local file: {p}")
            return pd.ExcelFile(str(p))
        log(f"⚠️ UK_LIST_XLSX_PATH set but not found: {p} (will download from URL)")

    url = (os.getenv("UK_LIST_URL") or DEFAULT_UK_LIST_URL).strip() or DEFAULT_UK_LIST_URL
    log(f"📡 Downloading UK instrument list ... {url}")
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    return pd.ExcelFile(BytesIO(r.content))


def _detect_header_row(xls: pd.ExcelFile, sheet_name: str) -> int:
    key = (os.getenv("UK_HEADER_KEY") or DEFAULT_HEADER_KEY).strip() or DEFAULT_HEADER_KEY
    scan_rows = int(os.getenv("UK_HEADER_SCAN_ROWS", "30") or "30")

    tmp = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=scan_rows)
    for i in range(len(tmp)):
        row = tmp.iloc[i].astype(str).str.strip()
        if any(v == key for v in row.tolist()):
            return int(i)

    return 8  # typical (0-based)


def _read_sheet(xls: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    header_row = _detect_header_row(xls, sheet_name)
    log(f"🧩 UK header_row detected: {header_row} (0-based) on sheet={sheet_name!r}")
    df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row)
    df = df.dropna(how="all").reset_index(drop=True)
    return df


def _pick_sheet(xls: pd.ExcelFile) -> str:
    sheet = (os.getenv("UK_SHEET_NAME") or DEFAULT_SHEET_NAME).strip() or DEFAULT_SHEET_NAME
    if sheet in xls.sheet_names:
        return sheet

    log(f"⚠️ sheet '{sheet}' not found. Available sheets: {xls.sheet_names}")

    for cand in ["1.1 Shares", "Shares", "1.0 All Equity"]:
        if cand in xls.sheet_names:
            log(f"➡️ fallback sheet = {cand}")
            return cand

    for s in xls.sheet_names:
        if "share" in s.lower():
            log(f"➡️ fallback sheet = {s}")
            return s

    return xls.sheet_names[0]


# =============================================================================
# Public API
# =============================================================================
def get_uk_stock_list(db_path: Path, refresh_list: bool = True) -> List[Tuple[str, str]]:
    init_db(db_path)

    if not refresh_list and db_path.exists():
        conn = sqlite3.connect(str(db_path))
        try:
            df = pd.read_sql_query("SELECT symbol, name FROM stock_info WHERE market='UK'", conn)
            if not df.empty:
                items = [(str(r["symbol"]), str(r["name"])) for _, r in df.iterrows()]
                log(f"✅ 使用 DB stock_info 既有 UK 清單: {len(items)} 檔")
                return items
            log("⚠️ refresh_list=False but UK stock_info is empty; will fetch list.")
        finally:
            conn.close()

    try:
        xls = _open_excel()
    except Exception as e:
        log(f"❌ UK list open failed: {e}")
        return []

    sheet = _pick_sheet(xls)
    df = _read_sheet(xls, sheet)
    if df is None or df.empty:
        log("❌ UK list sheet empty")
        return []

    if "TIDM" not in df.columns or "Issuer Name" not in df.columns:
        log(f"❌ Missing required columns. columns={list(df.columns)}")
        return []

    if _env_bool("UK_REQUIRE_MIFIR_SHRS", "1") and "MiFIR Identifier Code" in df.columns:
        df = df[df["MiFIR Identifier Code"].astype(str).str.strip().str.upper() == "SHRS"].copy()

    has_super = "ICB Super-Sector Name" in df.columns
    has_ind = "ICB Industry" in df.columns

    limit_n = int(os.getenv("UK_LIMIT_SYMBOLS", "0") or "0")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(str(db_path))
    stock_list: List[Tuple[str, str]] = []

    try:
        for _, row in df.iterrows():
            tidm_raw = _safe_str(row.get("TIDM", ""), "").upper()
            if not tidm_raw:
                continue
            if not _looks_like_symbol(tidm_raw):
                continue

            name = _safe_str(row.get("Issuer Name", ""), tidm_raw) or tidm_raw
            if EXCLUDE_NAME_RE.search(name or ""):
                continue

            industry = _safe_str(row.get("ICB Industry", ""), "Unknown") if has_ind else "Unknown"
            super_sector = _safe_str(row.get("ICB Super-Sector Name", ""), "Unknown") if has_super else "Unknown"
            sector = super_sector if super_sector and super_sector != "Unknown" else (industry or "Unknown")

            lse_market = _safe_str(row.get("LSE Market", ""), "Unknown") if "LSE Market" in df.columns else "Unknown"

            symbol = _normalize_for_yf(tidm_raw)

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, name, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (symbol, name, sector, "UK", lse_market, now),
            )

            stock_list.append((symbol, name))

            if limit_n > 0 and len(stock_list) >= limit_n:
                break

        conn.commit()
    finally:
        conn.close()

    log(f"✅ UK list imported: {len(stock_list)} (sheet={sheet}, limit={limit_n or 'ALL'})")
    return stock_list


if __name__ == "__main__":
    test_db = Path(os.getenv("UK_DB_PATH", os.path.join(os.path.dirname(__file__), "uk_stock_warehouse.db")))
    get_uk_stock_list(test_db, refresh_list=True)
