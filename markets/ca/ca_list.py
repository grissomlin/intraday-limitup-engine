# markets/ca/ca_list.py
# -*- coding: utf-8 -*-
"""
Canada stock list fetch (TMX Issuer list Excel).

Flow:
- download TMX excel (default https://www.tsx.com/resource/en/571)
- read TSX (sheet0) + TSXV (sheet1)
- detect header row (robust vs skiprows change)
- keep core cols: Root Ticker / Name / Sector (+ optional filters)
- map to yfinance symbols:
    TSX  -> .TO
    TSXV -> .V
- write into DB stock_info (reuse US schema)

Env:
- CA_DB_PATH
- CA_LIST_URL (default TMX 571)
- CA_LIST_XLSX_PATH (optional local path)
- CA_HEADER_SCAN_ROWS (default 40)
- CA_LIMIT_SYMBOLS (default 0)
- CA_KEEP_RAW_TICKER (default 0)   # if 1, don't append suffix (rare)

Filtering (ALL env-switchable, default is ON and uses sensible defaults):
- CA_ENABLE_FILTERS (default 1)           # master switch for ALL filters
- CA_ENABLE_SECTOR_FILTER (default 1)
- CA_BAD_SECTORS (comma-separated override)
    default: ETP, CDR, Closed-End Funds, Structured Products

- CA_ENABLE_SP_TYPE_FILTER (default 1)    # TSX only (but if column exists elsewhere, will apply too)
- CA_DROP_SP_TYPES (comma-separated override)
    default drops: Exchange Traded Funds, CDR, Split Shares,
                   Fund of Equities, Fund of Debt, Fund of Multi-Asset, Fund of Other,
                   Commodity Funds, Exchange Traded Receipt, Fixed Income

- CA_ENABLE_NAME_REGEX_FILTER (default 1)
- CA_BAD_NAME_REGEX (override)
    default: r"\b(REIT|TRUST|FUND|ETF|INCOME FUND|UNIT|L\.P\.|LP)\b"

Liquidity filters (default ON for TSXV; OFF for TSX unless you enable it)
- CA_ENABLE_TSXV_CPC_FILTER (default 1)   # drop Sector == "CPC" on TSXV
- CA_ENABLE_TSXV_LIQ_FILTER (default 1)   # TSXV only
- CA_TSXV_MIN_MCAP (default 14000000)     # Market Cap (C$) threshold
- CA_TSXV_MIN_TRADES_YTD (default 300)    # Number of Trades YTD threshold

Optional: also apply liquidity filter to TSX if you want
- CA_ENABLE_TSX_LIQ_FILTER (default 0)
- CA_TSX_MIN_MCAP (default 0)
- CA_TSX_MIN_TRADES_YTD (default 0)
"""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict

import pandas as pd
import requests


# ---------------------------------------------------------------------
# logging fallback
# ---------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


DEFAULT_CA_LIST_URL = "https://www.tsx.com/resource/en/571"


def _db_path() -> Path:
    return Path(os.getenv("CA_DB_PATH", os.path.join(os.path.dirname(__file__), "ca_stock_warehouse.db")))


def _env_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except Exception:
        return int(default)


def _safe_str(x: object, default: str = "") -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
    except Exception:
        pass
    s = str(x).strip()
    return s if s else default


def _to_float(x: Any) -> Optional[float]:
    """
    Parse numeric-like values robustly:
    - supports strings like " 40,611,823 " or "40 611 823"
    - returns None if cannot parse
    """
    if x is None:
        return None
    try:
        if isinstance(x, float) and pd.isna(x):
            return None
    except Exception:
        pass
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return None
    s = str(x).strip()
    if not s:
        return None
    # remove commas and spaces
    s = s.replace(",", "").replace(" ", "")
    # handle stray currency symbols
    s = re.sub(r"[^0-9.\-]", "", s)
    if not s or s in {".", "-", "-.", ".-"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_info_market ON stock_info(market)")
        conn.commit()
    finally:
        conn.close()


def _open_excel() -> pd.ExcelFile:
    local_path = (_env_str("CA_LIST_XLSX_PATH", "") or "").strip()
    if local_path:
        p = Path(local_path)
        if p.exists():
            log(f"📄 Reading CA list from local file: {p}")
            return pd.ExcelFile(str(p))
        log(f"⚠️ CA_LIST_XLSX_PATH set but not found: {p} (will download from URL)")

    url = (_env_str("CA_LIST_URL", DEFAULT_CA_LIST_URL) or DEFAULT_CA_LIST_URL).strip() or DEFAULT_CA_LIST_URL
    log(f"📡 Downloading TMX issuer list ... {url}")
    r = requests.get(url, timeout=90, allow_redirects=True)
    r.raise_for_status()
    return pd.ExcelFile(BytesIO(r.content))


def _detect_header_row(xls: pd.ExcelFile, sheet_name: str) -> int:
    scan_rows = _env_int("CA_HEADER_SCAN_ROWS", 40)
    tmp = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=scan_rows)

    def norm(v: object) -> str:
        return str(v).replace("\n", " ").strip().lower()

    for i in range(len(tmp)):
        row = [norm(v) for v in tmp.iloc[i].tolist()]
        has_root = any("root" in v for v in row)
        has_ticker = any("ticker" in v for v in row)
        has_name = any(v == "name" or "name" in v for v in row)
        if has_root and has_ticker and has_name:
            return int(i)

    # historical guess: many TMX files use header around row 9 (0-based)
    return 9


def _read_sheet(xls: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    header_row = _detect_header_row(xls, sheet_name)
    log(f"🧩 CA header_row detected: {header_row} (0-based) on sheet={sheet_name!r}")
    df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row)
    df = df.dropna(how="all").reset_index(drop=True)
    return df


def _pick_sheet_by_index(xls: pd.ExcelFile, idx: int, fallback_keyword: str) -> Optional[str]:
    if idx < len(xls.sheet_names):
        return xls.sheet_names[idx]
    for s in xls.sheet_names:
        if fallback_keyword.lower() in s.lower():
            return s
    return xls.sheet_names[0] if xls.sheet_names else None


def _normalize_yahoo_symbol(ticker: str, exch: str) -> str:
    ticker = (ticker or "").strip().upper()
    exch = (exch or "").strip().upper()
    if _env_bool("CA_KEEP_RAW_TICKER", "0"):
        return ticker

    suffix_map = {"TSX": ".TO", "TSXV": ".V"}
    suf = suffix_map.get(exch, "")
    return f"{ticker}{suf}" if ticker and suf else ticker


def _split_csv_set(s: str) -> set[str]:
    items = []
    for part in (s or "").split(","):
        p = part.strip()
        if p:
            items.append(p)
    return set(items)


def get_ca_stock_list(db_path: Path, refresh_list: bool = True) -> List[Tuple[str, str]]:
    init_db(db_path)

    if not refresh_list and db_path.exists():
        conn = sqlite3.connect(str(db_path))
        try:
            df = pd.read_sql_query("SELECT symbol, name FROM stock_info WHERE market='CA'", conn)
            if not df.empty:
                items = [(str(r["symbol"]), str(r["name"])) for _, r in df.iterrows()]
                log(f"✅ Using DB stock_info cached CA list: {len(items)}")
                return items
        finally:
            conn.close()

    try:
        xls = _open_excel()
    except Exception as e:
        log(f"❌ CA list open failed: {e}")
        return []

    sheet_tsx = _pick_sheet_by_index(xls, 0, "TSX") or xls.sheet_names[0]
    sheet_tsxv = _pick_sheet_by_index(xls, 1, "TSXV") or (xls.sheet_names[1] if len(xls.sheet_names) > 1 else sheet_tsx)

    df_tsx = _read_sheet(xls, sheet_tsx)
    df_tsxv = _read_sheet(xls, sheet_tsxv) if sheet_tsxv else pd.DataFrame()

    def _find_col(df: pd.DataFrame, want: List[str]) -> Optional[str]:
        cols = list(df.columns)
        norm_map = {c: str(c).replace("\n", " ").strip().lower() for c in cols}

        # exact match first
        for w in want:
            w2 = w.lower()
            for c, nc in norm_map.items():
                if nc == w2:
                    return c

        # substring match
        for w in want:
            w2 = w.lower()
            for c, nc in norm_map.items():
                if w2 in nc:
                    return c
        return None

    def _prep(df: pd.DataFrame, exch: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(
                columns=[
                    "Ticker",
                    "Company_Name",
                    "Sector",
                    "Exchange",
                    "SP_Type",
                    "Market_Cap_CAD",
                    "Trades_YTD",
                ]
            )

        c_ticker = _find_col(df, ["Root Ticker", "Root\nTicker", "Ticker", "Root"])
        c_name = _find_col(df, ["Name", "Issuer Name", "Company Name"])
        c_sector = _find_col(df, ["Sector", "Industry Sector", "Industry"])

        # Optional columns (may not exist on TSXV depending on template)
        c_sp_type = _find_col(df, ["SP_Type", "SP Type", "SP Type ", "SP Type/Category"])
        c_mcap = _find_col(
            df,
            [
                "Market Cap (C$)",
                "Market Cap",
                "Market Cap (C$) 31-January-2026",
                "Market Cap (C$) 31-January-2026 ",
            ],
        )
        c_trades = _find_col(
            df,
            [
                "Number of Trades YTD",
                "Number of Trades YTD 31-January-2026",
                "Number of Trades",
                "Trades YTD",
            ],
        )

        if not c_ticker or not c_name:
            log(f"⚠️ Missing required cols on {exch}: ticker={c_ticker}, name={c_name}, cols={list(df.columns)}")
            return pd.DataFrame(
                columns=[
                    "Ticker",
                    "Company_Name",
                    "Sector",
                    "Exchange",
                    "SP_Type",
                    "Market_Cap_CAD",
                    "Trades_YTD",
                ]
            )

        out = pd.DataFrame(
            {
                "Ticker": df[c_ticker],
                "Company_Name": df[c_name],
                "Sector": df[c_sector] if c_sector else "Unknown",
                "SP_Type": df[c_sp_type] if c_sp_type else "",
                "Market_Cap_CAD": df[c_mcap] if c_mcap else None,
                "Trades_YTD": df[c_trades] if c_trades else None,
            }
        )
        out["Exchange"] = exch
        return out

    df1 = _prep(df_tsx, "TSX")
    df2 = _prep(df_tsxv, "TSXV")

    full = pd.concat([df1, df2], ignore_index=True)

    # Normalize core fields
    full["Ticker"] = full["Ticker"].astype(str).str.strip().str.upper()
    full["Company_Name"] = full["Company_Name"].astype(str).str.strip()
    full["Sector"] = full["Sector"].astype(str).str.strip().replace("", "Unknown")
    full["Exchange"] = full["Exchange"].astype(str).str.strip().str.upper()

    # Optional fields normalize
    if "SP_Type" in full.columns:
        full["SP_Type"] = full["SP_Type"].astype(str).str.strip()
    else:
        full["SP_Type"] = ""

    full = full.dropna(subset=["Ticker"])
    full = full[full["Ticker"].astype(str).str.len() > 0].copy()

    # ------------------------------------------------------------------
    # Filters (default ON, all env-switchable)
    # ------------------------------------------------------------------
    enable_filters = _env_bool("CA_ENABLE_FILTERS", "1")
    if enable_filters:
        # 1) Sector filter
        if _env_bool("CA_ENABLE_SECTOR_FILTER", "1"):
            bad_sectors_env = (_env_str("CA_BAD_SECTORS", "") or "").strip()
            if bad_sectors_env:
                bad_sectors = _split_csv_set(bad_sectors_env)
            else:
                bad_sectors = {"ETP", "CDR", "Closed-End Funds", "Structured Products"}
            before = len(full)
            full = full[~full["Sector"].isin(bad_sectors)].copy()
            log(f"🧹 Sector filter: -{before - len(full)} (drop={sorted(list(bad_sectors))})")

        # 2) SP_Type filter (best for TSX ETFs/funds/etc)
        if _env_bool("CA_ENABLE_SP_TYPE_FILTER", "1"):
            drop_sp_env = (_env_str("CA_DROP_SP_TYPES", "") or "").strip()
            if drop_sp_env:
                drop_sp_types = _split_csv_set(drop_sp_env)
            else:
                drop_sp_types = {
                    "Exchange Traded Funds",
                    "CDR",
                    "Split Shares",
                    "Fund of Equities",
                    "Fund of Debt",
                    "Fund of Multi-Asset",
                    "Fund of Other",
                    "Commodity Funds",
                    "Exchange Traded Receipt",
                    "Fixed Income",
                }
            if "SP_Type" in full.columns:
                before = len(full)
                # Keep rows where SP_Type is empty OR not in drop list
                full = full[(full["SP_Type"].astype(str).str.strip() == "") | (~full["SP_Type"].isin(drop_sp_types))].copy()
                log(f"🧹 SP_Type filter: -{before - len(full)} (drop={sorted(list(drop_sp_types))})")
            else:
                log("ℹ️ SP_Type filter enabled but column not found; skipped.")

        # 3) Name regex filter (coarse fallback)
        if _env_bool("CA_ENABLE_NAME_REGEX_FILTER", "1"):
            name_regex = (_env_str("CA_BAD_NAME_REGEX", "") or "").strip()
            if not name_regex:
                name_regex = r"\b(REIT|TRUST|FUND|ETF|INCOME FUND|UNIT|L\.P\.|LP)\b"
            name_pat = re.compile(name_regex, flags=re.IGNORECASE)
            before = len(full)
            full = full[~full["Company_Name"].str.contains(name_pat, na=False)].copy()
            log(f"🧹 Name regex filter: -{before - len(full)} (regex={name_regex})")

        # 4) TSXV-only CPC + liquidity
        #    (these are the big speed wins for TSXV small/illiquid names)
        drop_tsxv_cpc = _env_bool("CA_ENABLE_TSXV_CPC_FILTER", "1")
        enable_tsxv_liq = _env_bool("CA_ENABLE_TSXV_LIQ_FILTER", "1")

        if drop_tsxv_cpc:
            before = len(full)
            full = full[~((full["Exchange"] == "TSXV") & (full["Sector"].astype(str).str.upper() == "CPC"))].copy()
            log(f"🧹 TSXV CPC filter: -{before - len(full)}")

        if enable_tsxv_liq:
            min_mcap = float(_env_int("CA_TSXV_MIN_MCAP", 14_000_000))
            min_trades = float(_env_int("CA_TSXV_MIN_TRADES_YTD", 300))

            # Only apply if the columns exist meaningfully
            if "Market_Cap_CAD" in full.columns and "Trades_YTD" in full.columns:
                mcap = full["Market_Cap_CAD"].apply(_to_float)
                trades = full["Trades_YTD"].apply(_to_float)

                mask_tsxv = full["Exchange"] == "TSXV"
                # Keep TSXV rows that pass thresholds; if value missing -> treat as fail (drop)
                pass_mcap = (mcap >= min_mcap) & mcap.notna()
                pass_trades = (trades >= min_trades) & trades.notna()
                before = len(full)
                full = full[(~mask_tsxv) | (pass_mcap & pass_trades)].copy()
                log(f"🧹 TSXV liquidity filter: -{before - len(full)} (mcap>={int(min_mcap)}, trades>={int(min_trades)})")
            else:
                log("ℹ️ TSXV liquidity filter enabled but Market_Cap/Trades columns not found; skipped.")

        # 5) Optional: TSX liquidity filter (default OFF)
        if _env_bool("CA_ENABLE_TSX_LIQ_FILTER", "0"):
            min_mcap = float(_env_int("CA_TSX_MIN_MCAP", 0))
            min_trades = float(_env_int("CA_TSX_MIN_TRADES_YTD", 0))
            if min_mcap <= 0 and min_trades <= 0:
                log("ℹ️ TSX liquidity filter enabled but thresholds are 0; skipped.")
            elif "Market_Cap_CAD" in full.columns and "Trades_YTD" in full.columns:
                mcap = full["Market_Cap_CAD"].apply(_to_float)
                trades = full["Trades_YTD"].apply(_to_float)
                mask_tsx = full["Exchange"] == "TSX"

                cond = pd.Series(True, index=full.index)
                if min_mcap > 0:
                    cond = cond & (mcap >= min_mcap) & mcap.notna()
                if min_trades > 0:
                    cond = cond & (trades >= min_trades) & trades.notna()

                before = len(full)
                full = full[(~mask_tsx) | cond].copy()
                log(f"🧹 TSX liquidity filter: -{before - len(full)} (mcap>={int(min_mcap)}, trades>={int(min_trades)})")
            else:
                log("ℹ️ TSX liquidity filter enabled but Market_Cap/Trades columns not found; skipped.")

    # ------------------------------------------------------------------
    # yfinance symbols
    # ------------------------------------------------------------------
    full["symbol"] = full.apply(lambda r: _normalize_yahoo_symbol(str(r["Ticker"]), str(r["Exchange"])), axis=1)
    full = full.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

    limit_n = _env_int("CA_LIMIT_SYMBOLS", 0)
    if limit_n > 0:
        full = full.head(limit_n).copy()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(str(db_path))
    items: List[Tuple[str, str]] = []
    try:
        for _, r in full.iterrows():
            sym = _safe_str(r.get("symbol", ""), "")
            if not sym:
                continue
            name = _safe_str(r.get("Company_Name", ""), sym) or sym
            sector = _safe_str(r.get("Sector", ""), "Unknown") or "Unknown"
            exch = _safe_str(r.get("Exchange", ""), "Unknown") or "Unknown"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, name, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (sym, name, sector, "CA", exch, now),
            )
            items.append((sym, name))
        conn.commit()
    finally:
        conn.close()

    log(f"✅ CA list imported: {len(items)} (TSX={sheet_tsx}, TSXV={sheet_tsxv}, limit={limit_n or 'ALL'})")
    return items


if __name__ == "__main__":
    get_ca_stock_list(_db_path(), refresh_list=True)