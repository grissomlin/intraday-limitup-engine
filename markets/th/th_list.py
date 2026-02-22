# markets/th/th_list.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .th_config import _disable_thaifin, _list_xlsx_path, _yf_suffix, log


def _try_load_list_from_thaifin() -> Optional[pd.DataFrame]:
    """
    Expected DF columns from thaifin:
      ['symbol', 'name', 'industry', 'sector', 'market']
    """
    if _disable_thaifin():
        return None

    try:
        from thaifin import Stocks  # type: ignore
    except Exception:
        return None

    try:
        df = Stocks.list_with_names()
        if df is None or len(df) == 0:
            return None
        df = df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df
    except Exception as e:
        log(f"⚠️ thaifin list failed: {e}")
        return None


def _load_list_from_xlsx(xlsx_path: str) -> Optional[pd.DataFrame]:
    if not xlsx_path or not os.path.exists(xlsx_path):
        return None
    try:
        df = pd.read_excel(xlsx_path, engine="openpyxl")
        df = df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df
    except Exception as e:
        log(f"⚠️ xlsx list failed: {e}")
        return None


def _is_blankish(x: Any) -> bool:
    s = ("" if x is None else str(x)).strip()
    if (not s) or s in ("-", "—", "--", "－", "–", "nan", "None"):
        return True
    if s.lower() in ("unknown", "unclassified"):
        return True
    return False


def _norm_text(x: Any, default: str) -> str:
    s = ("" if x is None else str(x)).strip()
    if _is_blankish(s):
        return default
    return s


def _merge_keep_old_if_new_blank(new_val: Any, old_val: Any, default_if_both_blank: str) -> str:
    if not _is_blankish(new_val):
        return str(new_val).strip()
    if not _is_blankish(old_val):
        return str(old_val).strip()
    return default_if_both_blank


def _to_yf_symbol(local_symbol: str) -> str:
    s = (local_symbol or "").strip().upper()
    if not s:
        return ""
    suf = _yf_suffix()
    if s.endswith(suf.upper()) or s.endswith(suf.lower()):
        return s
    return f"{s}{suf}"


def _load_existing_stock_info_map(conn: sqlite3.Connection) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    try:
        rows = conn.execute(
            "SELECT symbol, local_symbol, name, industry, sector, market, market_detail FROM stock_info"
        ).fetchall()
    except Exception:
        rows = []

    for sym, local_sym, name, industry, sector, market, md in rows:
        if not sym:
            continue
        out[str(sym)] = {
            "local_symbol": str(local_sym or ""),
            "name": str(name or ""),
            "industry": str(industry or ""),
            "sector": str(sector or ""),
            "market": str(market or ""),
            "market_detail": str(md or ""),
        }
    return out


def get_th_stock_list(db_path: str) -> List[Tuple[str, str, str, str, str, str]]:
    """
    Returns list of tuples:
      (yf_symbol, local_symbol, name, industry, sector, market_detail)
    Also upserts into stock_info with "do-not-overwrite-nonempty-with-blankish" strategy.
    """
    log("📡 正在同步泰國股票名單 (thaifin -> fallback xlsx)...")
    log(
        f"🧩 TH_DISABLE_THAIFIN={_disable_thaifin()} | TH_LIST_XLSX_PATH={os.getenv('TH_LIST_XLSX_PATH','').strip() or '(auto)'}"
    )

    df = _try_load_list_from_thaifin()
    src = "thaifin"
    xlsx_used = None

    if df is None or df.empty:
        src = "xlsx"
        xlsx_used = _list_xlsx_path()
        df = _load_list_from_xlsx(xlsx_used)

    if df is None or df.empty:
        log("❌ TH 名單載入失敗：thaifin 不可用/停用且 xlsx 不存在或讀取失敗")
        if src == "xlsx":
            log(f"   tried xlsx: {xlsx_used}")
        return []

    for c in ["symbol", "name", "industry", "sector", "market"]:
        if c not in df.columns:
            df[c] = None

    items: List[Tuple[str, str, str, str, str, str]] = []
    conn = sqlite3.connect(db_path)
    try:
        existing = _load_existing_stock_info_map(conn)
        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for _, row in df.iterrows():
            local_symbol_raw = row.get("symbol")
            local_symbol = _norm_text(local_symbol_raw, "")
            if not local_symbol:
                continue

            yf_symbol = _to_yf_symbol(local_symbol)
            if not yf_symbol:
                continue

            old = existing.get(yf_symbol, {})

            name_new = _norm_text(row.get("name"), "Unknown")
            industry_new = _norm_text(row.get("industry"), "Unclassified")
            sector_new = _norm_text(row.get("sector"), "Unclassified")
            market_new = _norm_text(row.get("market"), "SET")

            name_final = _merge_keep_old_if_new_blank(name_new, old.get("name"), "Unknown")
            industry_final = _merge_keep_old_if_new_blank(industry_new, old.get("industry"), "Unclassified")
            sector_final = _merge_keep_old_if_new_blank(sector_new, old.get("sector"), "Unclassified")
            local_symbol_final = _merge_keep_old_if_new_blank(local_symbol, old.get("local_symbol"), local_symbol)

            market_detail = f"{market_new}|{src}"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, local_symbol, name, industry, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    yf_symbol,
                    local_symbol_final,
                    name_final,
                    industry_final,
                    sector_final,
                    "TH",
                    market_detail,
                    now_s,
                ),
            )

            existing[yf_symbol] = {
                "local_symbol": local_symbol_final,
                "name": name_final,
                "industry": industry_final,
                "sector": sector_final,
                "market": "TH",
                "market_detail": market_detail,
            }

            items.append((yf_symbol, local_symbol_final, name_final, industry_final, sector_final, market_detail))

        conn.commit()
    finally:
        conn.close()

    extra = f" (xlsx={xlsx_used})" if (src == "xlsx" and xlsx_used) else ""
    log(f"✅ 泰股名單同步完成：共 {len(items)} 檔 (source={src}){extra}")
    return items


def get_th_stock_list_from_db(db_path: str) -> List[Tuple[str, str, str, str, str, str]]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT symbol, local_symbol, name, industry, sector, market_detail FROM stock_info"
        ).fetchall()
        out: List[Tuple[str, str, str, str, str, str]] = []
        for sym, local_sym, name, industry, sector, md in rows:
            if not sym:
                continue
            out.append(
                (
                    str(sym),
                    str(local_sym or ""),
                    str(name or "Unknown"),
                    str(industry or "Unclassified"),
                    str(sector or "Unclassified"),
                    str(md or "unknown"),
                )
            )
        return out
    finally:
        conn.close()