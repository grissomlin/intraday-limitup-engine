# markets/india/india_list.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, List, Tuple

import pandas as pd

from .india_config import _master_csv_path, _yf_suffix, log


def _is_blankish(x: Any) -> bool:
    s = ("" if x is None else str(x)).strip()
    return (not s) or s in ("-", "—", "--", "－", "–", "nan", "None")


def _norm_text(x: Any, default: str) -> str:
    s = ("" if x is None else str(x)).strip()
    if _is_blankish(s):
        return default
    return s


def _to_yf_symbol(local_symbol: str) -> str:
    s = (local_symbol or "").strip().upper()
    if not s:
        return ""
    suf = _yf_suffix()
    if s.endswith(suf.upper()) or s.endswith(suf.lower()):
        return s
    return f"{s}{suf}"


def _load_master_csv(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"INDIA master CSV not found: {path} (set INDIA_MASTER_CSV_PATH)")
    df = pd.read_csv(path)
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def get_india_stock_list(db_path: str) -> List[Tuple[str, str, str, str, str, str]]:
    """
    Returns list of tuples:
      (yf_symbol, local_symbol, name, industry, sector, market_detail)

    market_detail example:
      "NSE|band=20|remarks=-|src=master_csv"
    """
    path = _master_csv_path()
    log(f"📡 同步印度 NSE 名單 (master_csv) path={path}")

    df = _load_master_csv(path)

    for c in ["SYMBOL", "NAME OF COMPANY", "Band", "Remarks", "sector", "industry"]:
        if c not in df.columns:
            df[c] = None

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    df = df[df["SYMBOL"].notna() & (df["SYMBOL"].str.len() > 0)].copy()

    items: List[Tuple[str, str, str, str, str, str]] = []
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db_path)
    try:
        for _, r in df.iterrows():
            local_symbol = _norm_text(r.get("SYMBOL"), "")
            if not local_symbol:
                continue

            yf_symbol = _to_yf_symbol(local_symbol)
            if not yf_symbol:
                continue

            name = _norm_text(r.get("NAME OF COMPANY"), "Unknown")
            sector = _norm_text(r.get("sector"), "Unclassified")
            industry = _norm_text(r.get("industry"), "Unclassified")

            band = _norm_text(r.get("Band"), "")
            remarks = _norm_text(r.get("Remarks"), "")

            md = f"NSE|band={band}|remarks={remarks}|src=master_csv"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, local_symbol, name, industry, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (yf_symbol, local_symbol, name, industry, sector, "INDIA", md, now_s),
            )

            items.append((yf_symbol, local_symbol, name, industry, sector, md))

        conn.commit()
    finally:
        conn.close()

    log(f"✅ INDIA NSE 名單同步完成：共 {len(items)} 檔")
    return items


def get_india_stock_list_from_db(db_path: str) -> List[Tuple[str, str, str, str, str, str]]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT symbol, local_symbol, name, industry, sector, market_detail FROM stock_info WHERE market='INDIA'"
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
