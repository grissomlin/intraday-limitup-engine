# markets/cn/cn_stock_list.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import List, Tuple

import pandas as pd

from .cn_config import log
from .cn_market import classify_cn_market

def _normalize_code_name_df(df: pd.DataFrame) -> Tuple[str, str]:
    code_col = "code" if "code" in df.columns else ("代码" if "代码" in df.columns else None)
    name_col = "name" if "name" in df.columns else ("名称" if "名称" in df.columns else None)
    if not code_col or not name_col:
        raise RuntimeError(f"unexpected columns: {list(df.columns)}")
    return code_col, name_col

def get_cn_stock_list(db_path: str, refresh_list: bool = True) -> List[Tuple[str, str]]:
    """
    回傳 [(symbol, name), ...]，symbol 為 Yahoo 格式：xxxxxx.SS / xxxxxx.SZ
    refresh_list=False 代表不重新抓名單，直接從 DB stock_info 取回。
    """
    if not refresh_list and os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute("SELECT symbol, name FROM stock_info").fetchall()
            items = [(s, (n or "Unknown")) for s, n in rows if s]
            if items:
                log(f"✅ 使用 DB stock_info 既有清單: {len(items)} 檔")
                return items
        finally:
            conn.close()

    log("📡 正在獲取 A 股清單（akshare）...")

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
                market, market_detail = classify_cn_market(symbol)
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
                market, market_detail = classify_cn_market(symbol)
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

    # --- 3) fallback DB ---
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT symbol, name FROM stock_info").fetchall()
        items = [(s, (n or "Unknown")) for s, n in rows if s]
        if items:
            log(f"✅ 使用 DB stock_info 既有清單: {len(items)} 檔")
            return items
    finally:
        conn.close()

    log("❌ 無可用 A 股清單（akshare 失敗且 DB 無既有名單）")
    return []
