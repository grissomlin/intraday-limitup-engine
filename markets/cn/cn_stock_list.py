# markets/cn/cn_stock_list.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import time
import subprocess
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


def _ensure_akshare():
    try:
        import akshare as ak  # type: ignore
        return ak
    except Exception:
        log("📦 akshare 未安裝，嘗試安裝中 ...")
        subprocess.check_call(["pip", "install", "-q", "akshare"])
        import akshare as ak  # type: ignore
        return ak


def _iter_to_db(
    df: pd.DataFrame,
    db_path: str,
    valid_prefixes: Tuple[str, ...],
    code_col: str,
    name_col: str,
) -> List[Tuple[str, str]]:
    conn = sqlite3.connect(db_path)
    stock_list: List[Tuple[str, str]] = []
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for _, row in df.iterrows():
            code = str(row.get(code_col, "")).strip().zfill(6)
            if not code or not code.startswith(valid_prefixes):
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
        return stock_list
    finally:
        conn.close()


def _fallback_db_stock_info(db_path: str) -> List[Tuple[str, str]]:
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT symbol, name FROM stock_info").fetchall()
        items = [(s, (n or "Unknown")) for s, n in rows if s]
        return items
    finally:
        conn.close()


def get_cn_stock_list(db_path: str, refresh_list: bool = True) -> List[Tuple[str, str]]:
    """
    回傳 [(symbol, name), ...]，symbol 為 Yahoo 格式：xxxxxx.SS / xxxxxx.SZ
    refresh_list=False 代表不重新抓名單，直接從 DB stock_info 取回。

    ✅ 強化策略（你要的）：
    - 方案A（主）：ak.stock_info_a_code_name() 失敗重試最多 3 次
    - 方案B（備）：ak.stock_zh_a_spot_em()（東方財富）同樣重試最多 3 次
    - 最終備援：若 DB 也沒有，塞入最小清單（3 檔）避免整條 pipeline 空跑
    """
    # --- 0) fast path: no refresh -> DB ---
    if not refresh_list:
        items = _fallback_db_stock_info(db_path)
        if items:
            log(f"✅ 使用 DB stock_info 既有清單: {len(items)} 檔")
            return items
        log("⚠️ refresh_list=False 但 DB 無既有清單，將改為抓取新名單…")

    log("📡 正在獲取 A 股清單（akshare）...")

    valid_prefixes: Tuple[str, ...] = (
        "000", "001", "002", "003",
        "300", "301", "302",
        "600", "601", "603", "605",
        "688", "689",
    )

    ak = _ensure_akshare()

    # --- 1) 方案A：stock_info_a_code_name（重試） ---
    last_err_a: Exception | None = None
    for attempt in range(1, 4):
        try:
            df = ak.stock_info_a_code_name()
            code_col, name_col = _normalize_code_name_df(df)
            stock_list = _iter_to_db(df, db_path, valid_prefixes, code_col, name_col)
            if stock_list:
                log(f"✅ A 股清單導入成功(方案A code_name): {len(stock_list)} 檔")
                return stock_list
            raise RuntimeError("方案A 回傳空清單")
        except Exception as e:
            last_err_a = e
            if attempt < 3:
                wait = 5 * attempt
                log(f"⚠️ 方案A 第{attempt}次失敗：{e}，{wait}秒後重試…")
                time.sleep(wait)
            else:
                log(f"⚠️ 方案A 第{attempt}次失敗：{e}，改用方案B（東方財富 spot_em）")

    # --- 2) 方案B：stock_zh_a_spot_em（重試） ---
    last_err_b: Exception | None = None
    for attempt in range(1, 4):
        try:
            df_spot = ak.stock_zh_a_spot_em()
            code_col = "代码" if "代码" in df_spot.columns else ("code" if "code" in df_spot.columns else None)
            name_col = "名称" if "名称" in df_spot.columns else ("name" if "name" in df_spot.columns else None)
            if not code_col or not name_col:
                raise RuntimeError(f"unexpected columns: {list(df_spot.columns)}")

            stock_list = _iter_to_db(df_spot, db_path, valid_prefixes, code_col, name_col)
            if stock_list:
                log(f"✅ A 股清單導入成功(方案B spot_em): {len(stock_list)} 檔")
                return stock_list
            raise RuntimeError("方案B 回傳空清單")
        except Exception as e:
            last_err_b = e
            if attempt < 3:
                wait = 5 * attempt
                log(f"⚠️ 方案B 第{attempt}次失敗：{e}，{wait}秒後重試…")
                time.sleep(wait)
            else:
                log(f"⚠️ 方案B 第{attempt}次失敗：{e}，將改用 DB 既有 stock_info / 最小備援")

    # --- 3) fallback DB ---
    items = _fallback_db_stock_info(db_path)
    if items:
        log(f"✅ 使用 DB stock_info 既有清單: {len(items)} 檔")
        return items

    # --- 4) 最終備援：最小清單塞入 DB，避免空跑 ---
    minimal = [("600519.SS", "貴州茅台"), ("000001.SZ", "平安銀行"), ("300750.SZ", "寧德時代")]
    log("❌ 無可用 A 股清單（akshare 方案A/方案B 皆失敗且 DB 無既有名單）")
    log(f"⚠️ 使用最小備援清單（3 檔）寫入 DB 以保 pipeline 可續跑：A_err={last_err_a} | B_err={last_err_b}")

    # 把 minimal 寫進 DB（用同樣 schema）
    conn = sqlite3.connect(db_path)
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for symbol, name in minimal:
            market, market_detail = classify_cn_market(symbol)
            sector = "A-Share"
            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, name, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (symbol, name, sector, market, market_detail, now),
            )
        conn.commit()
    finally:
        conn.close()

    return minimal
