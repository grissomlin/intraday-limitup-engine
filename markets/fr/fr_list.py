# markets/fr/fr_list.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd

# 你也可以把 log 統一收斂到 fr_config.py，先給最小可跑版
def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


def _db_path() -> Path:
    return Path(os.getenv("FR_DB_PATH", os.path.join(os.path.dirname(__file__), "fr_stock_warehouse.db")))


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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fr_prices_symbol ON stock_prices(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fr_prices_date ON stock_prices(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fr_info_market ON stock_info(market)")
        conn.commit()
    finally:
        conn.close()


def _safe_str(x: object, default: str = "") -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
    except Exception:
        pass
    s = str(x).strip()
    return s if s else default


def get_fr_stock_list(db_path: Path, refresh_list: bool = True) -> List[Tuple[str, str]]:
    """
    讀取 FR 主清單（你現在這份 CSV），寫入 DB.stock_info，回傳 [(yf_symbol, name), ...]

    Env:
      - FR_MASTER_CSV_PATH:
          預設 data/cache/FR_Stock_Master_Data.csv（你可改成你 pipeline 產出的路徑）
      - FR_USE_DB_LIST=1：若 refresh_list=False 優先用 DB 內既有清單
    """
    init_db(db_path)

    if (not refresh_list) and os.getenv("FR_USE_DB_LIST", "1").strip() == "1" and db_path.exists():
        conn = sqlite3.connect(str(db_path))
        try:
            df = pd.read_sql_query("SELECT symbol, name FROM stock_info WHERE market='FR'", conn)
            if not df.empty:
                items = [(str(r["symbol"]), str(r["name"])) for _, r in df.iterrows()]
                log(f"✅ 使用 DB stock_info 既有 FR 清單: {len(items)} 檔")
                return items
        finally:
            conn.close()

    csv_path = (os.getenv("FR_MASTER_CSV_PATH") or "").strip()
    if not csv_path:
        # 預設放在 repo 內 data/cache
        repo_root = Path(__file__).resolve().parents[2]
        csv_path = str(repo_root / "data" / "cache" / "FR_Stock_Master_Data.csv")

    p = Path(csv_path)
    if not p.exists():
        log(f"❌ FR_MASTER_CSV_PATH not found: {p}")
        return []

    df = pd.read_csv(p)
    if df.empty:
        log("❌ FR master csv empty")
        return []

    # 你現在檔案有 yf_symbol / company_name / sector
    if "yf_symbol" not in df.columns:
        log(f"❌ FR master csv missing yf_symbol. columns={list(df.columns)}")
        return []

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(str(db_path))
    items: List[Tuple[str, str]] = []
    try:
        for _, r in df.iterrows():
            sym = _safe_str(r.get("yf_symbol"), "").upper()
            if not sym:
                continue

            name = _safe_str(r.get("company_name"), "") or _safe_str(r.get("name"), "") or sym
            sector = _safe_str(r.get("sector"), "Unknown") or "Unknown"

            # Euronext Paris
            market_detail = "Euronext Paris"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, name, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (sym, name, sector, "FR", market_detail, now),
            )
            items.append((sym, name))

        conn.commit()
    finally:
        conn.close()

    log(f"✅ FR list imported: {len(items)}")
    return items


if __name__ == "__main__":
    items = get_fr_stock_list(_db_path(), refresh_list=True)
    print("items:", len(items), "head:", items[:5])
