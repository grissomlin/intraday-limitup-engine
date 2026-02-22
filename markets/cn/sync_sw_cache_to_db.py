# markets/cn/sync_sw_cache_to_db.py
# -*- coding: utf-8 -*-
"""
Sync CN SW industry cache JSON -> sqlite stock_info

用途：
- 讀 markets/cn/cn_sw_merged_cache.json
- 將 sector / sw_l3 / sw_code 寫回 markets/cn/cn_stock_warehouse.db 的 stock_info
- 預設只更新「缺失或壞值」(only-missing) —— 很快
- 支援 dry-run、統計、以及自動補欄位(ALTER TABLE)

JSON 格式（你目前的 merged 檔）：
data[symbol] = {
  "name": "...",
  "sector": "股份制银行Ⅲ",
  "sector_level": "l3",
  "sector_code": "857831.SI",
  "sw_l3": {"sw_code": "...", "sw_name": "..."}  # 可有可無
}

注意：
- cache 裡 sector 可能是空字串（unmapped），這種會跳過不寫
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple

BAD_SECTOR = {"", "A-Share", "—", "-", "--", "－", "–", None, "未分類"}


def _strip(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _default_db_path() -> str:
    return os.getenv("CN_DB_PATH", os.path.join(os.path.dirname(__file__), "cn_stock_warehouse.db"))


def _default_cache_path() -> str:
    return os.getenv("CN_SW_CACHE_PATH", os.path.join(os.path.dirname(__file__), "cn_sw_merged_cache.json"))


def _connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=120)


def _has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    cols = {r[1] for r in rows}
    return col in cols


def _ensure_columns(conn: sqlite3.Connection) -> None:
    """
    確保 stock_info 有 sw_l3 / sw_code 欄位（沒有就補）
    sector 欄位通常本來就有，但也做保險檢查
    """
    # stock_info 必須存在
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_info'"
    ).fetchone()
    if not row:
        raise RuntimeError("DB missing table: stock_info (請先跑 downloader.run_sync 建表)")

    # sector（大多已存在）
    if not _has_column(conn, "stock_info", "sector"):
        conn.execute("ALTER TABLE stock_info ADD COLUMN sector TEXT")

    if not _has_column(conn, "stock_info", "sw_l3"):
        conn.execute("ALTER TABLE stock_info ADD COLUMN sw_l3 TEXT")

    if not _has_column(conn, "stock_info", "sw_code"):
        conn.execute("ALTER TABLE stock_info ADD COLUMN sw_code TEXT")


def _load_cache(cache_path: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    obj = json.loads(open(cache_path, "r", encoding="utf-8").read())
    meta = obj.get("_meta", {}) if isinstance(obj, dict) else {}
    data = obj.get("data", {}) if isinstance(obj, dict) else {}
    if not isinstance(data, dict):
        data = {}
    return data, meta


def _normalize_symbol(sym: str) -> str:
    """
    你 repo 用的格式是 000001.SZ / 600000.SS
    cache 也同樣。
    這裡只做 strip + upper。
    """
    return _strip(sym).upper()


def _is_bad_sector(v: Any) -> bool:
    s = _strip(v)
    return (s in BAD_SECTOR) or (s == "")


def _build_updates_from_cache(
    cache_data: Dict[str, Any],
    *,
    only_missing: bool,
) -> Dict[str, Dict[str, str]]:
    """
    回傳 mapping: symbol -> {"sector": ..., "sw_l3": ..., "sw_code": ...}
    只納入 cache 裡 sector 非空的股票
    """
    out: Dict[str, Dict[str, str]] = {}
    for sym, item in cache_data.items():
        sym2 = _normalize_symbol(sym)
        if not sym2:
            continue
        if not isinstance(item, dict):
            continue

        sector = _strip(item.get("sector"))
        sw_code = _strip(item.get("sector_code")) or _strip(item.get("sector_code", ""))
        # 你的 json 裡 sw_l3 還可能有 sw_name/sw_code；但我們優先採 sector/sector_code
        if not sector:
            continue  # unmapped -> 跳過

        out[sym2] = {"sector": sector, "sw_l3": sector, "sw_code": sw_code}

    return out


def _fetch_existing_info(conn: sqlite3.Connection) -> Dict[str, Dict[str, str]]:
    """
    取出 stock_info 目前 sector/sw_l3/sw_code，回傳 dict
    """
    rows = conn.execute(
        "SELECT symbol, COALESCE(sector,''), COALESCE(sw_l3,''), COALESCE(sw_code,'') FROM stock_info"
    ).fetchall()
    m: Dict[str, Dict[str, str]] = {}
    for sym, sector, sw_l3, sw_code in rows:
        s = _normalize_symbol(sym)
        m[s] = {"sector": _strip(sector), "sw_l3": _strip(sw_l3), "sw_code": _strip(sw_code)}
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=_default_db_path(), help="sqlite db path")
    ap.add_argument("--cache", default=_default_cache_path(), help="cn_sw_merged_cache.json path")
    ap.add_argument("--only-missing", action="store_true", help="只更新 sector/sw 缺失或壞值者（建議）")
    ap.add_argument("--full", action="store_true", help="強制全量覆蓋（不建議，除非你要以 cache 為準）")
    ap.add_argument("--dry-run", action="store_true", help="只印統計，不寫 DB")
    ap.add_argument("--limit", type=int, default=0, help="只更新前 N 筆（測試用）")
    args = ap.parse_args()

    only_missing = True
    if args.full:
        only_missing = False
    if args.only_missing:
        only_missing = True

    if not os.path.exists(args.cache):
        raise FileNotFoundError(f"Cache not found: {args.cache}")
    if not os.path.exists(args.db):
        raise FileNotFoundError(f"DB not found: {args.db}")

    cache_data, meta = _load_cache(args.cache)
    updates = _build_updates_from_cache(cache_data, only_missing=only_missing)

    conn = _connect(args.db)
    try:
        _ensure_columns(conn)
        conn.commit()

        existing = _fetch_existing_info(conn)

        # 決定哪些要寫
        to_write: List[Tuple[str, str, str, str]] = []
        skip_not_in_db = 0
        skip_no_cache = 0
        skip_not_missing = 0

        for sym, u in updates.items():
            cur = existing.get(sym)
            if cur is None:
                skip_not_in_db += 1
                continue

            new_sector = u["sector"]
            new_sw_l3 = u["sw_l3"]
            new_sw_code = u["sw_code"]

            if only_missing:
                # 只更新缺失/壞值
                need = False
                if _is_bad_sector(cur.get("sector")):
                    need = True
                if _strip(cur.get("sw_l3")) == "":
                    need = True
                if _strip(cur.get("sw_code")) == "":
                    need = True

                if not need:
                    skip_not_missing += 1
                    continue

            to_write.append((new_sector, new_sw_l3, new_sw_code, sym))
            if args.limit and len(to_write) >= args.limit:
                break

        total_db = len(existing)
        total_cache = len(cache_data)
        total_updates = len(updates)
        planned = len(to_write)

        print("📦 DB:", args.db)
        print("🧾 cache:", args.cache)
        print("🧾 cache meta:", {k: meta.get(k) for k in ("generated_at", "total_symbols", "mapped_symbols", "unmapped_symbols")})
        print("📊 DB stock_info symbols:", total_db)
        print("📊 cache symbols:", total_cache)
        print("📊 usable cache (sector non-empty):", total_updates)
        print("🎯 planned updates:", planned, "| only_missing=", only_missing, "| dry_run=", bool(args.dry_run))
        print("⏭️ skip_not_in_db:", skip_not_in_db, "| skip_not_missing:", skip_not_missing)

        if args.dry_run:
            print("🧪 dry-run: no DB writes.")
            return

        if planned == 0:
            print("✅ nothing to update.")
            return

        conn.execute("BEGIN")
        conn.executemany(
            """
            UPDATE stock_info
            SET sector = ?,
                sw_l3  = ?,
                sw_code= ?
            WHERE symbol = ?
            """,
            to_write,
        )
        conn.commit()
        print(f"✅ updated rows: {planned}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
