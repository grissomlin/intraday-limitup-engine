# -*- coding: utf-8 -*-
"""
sw_industry_sync.py
-------------------
把 A 股股票 -> 申萬行業(1/2/3級) 對應起來，並寫回 cn_stock_warehouse.db 的 stock_info

依賴：
  pip install akshare pandas tqdm

用法：
  python markets/cn/sw_industry_sync.py --db markets/cn/cn_stock_warehouse.db

可選：
  --only-missing     只更新目前 stock_info 中 sw_* 欄位為空的股票（同時也只覆蓋 sector 缺失/壞值）
  --max-industries   只跑前 N 個行業（測試用）
  --sector-level     sector 要用哪一層申萬：l1 / l2 / l3（預設 l3）
環境變數（可選）：
  CN_SECTOR_LEVEL    同 --sector-level（預設 l3）
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from datetime import datetime

import pandas as pd
from tqdm import tqdm


BAD_SECTOR_VALUES = {"", "A-Share", "—", "-", "--", "－", "–", "未分類", None}


def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


def ensure_columns(conn: sqlite3.Connection):
    # stock_info: symbol,name,sector,market,market_detail,updated_at (你現在已有)
    # 我們加：sw_l1, sw_l2, sw_l3, sw_code
    cols = [r[1] for r in conn.execute("PRAGMA table_info(stock_info)").fetchall()]
    need = []
    if "sw_l1" not in cols:
        need.append(("sw_l1", "TEXT"))
    if "sw_l2" not in cols:
        need.append(("sw_l2", "TEXT"))
    if "sw_l3" not in cols:
        need.append(("sw_l3", "TEXT"))
    if "sw_code" not in cols:
        need.append(("sw_code", "TEXT"))

    for name, typ in need:
        conn.execute(f"ALTER TABLE stock_info ADD COLUMN {name} {typ}")
    if need:
        conn.commit()
        log(f"🧩 已補欄位: {', '.join([n for n, _ in need])}")


def load_symbols(conn: sqlite3.Connection, only_missing: bool) -> set[str]:
    if not only_missing:
        rows = conn.execute("SELECT symbol FROM stock_info").fetchall()
        return {r[0] for r in rows if r and r[0]}

    # 只挑 sw 欄位缺的（你原本邏輯）
    rows = conn.execute(
        "SELECT symbol FROM stock_info WHERE COALESCE(sw_l1,'')='' OR COALESCE(sw_l3,'')=''"
    ).fetchall()
    return {r[0] for r in rows if r and r[0]}


def split_to_yf_symbol(code_any: str) -> str:
    """
    AKShare 成份股常見給 6 位碼 + 市場文字；我們統一轉成你 DB 用的 Yahoo 格式：
      6 開頭 -> .SS
      其餘 -> .SZ
    """
    code = str(code_any).strip()
    code = code.replace("SZ", "").replace("SH", "").replace(".", "").strip()
    code = code[:6].zfill(6)
    return f"{code}.SS" if code.startswith("6") else f"{code}.SZ"


def build_sw_mapping(max_industries: int | None = None) -> pd.DataFrame:
    import akshare as ak

    # 申萬三級行業列表
    df_l3 = ak.sw_index_third_info()
    # 兼容不同列名
    code_col = "行业代码" if "行业代码" in df_l3.columns else "行業代碼"
    name_col = "行业名称" if "行业名称" in df_l3.columns else "行業名稱"

    l3_codes = df_l3[code_col].astype(str).tolist()
    l3_names = df_l3[name_col].astype(str).tolist()

    if max_industries:
        l3_codes = l3_codes[:max_industries]
        l3_names = l3_names[:max_industries]

    rows = []
    pbar = tqdm(list(zip(l3_codes, l3_names)), desc="SW三級行業", unit="行業")

    for l3_code, l3_name in pbar:
        try:
            df_cons = ak.sw_index_third_cons(symbol=str(l3_code))
            if df_cons is None or df_cons.empty:
                continue

            stock_col = "股票代码" if "股票代码" in df_cons.columns else "股票代碼"
            sw1_col = "申万1级" if "申万1级" in df_cons.columns else "申萬1級"
            sw2_col = "申万2级" if "申万2级" in df_cons.columns else "申萬2級"
            sw3_col = "申万3级" if "申万3级" in df_cons.columns else "申萬3級"

            for _, r in df_cons.iterrows():
                yf_sym = split_to_yf_symbol(r.get(stock_col, ""))
                sw1 = str(r.get(sw1_col, "")).strip()
                sw2 = str(r.get(sw2_col, "")).strip()
                sw3 = str(r.get(sw3_col, "")).strip() or str(l3_name).strip()

                if not yf_sym.endswith((".SS", ".SZ")):
                    continue

                rows.append(
                    {
                        "symbol": yf_sym,
                        "sw_l1": sw1,
                        "sw_l2": sw2,
                        "sw_l3": sw3,
                        "sw_code": str(l3_code),
                    }
                )

            time.sleep(0.05)  # 降速避免被限流
        except Exception as e:
            pbar.set_postfix_str(f"skip {l3_code}: {e}")
            continue

    df_map = pd.DataFrame(rows).dropna(subset=["symbol"]).drop_duplicates(subset=["symbol"])
    return df_map


def _pick_sector_value(row: pd.Series, sector_level: str) -> str:
    """
    sector_level: l1/l2/l3
    預設使用 l3；若該層為空，依序 fallback 到 l2/l1
    """
    l1 = str(row.get("sw_l1", "") or "").strip()
    l2 = str(row.get("sw_l2", "") or "").strip()
    l3 = str(row.get("sw_l3", "") or "").strip()

    level = (sector_level or "l3").strip().lower()
    if level == "l1":
        return l1 or l2 or l3
    if level == "l2":
        return l2 or l3 or l1
    return l3 or l2 or l1  # l3 default


def upsert_mapping_to_db(db_path: str, only_missing: bool, max_industries: int | None, sector_level: str):
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        ensure_columns(conn)

        universe = load_symbols(conn, only_missing=only_missing)
        log(f"🎯 DB stock_info symbols: {len(universe)} (only_missing={only_missing})")

        log("📡 下載申萬行業成份，建立股票→行業 mapping ...")
        df_map = build_sw_mapping(max_industries=max_industries)
        log(f"✅ mapping 產出: {len(df_map)} 檔")

        # 只更新你 DB 內存在的 symbol
        df_map = df_map[df_map["symbol"].isin(universe)]
        log(f"🔎 過濾成 DB 內存在者: {len(df_map)} 檔")

        # 準備 sector（預設用 sw_l3）
        df_map = df_map.copy()
        df_map["sector_new"] = df_map.apply(lambda r: _pick_sector_value(r, sector_level), axis=1)
        df_map["sector_new"] = df_map["sector_new"].fillna("").astype(str).str.strip()

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.cursor()

        if only_missing:
            # ✅ only-missing：sector 只覆蓋缺失/壞值（避免破壞你後續手動修過的 sector）
            cur.executemany(
                """
                UPDATE stock_info
                   SET sw_l1=?,
                       sw_l2=?,
                       sw_l3=?,
                       sw_code=?,
                       sector=CASE
                               WHEN sector IS NULL
                                 OR TRIM(sector)=''
                                 OR sector IN ('A-Share','—','-','--','－','–','未分類')
                               THEN ?
                               ELSE sector
                             END,
                       updated_at=?
                 WHERE symbol=?
                """,
                [
                    (r["sw_l1"], r["sw_l2"], r["sw_l3"], r["sw_code"], r["sector_new"], now, r["symbol"])
                    for _, r in df_map.iterrows()
                    if str(r["sector_new"] or "").strip() != ""
                ],
            )
        else:
            # ✅ 非 only-missing：sector 直接覆蓋成 sw 層級（但仍不把空值寫進去）
            cur.executemany(
                """
                UPDATE stock_info
                   SET sw_l1=?,
                       sw_l2=?,
                       sw_l3=?,
                       sw_code=?,
                       sector=?,
                       updated_at=?
                 WHERE symbol=?
                """,
                [
                    (r["sw_l1"], r["sw_l2"], r["sw_l3"], r["sw_code"], r["sector_new"], now, r["symbol"])
                    for _, r in df_map.iterrows()
                    if str(r["sector_new"] or "").strip() != ""
                ],
            )

        conn.commit()
        log(f"🧾 已更新 stock_info: {cur.rowcount} 筆 (sector_level={sector_level})")

        # 驗證：看前 20 筆
        sample = conn.execute(
            "SELECT symbol,name,sector,sw_l1,sw_l2,sw_l3 FROM stock_info WHERE COALESCE(sw_l3,'')<>'' LIMIT 20"
        ).fetchall()
        log("🔍 sample with SW industry:")
        for row in sample:
            log("  " + " | ".join([str(x) for x in row]))

    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="markets/cn/cn_stock_warehouse.db", help="path to cn_stock_warehouse.db")
    ap.add_argument("--only-missing", action="store_true", help="only update rows with empty sw fields")
    ap.add_argument("--max-industries", type=int, default=None, help="limit industries for quick test")
    ap.add_argument(
        "--sector-level",
        default=os.getenv("CN_SECTOR_LEVEL", "l3"),
        choices=["l1", "l2", "l3"],
        help="which SW level to write into stock_info.sector (default: l3)",
    )
    args = ap.parse_args()

    upsert_mapping_to_db(
        args.db,
        only_missing=args.only_missing,
        max_industries=args.max_industries,
        sector_level=args.sector_level,
    )


if __name__ == "__main__":
    main()
