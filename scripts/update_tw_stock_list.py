# scripts/update_tw_stock_list.py
# -*- coding: utf-8 -*-
"""
Update Taiwan stock universe from TWSE ISIN pages.
Outputs: data/tw_stock_list.json

✅ 目的
- 每次執行都從 TWSE ISIN 網頁重建「股票名單 + 產業別 + 市場別」
- 讓 markets/tw/downloader.py 讀取這份 json，再去 yfinance 抓價格

✅ 市場映射（重要）
- listed (.TW) -> market_detail="listed"
- otc (.TWO) -> market_detail="otc"
- dr (.TW) -> market_detail="dr"
- rotc (.TWO) -> market_detail="emerging"   # ⭐ 興櫃/ROTC：這裡把 rotc 映射成 emerging
- innovation pages -> market_detail="innovation_a"/"innovation_c"

Usage:
  python scripts/update_tw_stock_list.py

Optional env:
  TW_STOCKLIST_SLEEP=0.4      # 每次 request 間隔秒數
  TW_STOCKLIST_TIMEOUT=15     # request timeout 秒
  TW_STOCKLIST_RETRIES=3      # 失敗重試次數
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from io import StringIO

import requests
import pandas as pd


# =============================================================================
# Paths
# =============================================================================
REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = REPO_ROOT / "data" / "tw_stock_list.json"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Session
# =============================================================================
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
)


# =============================================================================
# Source URLs (你貼的來源：保留 rotc / 創新 A/C)
# =============================================================================
URL_CONFIGS = [
    {
        "name": "listed",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y",
        "suffix": ".TW",
    },
    {
        "name": "dr",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y",
        "suffix": ".TW",
    },
    {
        "name": "otc",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y",
        "suffix": ".TWO",
    },
    # ✅ 興櫃 / ROTC
    {
        "name": "rotc",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y",
        "suffix": ".TWO",
    },
    # ✅ 創新板 C（TW）
    {
        "name": "tw_innovation_c",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=C&issuetype=C&industry_code=&Page=1&chklike=Y",
        "suffix": ".TW",
    },
    # ✅ 創新板 A（TWO）
    {
        "name": "otc_innovation_a",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=A&issuetype=C&industry_code=&Page=1&chklike=Y",
        "suffix": ".TWO",
    },
]

# name -> market_detail mapping
MARKET_DETAIL_MAP = {
    "listed": "listed",
    "otc": "otc",
    "dr": "dr",
    "rotc": "emerging",  # ⭐ 關鍵：rotc 映射成 emerging，downloader 才會歸到 snapshot_open
    "tw_innovation_c": "innovation_c",
    "otc_innovation_a": "innovation_a",
}

# 這些欄位名稱在 TWSE ISIN 表格中常見（有時會略有差異）
CODE_COL_CANDIDATES = ["有價證券代號", "代號"]
NAME_COL_CANDIDATES = ["有價證券名稱", "名稱"]
SECTOR_COL_CANDIDATES = ["產業別", "產業別(或指數別)", "產業"]


# =============================================================================
# Tunables
# =============================================================================
SLEEP_S = float(os.getenv("TW_STOCKLIST_SLEEP", "0.4"))
TIMEOUT_S = int(os.getenv("TW_STOCKLIST_TIMEOUT", "15"))
RETRIES = int(os.getenv("TW_STOCKLIST_RETRIES", "3"))


# =============================================================================
# Helpers
# =============================================================================
def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def _read_first_table(html: str) -> pd.DataFrame:
    # TWSE ISIN 頁面通常第一個表格就是清單
    tables = pd.read_html(StringIO(html), header=0)
    if not tables:
        return pd.DataFrame()
    return tables[0]


def _fetch_table(url: str) -> pd.DataFrame:
    # 簡單重試：避免偶發 reset/timeout
    last_err: Optional[Exception] = None
    for i in range(max(1, RETRIES)):
        try:
            time.sleep(SLEEP_S)
            r = SESSION.get(url, timeout=TIMEOUT_S)
            r.raise_for_status()
            return _read_first_table(r.text)
        except Exception as e:
            last_err = e
            # 指數退避（小幅）
            time.sleep(min(2.0, 0.5 * (i + 1)))
    raise last_err or RuntimeError("fetch failed")


def _normalize_sector(x: Any) -> str:
    s = str(x or "").strip()
    if not s or s.lower() == "nan":
        return "未分類"
    return s


def _is_bad_row(code: str) -> bool:
    c = (code or "").strip()
    if not c or c.lower() == "nan":
        return True
    # 避免表頭重複列
    if "有價證券" in c or "代號" in c:
        return True
    return False


def _build_rows(cfg: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    """
    Returns (rows, skipped_count)
    """
    df = _fetch_table(cfg["url"])
    if df is None or df.empty:
        return [], 0

    code_col = _pick_col(df, CODE_COL_CANDIDATES)
    name_col = _pick_col(df, NAME_COL_CANDIDATES)
    sector_col = _pick_col(df, SECTOR_COL_CANDIDATES)

    if not code_col or not name_col:
        # 欄位找不到就全部跳過（避免產出垃圾）
        return [], len(df)

    suffix = cfg.get("suffix")
    mdetail = MARKET_DETAIL_MAP.get(cfg["name"], cfg["name"])

    out: List[Dict[str, Any]] = []
    skipped = 0

    for _, r in df.iterrows():
        code = str(r.get(code_col, "")).strip()
        name = str(r.get(name_col, "")).strip()

        if _is_bad_row(code):
            skipped += 1
            continue

        # 你這份 stock_list 只收「股票/DR/創新/興櫃」
        # suffix=None（權證等）就不收
        if not suffix:
            skipped += 1
            continue

        sym = f"{code}{suffix}"

        sector = "未分類"
        if sector_col:
            sector = _normalize_sector(r.get(sector_col))

        out.append(
            {
                "symbol": sym,
                "name": name,
                "sector": sector,
                "market": "tw",
                "market_detail": mdetail,
            }
        )

    return out, skipped


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    all_rows: List[Dict[str, Any]] = []
    stats: Dict[str, Any] = {"by_type": {}, "skipped_rows": {}, "total": 0}

    for cfg in URL_CONFIGS:
        try:
            rows, skipped = _build_rows(cfg)
            all_rows.extend(rows)
            stats["by_type"][cfg["name"]] = len(rows)
            stats["skipped_rows"][cfg["name"]] = int(skipped)
            print(f"✅ {cfg['name']}: {len(rows)} rows (skipped={skipped})")
        except Exception as e:
            stats["by_type"][cfg["name"]] = 0
            stats["skipped_rows"][cfg["name"]] = -1
            print(f"❌ {cfg['name']} failed: {e}")

    # 去重（同 symbol 取第一筆）
    dedup: Dict[str, Dict[str, Any]] = {}
    for r in all_rows:
        sym = (r.get("symbol") or "").strip()
        if sym and sym not in dedup:
            dedup[sym] = r

    rows_final = list(dedup.values())
    rows_final.sort(key=lambda x: (x.get("market_detail", ""), x.get("symbol", "")))

    stats["total"] = len(rows_final)
    emerging_cnt = sum(1 for x in rows_final if (x.get("market_detail") or "").strip() == "emerging")

    print(f"📦 total symbols = {len(rows_final)} | emerging = {emerging_cnt}")
    OUT_PATH.write_text(json.dumps(rows_final, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ wrote: {OUT_PATH}")


if __name__ == "__main__":
    main()
