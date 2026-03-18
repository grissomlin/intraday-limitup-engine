# stocksymboldownload/build_stock_metadata.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
STOCK_METADATA_PATH = ROOT / "stocksymboldownload" / "stock_metadata.csv"


URL_CONFIGS = [
    {
        "name": "listed",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y",
        "suffix": ".TW",
        "board": "LISTED",
    },
    {
        "name": "dr",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y",
        "suffix": ".TW",
        "board": "LISTED",
    },
    {
        "name": "otc",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y",
        "suffix": ".TWO",
        "board": "OTC",
    },
    {
        "name": "rotc",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?market=E&issuetype=R&industry_code=&Page=1&chklike=Y",
        "suffix": ".TWO",
        "board": "EMERGING",
    },
    {
        "name": "tw_innovation",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=C&issuetype=C&industry_code=&Page=1&chklike=Y",
        "suffix": ".TW",
        "board": "INNOVATION",
    },
    {
        "name": "otc_innovation",
        "url": "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=A&issuetype=C&industry_code=&Page=1&chklike=Y",
        "suffix": ".TWO",
        "board": "INNOVATION",
    },
]


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TWStockMetadataBot/1.0)"
}


def _normalize_stock_id(x: str) -> str:
    s = str(x).strip().upper()
    s = s.replace("TPE:", "").replace(":TW", "")
    s = re.sub(r"\.(TW|TWO)$", "", s)
    m = re.search(r"\d+", s)
    return m.group(0) if m else s


def fetch_isin_table(url_config: Dict[str, Any]) -> pd.DataFrame:
    try:
        time.sleep(0.3)

        r = requests.get(url_config["url"], headers=HEADERS, timeout=20)
        r.raise_for_status()

        tables = pd.read_html(StringIO(r.text), header=0)
        if not tables:
            return pd.DataFrame()

        df = tables[0].copy()

        if "有價證券代號" not in df.columns or "有價證券名稱" not in df.columns:
            return pd.DataFrame()

        out = df[["有價證券代號", "有價證券名稱"]].copy()
        out["stock_id_raw"] = out["有價證券代號"].astype(str).str.strip()
        out["stock_name"] = out["有價證券名稱"].astype(str).str.strip()
        out["suffix"] = url_config["suffix"]
        out["board"] = url_config["board"]
        out["source_name"] = url_config["name"]

        industry_col = None
        for c in df.columns:
            if "產業別" in str(c):
                industry_col = c
                break

        if industry_col is not None:
            out["industry"] = df[industry_col].astype(str).str.strip()
        else:
            out["industry"] = ""

        out = out[out["stock_id_raw"].str.fullmatch(r"\d+")].copy()

        out["ticker"] = out["stock_id_raw"] + out["suffix"]
        out["normalized_ticker"] = out["stock_id_raw"].apply(_normalize_stock_id)

        return out[
            [
                "stock_id_raw",
                "normalized_ticker",
                "ticker",
                "stock_name",
                "industry",
                "board",
                "source_name",
            ]
        ].copy()

    except Exception as e:
        print(f"❌ 抓取 {url_config['name']} 失敗：{e}")
        return pd.DataFrame()


def build_stock_metadata() -> pd.DataFrame:
    dfs = []

    for cfg in URL_CONFIGS:
        df = fetch_isin_table(cfg)
        print(f"✅ {cfg['name']} 抓到 {len(df)} 筆")
        if not df.empty:
            dfs.append(df)

    if not dfs:
        raise RuntimeError("所有股票清單都抓不到")

    df_all = pd.concat(dfs, ignore_index=True)

    # 同一 ticker 只保留第一筆
    df_all = df_all.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)

    # 清理產業
    df_all["industry"] = df_all["industry"].fillna("").astype(str).str.strip()
    df_all.loc[df_all["industry"].isin(["nan", "None", ""]), "industry"] = "未分類"

    # 清理名稱
    df_all["stock_name"] = df_all["stock_name"].fillna("").astype(str).str.strip()

    # 欄位命名
    df_all = df_all.rename(columns={"stock_id_raw": "stock_id"})

    # 排序
    df_all = df_all.sort_values(["board", "stock_id"]).reset_index(drop=True)

    # 儲存
    STOCK_METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(STOCK_METADATA_PATH, index=False, encoding="utf-8-sig")

    print(f"✅ stock_metadata.csv 已輸出：{STOCK_METADATA_PATH}")
    print(df_all.head(10).to_string())
    print()
    print("✅ 板別分布")
    print(df_all["board"].value_counts(dropna=False).to_string())
    print()
    print("✅ 交易所大產業前 20 名")
    print(df_all["industry"].value_counts(dropna=False).head(20).to_string())

    return df_all


def main():
    universe_df = build_stock_metadata()
    print(f"📈 總股票數：{len(universe_df)}")


if __name__ == "__main__":
    main()
