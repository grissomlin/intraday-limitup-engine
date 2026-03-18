# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import io
import json
import os
import re
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "stocksymboldownload"
STOCK_METADATA_PATH = OUT_DIR / "stock_metadata.csv"

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
    dfs: List[pd.DataFrame] = []

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

    # 清理欄位
    df_all["industry"] = df_all["industry"].fillna("").astype(str).str.strip()
    df_all.loc[df_all["industry"].isin(["nan", "None", ""]), "industry"] = "未分類"
    df_all["stock_name"] = df_all["stock_name"].fillna("").astype(str).str.strip()

    # 改欄名
    df_all = df_all.rename(columns={"stock_id_raw": "stock_id"})

    # 排序
    df_all = df_all.sort_values(["board", "stock_id"]).reset_index(drop=True)

    # 輸出
    OUT_DIR.mkdir(parents=True, exist_ok=True)
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


def build_drive_service():
    token_b64 = os.environ.get("GDRIVE_TOKEN_B64", "").strip()
    if not token_b64:
        raise RuntimeError("缺少 GDRIVE_TOKEN_B64")

    token_json = base64.b64decode(token_b64).decode("utf-8")
    token_info = json.loads(token_json)

    creds = Credentials.from_authorized_user_info(token_info)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service


def find_drive_file(service, folder_id: str, file_name: str) -> str | None:
    query = (
        f"name = '{file_name}' and "
        f"'{folder_id}' in parents and "
        f"trashed = false"
    )

    res = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id,name)",
        pageSize=10,
    ).execute()

    files = res.get("files", [])
    if not files:
        return None
    return files[0]["id"]


def upload_or_update_drive_file(service, local_path: Path, folder_id: str):
    file_name = local_path.name
    file_id = find_drive_file(service, folder_id, file_name)

    with open(local_path, "rb") as f:
        media = MediaIoBaseUpload(
            io.BytesIO(f.read()),
            mimetype="text/csv",
            resumable=False,
        )

    if file_id:
        updated = service.files().update(
            fileId=file_id,
            media_body=media,
            fields="id,name",
        ).execute()
        print(f"✅ 已更新 Google Drive 檔案：{updated['name']} ({updated['id']})")
    else:
        created = service.files().create(
            body={
                "name": file_name,
                "parents": [folder_id],
            },
            media_body=media,
            fields="id,name",
        ).execute()
        print(f"✅ 已上傳 Google Drive 檔案：{created['name']} ({created['id']})")


def maybe_upload_to_google_drive():
    folder_id = os.environ.get("TW_STOCKLIST_FOLDER_ID", "").strip()

    if not folder_id:
        print("⚠️ 未設定 TW_STOCKLIST_FOLDER_ID，略過 Google Drive 上傳")
        return

    if not STOCK_METADATA_PATH.exists():
        raise RuntimeError(f"找不到要上傳的檔案：{STOCK_METADATA_PATH}")

    service = build_drive_service()
    upload_or_update_drive_file(service, STOCK_METADATA_PATH, folder_id)


def main():
    universe_df = build_stock_metadata()
    print()
    print(f"📈 總股票數：{len(universe_df)}")
    print()

    try:
        maybe_upload_to_google_drive()
    except Exception as e:
        print(f"❌ Google Drive 上傳失敗：{e}")
        raise


if __name__ == "__main__":
    main()
