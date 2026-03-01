# -*- coding: utf-8 -*-
"""
NSE Stock Master (daily) + sector/industry cache (only fill missing) + Google Drive upload (update-in-place)

Env required:
- GDRIVE_TOKEN_B64           : base64(json) of Google OAuth token (authorized_user_info)
- GDRIVE_FOLDER_ID           : Google Drive folder id to store output
  (backward compat: IN_STOCKLIST also accepted)

Optional env:
- NSE_SECTOR_CACHE_PATH      : cache file path (default: data/cache/nse_sector_cache.csv)
- NSE_OUTPUT_NAME            : output csv name (default: NSE_Stock_Master_Data.csv)
- NSE_SLEEP_SEC              : yfinance per-ticker sleep (default: 0.12)
- NSE_MAX_RETRY              : yfinance retry count (default: 3)
- NSE_RETRY_BACKOFF_SEC      : base backoff seconds (default: 0.6)
"""

from __future__ import annotations

import base64
import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


# =============================================================================
# Config
# =============================================================================
URL_EQUITY_L = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
URL_SEC_LIST = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"
NSE_HOME = "https://www.nseindia.com/"

GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID") or os.environ.get("IN_STOCKLIST")

CACHE_PATH = Path(os.environ.get("NSE_SECTOR_CACHE_PATH", "data/cache/nse_sector_cache.csv"))
OUTPUT_NAME = os.environ.get("NSE_OUTPUT_NAME", "NSE_Stock_Master_Data.csv")

SLEEP_SEC = float(os.environ.get("NSE_SLEEP_SEC", "0.12"))
MAX_RETRY = int(os.environ.get("NSE_MAX_RETRY", "3"))
BACKOFF_BASE = float(os.environ.get("NSE_RETRY_BACKOFF_SEC", "0.6"))


# =============================================================================
# Google Drive
# =============================================================================
def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("❌ 找不到 GDRIVE_TOKEN_B64 環境變數")

    try:
        decoded_data = base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8")
        token_info = json.loads(decoded_data)
    except Exception as e:
        raise ValueError(f"❌ Base64 解碼或 JSON 解析失敗: {e}")

    try:
        creds = Credentials.from_authorized_user_info(token_info)
        if creds.expired and creds.refresh_token:
            print("🔄 Token 已過期，嘗試自動刷新...")
            creds.refresh(Request())
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        raise ValueError(f"❌ 憑證初始化失敗: {e}")


def find_file_id(service, file_name: str, folder_id: str) -> Optional[str]:
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    res = (
        service.files()
        .list(
            q=query,
            fields="files(id, name, modifiedTime)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = res.get("files", [])
    if not files:
        return None
    # If multiple, choose the latest modified
    files.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return files[0]["id"]


def upload_csv_update_in_place(service, folder_id: str, file_name: str, df: pd.DataFrame) -> str:
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    csv_buffer.seek(0)

    media = MediaIoBaseUpload(csv_buffer, mimetype="text/csv", resumable=True)
    existing_id = find_file_id(service, file_name, folder_id)

    if existing_id:
        print(f"♻️ Drive 同名檔案已存在，進行 update：{file_name} (fileId={existing_id})")
        service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        return existing_id

    print(f"📤 Drive 無同名檔案，進行 create：{file_name}")
    file_metadata = {"name": file_name, "parents": [folder_id]}
    created = (
        service.files()
        .create(body=file_metadata, media_body=media, supportsAllDrives=True)
        .execute()
    )
    return created["id"]


# =============================================================================
# NSE Fetch (more stable)
# =============================================================================
def _make_nse_session() -> requests.Session:
    s = requests.Session()
    # Keep headers close to a normal browser request
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": NSE_HOME,
        }
    )
    return s


def _nse_get_bytes(session: requests.Session, url: str, *, timeout: int = 30) -> bytes:
    # First hit NSE home to get cookies (important for avoiding 403)
    try:
        session.get(NSE_HOME, timeout=timeout)
    except Exception:
        # Even if homepage fails, still try the CSV
        pass

    # CSV fetch
    resp = session.get(url, timeout=timeout, headers={"Accept": "text/csv,*/*;q=0.8"})
    if resp.status_code != 200 or not resp.content:
        raise RuntimeError(f"NSE 下載失敗: {url} status={resp.status_code} len={len(resp.content or b'')}")
    return resp.content


def fetch_nse_frames() -> Tuple[pd.DataFrame, pd.DataFrame]:
    s = _make_nse_session()
    print("📥 正在從 NSE 下載 EQUITY_L.csv / sec_list.csv ...")
    b1 = _nse_get_bytes(s, URL_EQUITY_L)
    b2 = _nse_get_bytes(s, URL_SEC_LIST)

    df_base = pd.read_csv(io.BytesIO(b1))
    df_band = pd.read_csv(io.BytesIO(b2))

    df_base.columns = df_base.columns.str.strip()
    df_band.columns = df_band.columns.str.strip()
    return df_base, df_band


# =============================================================================
# Sector/Industry cache (only fill missing)
# =============================================================================
def load_cache(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            df = pd.read_csv(path)
            # Normalize columns
            for c in ["SYMBOL", "sector", "industry"]:
                if c not in df.columns:
                    raise ValueError(f"cache 欄位缺少: {c}")
            df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
            df["sector"] = df["sector"].astype(str)
            df["industry"] = df["industry"].astype(str)
            return df[["SYMBOL", "sector", "industry"]].drop_duplicates("SYMBOL", keep="last")
        except Exception as e:
            print(f"⚠️ 讀取 cache 失敗，將重建 cache：{path} err={e}")
    return pd.DataFrame(columns=["SYMBOL", "sector", "industry"])


def save_cache(path: Path, df_cache: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df_cache = df_cache.copy()
    df_cache["SYMBOL"] = df_cache["SYMBOL"].astype(str).str.strip().str.upper()
    df_cache = df_cache.drop_duplicates("SYMBOL", keep="last")
    df_cache.to_csv(path, index=False, encoding="utf-8-sig")


def yf_fetch_sector_industry(symbol: str) -> Tuple[str, str]:
    """
    Return (sector, industry).
    Retry with backoff for transient issues.
    """
    ticker = f"{symbol}.NS"
    last_err = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            info = yf.Ticker(ticker).info  # network call
            sector = info.get("sector") or "Unclassified"
            industry = info.get("industry") or "Unclassified"
            return sector, industry
        except Exception as e:
            last_err = e
            sleep = BACKOFF_BASE * (2 ** (attempt - 1))
            time.sleep(sleep)
    # After retries
    return "Error", "Error"


def build_sector_industry(df_symbols: pd.DataFrame, cache_path: Path) -> pd.DataFrame:
    """
    df_symbols must have column SYMBOL (upper).
    Only fetch missing symbols not in cache.
    """
    cache = load_cache(cache_path)
    cached_set = set(cache["SYMBOL"].astype(str).str.upper())

    symbols = df_symbols["SYMBOL"].astype(str).str.strip().str.upper()
    missing = [s for s in symbols.tolist() if s and s not in cached_set]

    print(f"🧠 sector/industry cache：已有 {len(cached_set)} 筆，需補 {len(missing)} 筆")
    new_rows = []

    for i, sym in enumerate(missing, 1):
        if i == 1 or i % 50 == 0:
            print(f"  進度: {i}/{len(missing)} (補缺 sector/industry)")
        sector, industry = yf_fetch_sector_industry(sym)
        new_rows.append({"SYMBOL": sym, "sector": sector, "industry": industry})
        time.sleep(SLEEP_SEC)

    if new_rows:
        cache2 = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
        save_cache(cache_path, cache2)
        print(f"💾 cache 已更新：{cache_path} (+{len(new_rows)})")
        return cache2[["SYMBOL", "sector", "industry"]].drop_duplicates("SYMBOL", keep="last")

    return cache[["SYMBOL", "sector", "industry"]].drop_duplicates("SYMBOL", keep="last")


# =============================================================================
# Main
# =============================================================================
def run():
    try:
        if not GDRIVE_FOLDER_ID:
            print("❌ 錯誤: 找不到 GDRIVE_FOLDER_ID / IN_STOCKLIST (Google Drive 資料夾 ID)")
            sys.exit(1)

        service = get_drive_service()

        # 1) fetch NSE files
        df_base, df_band = fetch_nse_frames()

        # 2) merge band into base
        print("🔗 正在整合 Price Band ...")
        # Normalize columns
        df_base["SYMBOL"] = df_base["SYMBOL"].astype(str).str.strip().str.upper()
        if "NAME OF COMPANY" in df_base.columns:
            pass
        else:
            # defensive
            raise ValueError("EQUITY_L.csv 欄位缺少: NAME OF COMPANY")

        df_band["Symbol"] = df_band["Symbol"].astype(str).str.strip().str.upper()

        df_merged = (
            pd.merge(
                df_base[["SYMBOL", "NAME OF COMPANY"]],
                df_band[["Symbol", "Band", "Remarks"]],
                left_on="SYMBOL",
                right_on="Symbol",
                how="left",
            )
            .drop(columns=["Symbol"])
        )

        # 3) sector/industry cache only fill missing
        print("🔍 只補缺：抓取 yfinance 行業資訊 ...")
        df_cache = build_sector_industry(df_merged[["SYMBOL"]], CACHE_PATH)

        # 4) final merge
        df_final = pd.merge(df_merged, df_cache, on="SYMBOL", how="left")

        # Optional metadata columns (useful long-term)
        df_final.insert(0, "asof_utc", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

        # 5) upload to drive (update in place)
        print("📤 上傳到 Google Drive ...")
        file_id = upload_csv_update_in_place(service, GDRIVE_FOLDER_ID, OUTPUT_NAME, df_final)

        # 6) small stats
        empty_sector = int(df_final["sector"].isna().sum())
        empty_industry = int(df_final["industry"].isna().sum())
        err_cnt = int(((df_final["sector"] == "Error") | (df_final["industry"] == "Error")).sum())
        unclassified_cnt = int(((df_final["sector"] == "Unclassified") | (df_final["industry"] == "Unclassified")).sum())

        print("✅ 任務完成！")
        print(f"   Drive fileId: {file_id}")
        print(f"   Rows: {len(df_final)} | empty_sector={empty_sector} empty_industry={empty_industry} Error={err_cnt} Unclassified={unclassified_cnt}")
        print(f"   cache: {CACHE_PATH}")

    except Exception as e:
        print(f"❌ 執行過程中發生錯誤: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
