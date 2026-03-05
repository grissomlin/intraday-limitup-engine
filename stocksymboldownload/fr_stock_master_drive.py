# -*- coding: utf-8 -*-
"""
FR Stock Master (daily) from MarketScreener + Google Drive upload (update-in-place)

Env required:
- GDRIVE_TOKEN_B64           : base64(json) of Google OAuth token (authorized_user_info)
- GDRIVE_FOLDER_ID           : Google Drive folder id to store output
  (backward compat: FR_STOCKLIST also accepted)

Optional env:
- FR_OUTPUT_NAME             : output csv name (default: FR_Stock_Master_Data.csv)
- FR_MAX_PAGES               : pages to scrape (default: 11)
- FR_SLEEP_SEC               : sleep seconds between pages (default: 3)
- FR_HTTP_TIMEOUT_SEC        : requests timeout (default: 20)
- FR_USER_AGENT              : override UA
"""

from __future__ import annotations

import base64
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


# =============================================================================
# Config
# =============================================================================
BASE_URL = "https://uk.marketscreener.com/stock-exchange/shares/europe/france-51/"
MS_HOME = "https://uk.marketscreener.com/"

GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID") or os.environ.get("FR_STOCKLIST")

OUTPUT_NAME = os.environ.get("FR_OUTPUT_NAME", "FR_Stock_Master_Data.csv")
MAX_PAGES = int(os.environ.get("FR_MAX_PAGES", "11"))
SLEEP_SEC = float(os.environ.get("FR_SLEEP_SEC", "3"))
HTTP_TIMEOUT = int(os.environ.get("FR_HTTP_TIMEOUT_SEC", "20"))
USER_AGENT = os.environ.get(
    "FR_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
)

REQ_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": MS_HOME,
}


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
    files.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return files[0]["id"]


def upload_csv_update_in_place(service, folder_id: str, file_name: str, df: pd.DataFrame) -> str:
    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)

    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=True)
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
    meta = {"name": file_name, "parents": [folder_id]}
    created = service.files().create(body=meta, media_body=media, supportsAllDrives=True).execute()
    return created["id"]


# =============================================================================
# Helpers
# =============================================================================
def _ms_full_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return MS_HOME.rstrip("/") + href
    return MS_HOME.rstrip("/") + "/" + href


def _clean_text(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()


def _guess_yf_symbol(symbol_local: str) -> str:
    """
    Naive guess for Euronext Paris in yfinance: <symbol>.PA
    We'll keep it as a 'guess' column; you can override later for mismatches.
    """
    sym = (symbol_local or "").strip().upper()
    if not sym:
        return ""
    # Some tickers contain spaces or weird chars; keep only common safe pattern
    sym2 = re.sub(r"[^A-Z0-9\.\-]", "", sym)
    if not sym2:
        return ""
    return f"{sym2}.PA"


# =============================================================================
# MarketScreener Scraper
# =============================================================================
def _find_first_table(soup: BeautifulSoup):
    for cls in ["std_tlist", "table", "tlist"]:
        t = soup.find("table", {"class": cls})
        if t:
            return t
    tables = soup.find_all("table")
    return tables[0] if tables else None


def scrape_page(p: int) -> List[Dict[str, str]]:
    url = f"{BASE_URL}?p={p}"
    print(f"正在抓取第 {p} 頁... ", end="")

    r = requests.get(url, headers=REQ_HEADERS, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        print(f"❌ status={r.status_code}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = _find_first_table(soup)
    if not table:
        print("❌ 找不到 table")
        return []

    rows = table.find_all("tr")
    if not rows:
        print("❌ table 無 rows")
        return []

    header_cells = rows[0].find_all(["th", "td"])
    col_names = [_clean_text(c.get_text(strip=True)) for c in header_cells]

    page_rows: List[Dict[str, str]] = []
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        values = [_clean_text(c.get_text(strip=True)) for c in cells]

        rec: Dict[str, str] = {}
        if col_names and len(values) == len(col_names):
            rec.update(dict(zip(col_names, values)))
        else:
            rec.update({f"col_{i}": v for i, v in enumerate(values)})

        # First link in row usually quote page
        a = row.find("a", href=True)
        if a:
            rec["ms_link"] = _ms_full_url(a["href"])
        else:
            rec["ms_link"] = ""

        # Try to capture symbol (often appears in one column; we keep raw and parse later)
        page_rows.append(rec)

    print(f"✅ {len(page_rows)} 筆")
    return page_rows


def normalize_records(records: List[Dict[str, str]]) -> pd.DataFrame:
    """
    Convert to a stable schema:
    - symbol_local, name, sector, ms_link
    Keep extra columns for debug.
    """
    if not records:
        return pd.DataFrame()

    # Determine best columns by header hints
    keys = list(records[0].keys())

    def pick_key(preds: List[str]) -> Optional[str]:
        for k in keys:
            lk = str(k).lower()
            for p in preds:
                if p in lk:
                    return k
        return None

    # On MarketScreener, columns vary; we try to map:
    # - "Name" / empty header / "Add to a list" column -> name (often includes company name)
    # - "Sector" -> sector
    # - sometimes there is a "Symbol" column; if not, we'll attempt parse from name col later
    name_key = pick_key(["name"]) or pick_key(["add to"])  # fallback
    sector_key = pick_key(["sector"])
    symbol_key = pick_key(["symbol", "ticker", "code"])

    rows: List[Dict[str, str]] = []
    for rec in records:
        name = _clean_text(rec.get(name_key, "")) if name_key else ""
        sector = _clean_text(rec.get(sector_key, "")) if sector_key else ""
        symbol = _clean_text(rec.get(symbol_key, "")) if symbol_key else ""

        ms_link = _clean_text(rec.get("ms_link", ""))

        # If symbol not found, try infer from ms_link tail (sometimes quote pages include symbol/name)
        # We keep blank if cannot.
        rows.append(
            {
                "name": name,
                "sector": sector,
                "symbol_local": symbol,
                "ms_link": ms_link,
                # keep original for debugging if needed
                "_raw": json.dumps(rec, ensure_ascii=False),
            }
        )

    df = pd.DataFrame(rows)

    # Filter junk rows
    df = df[df["name"].astype(str).str.strip().ne("")]
    df = df[~df["name"].astype(str).str.contains("Add to a list", na=False)]

    # Dedup by (ms_link) preferred, then name
    if "ms_link" in df.columns:
        df = df.drop_duplicates(subset=["ms_link"], keep="first")
    df = df.drop_duplicates(subset=["name"], keep="first").reset_index(drop=True)

    # Build yfinance guess / override placeholder
    df["symbol_local"] = df["symbol_local"].astype(str).str.strip().str.upper()
    df["yf_symbol_guess"] = df["symbol_local"].apply(_guess_yf_symbol)
    df["yf_symbol_override"] = ""  # fill later for the ~30 mismatches
    df["yf_symbol"] = df.apply(
        lambda r: (r["yf_symbol_override"].strip() or r["yf_symbol_guess"].strip()),
        axis=1,
    )

    # asof
    df.insert(0, "asof_utc", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    # order
    cols = [
        "asof_utc",
        "symbol_local",
        "yf_symbol",
        "yf_symbol_guess",
        "yf_symbol_override",
        "name",
        "sector",
        "ms_link",
        "_raw",
    ]
    cols2 = [c for c in cols if c in df.columns]
    df = df[cols2]

    return df


def run():
    if not GDRIVE_FOLDER_ID:
        print("❌ 錯誤: 找不到 GDRIVE_FOLDER_ID / FR_STOCKLIST (Google Drive 資料夾 ID)")
        sys.exit(1)

    service = get_drive_service()

    print("=" * 60)
    print(f"FR Stock Master | pages={MAX_PAGES} | out={OUTPUT_NAME}")
    print("=" * 60)

    all_rows: List[Dict[str, str]] = []

    first_row_fingerprint: Optional[str] = None

    for p in range(1, MAX_PAGES + 1):
        data = scrape_page(p)
        if not data:
            print(f"⚠️  第 {p} 頁無資料，停止")
            break

        # Detect repeated page (when blocked/free-limit), similar to your logic
        fp = ""
        try:
            fp = json.dumps(data[0], sort_keys=True, ensure_ascii=False)
        except Exception:
            fp = str(list(data[0].values())[:5])

        if p == 1:
            first_row_fingerprint = fp
        else:
            if first_row_fingerprint and fp == first_row_fingerprint:
                print(f"⚠️  第 {p} 頁與第 1 頁相同，疑似被擋/免費上限，停止")
                break

        all_rows.extend(data)
        if p < MAX_PAGES:
            time.sleep(SLEEP_SEC)

    if not all_rows:
        print("❌ 沒有抓到任何資料")
        sys.exit(2)

    df = normalize_records(all_rows)

    # Basic stats
    empty_sector = int(df["sector"].astype(str).str.strip().eq("").sum())
    empty_symbol = int(df["symbol_local"].astype(str).str.strip().eq("").sum())

    print("📊 normalize 完成")
    print(f"   rows={len(df)} empty_sector={empty_sector} empty_symbol_local={empty_symbol}")

    print("📤 上傳到 Google Drive ...")
    file_id = upload_csv_update_in_place(service, GDRIVE_FOLDER_ID, OUTPUT_NAME, df)

    print("✅ 任務完成！")
    print(f"   Drive fileId: {file_id}")
    print(f"   Rows: {len(df)}")
    print("   Head:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    run()
