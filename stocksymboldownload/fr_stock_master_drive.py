# -*- coding: utf-8 -*-
"""
FR Stock Master (daily) - combine:
- stockanalysis.com Euronext Paris list (gives symbol -> yfinance ticker)
- MarketScreener France list pages (gives sector)
Then merge by normalized company name, with fuzzy fallback.
Upload merged CSV to Google Drive (update-in-place).

Env required:
- GDRIVE_TOKEN_B64
- GDRIVE_FOLDER_ID (or FR_STOCKLIST)

Optional env:
- FR_OUTPUT_NAME              default: FR_Stock_Master_Data.csv
- FR_SA_PAGES                 stockanalysis pages (default 2)
- FR_MS_PAGES                 marketscreener pages (default 11)
- FR_HTTP_TIMEOUT_SEC         default 20
- FR_SLEEP_SEC                default 2
- FR_FUZZY_CUTOFF             default 0.90  (difflib similarity)
"""

from __future__ import annotations

import base64
import io
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


# =============================================================================
# Config
# =============================================================================
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID") or os.environ.get("FR_STOCKLIST")

OUTPUT_NAME = os.environ.get("FR_OUTPUT_NAME", "FR_Stock_Master_Data.csv")

SA_PAGES = int(os.environ.get("FR_SA_PAGES", "2"))
MS_PAGES = int(os.environ.get("FR_MS_PAGES", "11"))

HTTP_TIMEOUT = int(os.environ.get("FR_HTTP_TIMEOUT_SEC", "20"))
SLEEP_SEC = float(os.environ.get("FR_SLEEP_SEC", "2"))
FUZZY_CUTOFF = float(os.environ.get("FR_FUZZY_CUTOFF", "0.90"))

USER_AGENT = os.environ.get(
    "FR_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}

SA_BASE = "https://stockanalysis.com/list/euronext-paris/?page={p}"
MS_BASE = "https://uk.marketscreener.com/stock-exchange/shares/europe/france-51/?p={p}"
MS_HOME = "https://uk.marketscreener.com/"


# =============================================================================
# Google Drive
# =============================================================================
def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("❌ 找不到 GDRIVE_TOKEN_B64 環境變數")

    decoded_data = base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8")
    token_info = json.loads(decoded_data)

    creds = Credentials.from_authorized_user_info(token_info)
    if creds.expired and creds.refresh_token:
        print("🔄 Token 已過期，嘗試自動刷新...")
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


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
        service.files().update(fileId=existing_id, media_body=media, supportsAllDrives=True).execute()
        return existing_id

    print(f"📤 Drive 無同名檔案，進行 create：{file_name}")
    meta = {"name": file_name, "parents": [folder_id]}
    created = service.files().create(body=meta, media_body=media, supportsAllDrives=True).execute()
    return created["id"]


# =============================================================================
# Normalization
# =============================================================================
LEGAL_SUFFIXES = [
    "S.A.", "SA", "SE", "SOCIETE ANONYME", "SOCIÉTÉ ANONYME",
    "SCA", "S.C.A.", "NV", "N.V.", "PLC", "LTD", "LIMITED",
    "GROUPE", "GROUP",
]

def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm_name(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = strip_accents(s)
    s = s.upper()

    # remove bracket content
    s = re.sub(r"\([^)]*\)", " ", s)

    # normalize apostrophes etc.
    s = s.replace("’", "'")

    # remove legal suffixes tokens
    for suf in LEGAL_SUFFIXES:
        suf2 = strip_accents(suf).upper()
        s = re.sub(rf"\b{re.escape(suf2)}\b", " ", s)

    # keep alnum + spaces
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


# =============================================================================
# Scrape stockanalysis (symbol + company)
# =============================================================================
def scrape_stockanalysis() -> pd.DataFrame:
    all_records: List[Dict[str, str]] = []
    for p in range(1, SA_PAGES + 1):
        url = SA_BASE.format(p=p)
        print(f"📄 stockanalysis page {p}: {url}")
        r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        r.encoding = "utf-8"
        if r.status_code != 200:
            raise RuntimeError(f"stockanalysis failed: status={r.status_code}")

        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            raise RuntimeError("stockanalysis: no table found")

        rows = table.find_all("tr")
        header = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            vals = [c.get_text(strip=True) for c in cells]
            if not any(vals):
                continue
            rec = dict(zip(header, vals))
            all_records.append(rec)

        if p < SA_PAGES:
            time.sleep(SLEEP_SEC)

    df = pd.DataFrame(all_records)
    # standardize
    rename_map = {
        "No.": "no",
        "Symbol": "symbol",
        "Company Name": "company_name",
        "Market Cap": "market_cap",
        "Stock Price": "stock_price",
        "% Change": "pct_change",
        "Revenue": "revenue",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["yf_symbol"] = df["symbol"].apply(lambda s: f"{s}.PA" if s and s != "NAN" else "")
    df["ticker"] = df["symbol"]

    df["company_name"] = df["company_name"].astype(str).str.strip()
    df["name_key"] = df["company_name"].apply(norm_name)

    df = df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    print(f"✅ stockanalysis rows={len(df)} (has symbol={int(df['symbol'].ne('').sum())})")
    return df


# =============================================================================
# Scrape MarketScreener list (name + sector)
# =============================================================================
def _ms_full_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return MS_HOME.rstrip("/") + href
    return MS_HOME.rstrip("/") + "/" + href


def _find_first_table(soup: BeautifulSoup):
    for cls in ["std_tlist", "table", "tlist", "screener-table"]:
        t = soup.find("table", {"class": cls})
        if t:
            return t
    tables = soup.find_all("table")
    return tables[0] if tables else None


def scrape_marketscreener_sector() -> pd.DataFrame:
    rows_out: List[Dict[str, str]] = []
    for p in range(1, MS_PAGES + 1):
        url = MS_BASE.format(p=p)
        print(f"📄 marketscreener page {p}: {url}")
        r = requests.get(url, headers={**HEADERS, "Referer": MS_HOME}, timeout=HTTP_TIMEOUT)
        r.encoding = "utf-8"
        if r.status_code != 200:
            print(f"  ⚠️ status={r.status_code}, stop")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        table = _find_first_table(soup)
        if not table:
            print("  ⚠️ no table, stop")
            break

        trs = table.find_all("tr")
        if len(trs) < 2:
            print("  ⚠️ no rows, stop")
            break

        # header
        header = [th.get_text(strip=True) for th in trs[0].find_all(["th", "td"])]
        header_lower = [h.lower() for h in header]

        # find sector column
        sector_idx = None
        for i, h in enumerate(header_lower):
            if "sector" in h:
                sector_idx = i
                break

        for tr in trs[1:]:
            tds = tr.find_all(["td", "th"])
            vals = [td.get_text(strip=True) for td in tds]
            if not any(vals):
                continue

            # first link is quote page
            a = tr.find("a", href=True)
            ms_link = _ms_full_url(a["href"]) if a else ""

            # name: pick first non-empty cell (works well on this list)
            name = ""
            for v in vals:
                if v and "Add to a list" not in v:
                    name = v
                    break

            sector = ""
            if sector_idx is not None and sector_idx < len(vals):
                sector = vals[sector_idx]

            name = (name or "").strip()
            sector = (sector or "").strip()
            if not name:
                continue

            rows_out.append(
                {
                    "ms_name": name,
                    "ms_sector": sector,
                    "ms_link": ms_link,
                    "ms_name_key": norm_name(name),
                }
            )

        if p < MS_PAGES:
            time.sleep(SLEEP_SEC)

    df = pd.DataFrame(rows_out)
    df = df.drop_duplicates(subset=["ms_name_key"], keep="first").reset_index(drop=True)
    print(f"✅ marketscreener rows={len(df)} (sector non-empty={int(df['ms_sector'].astype(str).str.strip().ne('').sum())})")
    return df


# =============================================================================
# Merge logic: exact on normalized name, then fuzzy fallback
# =============================================================================
def merge_sa_ms(df_sa: pd.DataFrame, df_ms: pd.DataFrame) -> pd.DataFrame:
    df = df_sa.copy()

    # exact match first
    ms_map = dict(zip(df_ms["ms_name_key"], df_ms["ms_sector"]))
    df["sector"] = df["name_key"].map(lambda k: ms_map.get(k, "")).fillna("")

    unmatched = df["sector"].astype(str).str.strip().eq("")
    unmatched_idx = df.index[unmatched].tolist()
    print(f"🔎 merge exact done. unmatched={len(unmatched_idx)}")

    if not unmatched_idx:
        return df

    # fuzzy fallback: build candidate list once
    ms_keys = df_ms["ms_name_key"].astype(str).tolist()
    ms_sector_map = dict(zip(df_ms["ms_name_key"], df_ms["ms_sector"]))

    fixed = 0
    for i in unmatched_idx:
        key = str(df.at[i, "name_key"])
        if not key:
            continue

        best_k = ""
        best_s = 0.0
        # small optimization: compare only candidates sharing first token
        first = key.split(" ")[0]
        candidates = [k for k in ms_keys if k.startswith(first)] or ms_keys

        for k in candidates:
            sc = sim(key, k)
            if sc > best_s:
                best_s = sc
                best_k = k

        if best_s >= FUZZY_CUTOFF and best_k:
            df.at[i, "sector"] = ms_sector_map.get(best_k, "")
            fixed += 1

    print(f"✅ fuzzy filled: {fixed}")
    return df


# =============================================================================
# Main
# =============================================================================
def run():
    if not GDRIVE_FOLDER_ID:
        print("❌ 找不到 GDRIVE_FOLDER_ID / FR_STOCKLIST")
        sys.exit(1)

    print("=" * 70)
    print(f"FR Stock Master (merge) | SA pages={SA_PAGES} | MS pages={MS_PAGES} | out={OUTPUT_NAME}")
    print("=" * 70)

    df_sa = scrape_stockanalysis()
    df_ms = scrape_marketscreener_sector()

    df = merge_sa_ms(df_sa, df_ms)

    df.insert(0, "asof_utc", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    # final columns
    out_cols = [
        "asof_utc",
        "ticker",
        "yf_symbol",
        "symbol",
        "company_name",
        "sector",
        "market_cap",
        "stock_price",
        "pct_change",
        "revenue",
        "name_key",
    ]
    out_cols = [c for c in out_cols if c in df.columns]
    df_out = df[out_cols].copy()

    empty_sector = int(df_out["sector"].astype(str).str.strip().eq("").sum())
    print(f"📊 final rows={len(df_out)} empty_sector={empty_sector}")

    service = get_drive_service()
    print("📤 上傳到 Google Drive ...")
    file_id = upload_csv_update_in_place(service, GDRIVE_FOLDER_ID, OUTPUT_NAME, df_out)

    print("✅ 任務完成！")
    print(f"   Drive fileId: {file_id}")
    print(df_out.head(10).to_string(index=False))


if __name__ == "__main__":
    run()
