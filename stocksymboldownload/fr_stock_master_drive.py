# -*- coding: utf-8 -*-
"""
FR Stock Master (daily) from MarketScreener + quote-page ticker resolver (cached)
+ Google Drive upload (update-in-place)

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

Ticker resolve + cache:
- FR_TICKER_CACHE_PATH       : cache file path (default: data/cache/fr_ticker_cache.csv)
- FR_RESOLVE_MAX             : max quote pages to resolve per run (default: 120)
- FR_RESOLVE_SLEEP_SEC       : sleep seconds between quote resolves (default: 0.25)
- FR_MAX_RETRY               : retry count for quote page fetch (default: 3)

Overrides (optional):
- FR_OVERRIDE_PATH           : csv path with overrides (default: data/overrides/fr_yf_override.csv)
                               columns:
                                 - ms_link,yf_symbol_override   OR
                                 - isin,yf_symbol_override
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
from pathlib import Path
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

# resolve + cache
CACHE_PATH = Path(os.environ.get("FR_TICKER_CACHE_PATH", "data/cache/fr_ticker_cache.csv"))
RESOLVE_MAX = int(os.environ.get("FR_RESOLVE_MAX", "120"))
RESOLVE_SLEEP = float(os.environ.get("FR_RESOLVE_SLEEP_SEC", "0.25"))
MAX_RETRY = int(os.environ.get("FR_MAX_RETRY", "3"))

# overrides
OVERRIDE_PATH = Path(os.environ.get("FR_OVERRIDE_PATH", "data/overrides/fr_yf_override.csv"))

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


# =============================================================================
# List scraper
# =============================================================================
def _find_first_table(soup: BeautifulSoup):
    for cls in ["std_tlist", "table", "tlist", "screener-table"]:
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

        a = row.find("a", href=True)
        rec["ms_link"] = _ms_full_url(a["href"]) if a else ""

        if any(values):
            page_rows.append(rec)

    print(f"✅ {len(page_rows)} 筆")
    return page_rows


def normalize_list_records(records: List[Dict[str, str]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    keys = list(records[0].keys())

    def pick_key(preds: List[str]) -> Optional[str]:
        for k in keys:
            lk = str(k).lower()
            for p in preds:
                if p in lk:
                    return k
        return None

    name_key = pick_key(["name"]) or pick_key(["add to"]) or pick_key(["col_1"]) or keys[0]
    sector_key = pick_key(["sector"])

    rows: List[Dict[str, str]] = []
    for rec in records:
        name = _clean_text(rec.get(name_key, "")) if name_key else ""
        sector = _clean_text(rec.get(sector_key, "")) if sector_key else ""
        ms_link = _clean_text(rec.get("ms_link", ""))

        rows.append(
            {
                "name": name,
                "sector": sector,
                "ms_link": ms_link,
                "_raw": json.dumps(rec, ensure_ascii=False),
            }
        )

    df = pd.DataFrame(rows)
    df = df[df["name"].astype(str).str.strip().ne("")]
    df = df[~df["name"].astype(str).str.contains("Add to a list", na=False)]

    if "ms_link" in df.columns:
        df = df.drop_duplicates(subset=["ms_link"], keep="first")
    df = df.drop_duplicates(subset=["name"], keep="first").reset_index(drop=True)

    df.insert(0, "asof_utc", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    # ensure columns for later steps
    df["ticker"] = ""
    df["isin"] = ""
    df["yf_symbol_override"] = ""
    return df


# =============================================================================
# Quote-page resolve (DOM-first) + cache
# =============================================================================
def load_ticker_cache(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            df = pd.read_csv(path)
            for c in ["ms_link", "ticker", "isin"]:
                if c not in df.columns:
                    raise ValueError(f"cache 欄位缺少: {c}")
            df["ms_link"] = df["ms_link"].astype(str).str.strip()
            df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
            df["isin"] = df["isin"].astype(str).str.strip().str.upper()
            # keep last
            df = df.drop_duplicates("ms_link", keep="last").reset_index(drop=True)
            return df
        except Exception as e:
            print(f"⚠️ 讀取 ticker cache 失敗，將重建: {path} err={e}")
    return pd.DataFrame(columns=["asof_utc", "ms_link", "ticker", "isin"])


def save_ticker_cache(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df2 = df.copy()
    df2["ms_link"] = df2["ms_link"].astype(str).str.strip()
    df2["ticker"] = df2["ticker"].astype(str).str.strip().str.upper()
    df2["isin"] = df2["isin"].astype(str).str.strip().str.upper()
    df2 = df2.drop_duplicates("ms_link", keep="last")
    df2.to_csv(path, index=False, encoding="utf-8-sig")


def _fetch_quote_html(url: str) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            r = requests.get(url, headers=REQ_HEADERS, timeout=HTTP_TIMEOUT)
            if r.status_code == 200 and r.text:
                return r.text
            last_err = RuntimeError(f"status={r.status_code}")
        except Exception as e:
            last_err = e
        time.sleep(0.6 * (2 ** (attempt - 1)))
    raise RuntimeError(f"quote fetch failed: {url} err={last_err}")


def _pick_value_from_kv_table(soup: BeautifulSoup, labels: List[str]) -> str:
    """
    Find value next to label in common key-value tables.
    Example:
      <tr><td>ISIN</td><td>FR0000121014</td></tr>
      <tr><th>Mnemonic</th><td>MC</td></tr>
    """
    for lab in labels:
        node = soup.find(string=re.compile(rf"^\s*{re.escape(lab)}\s*$", flags=re.I))
        if not node:
            continue
        cell = node.find_parent(["td", "th"])
        if not cell:
            continue
        row = cell.find_parent("tr")
        if not row:
            continue
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        try:
            idx = cells.index(cell)
        except ValueError:
            idx = 0
        if idx + 1 < len(cells):
            val = _clean_text(cells[idx + 1].get_text(" ", strip=True))
            if val:
                return val
    return ""


def parse_ticker_isin_from_quote(html: str) -> Tuple[str, str]:
    """
    Robust parse ticker(=mnemo) and ISIN from MarketScreener quote page.
    Priority:
      1) DOM table lookup
      2) HTML regex fallback
      3) Text fallback
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) DOM lookup
    isin = _pick_value_from_kv_table(soup, ["ISIN"])
    ticker = _pick_value_from_kv_table(soup, ["Mnemonic", "Mnemo", "Ticker", "Symbol", "Stock symbol"])

    isin = (isin or "").strip().upper()
    ticker = (ticker or "").strip().upper()

    # 2) HTML regex fallback
    if not isin:
        m = re.search(r'data-isin\s*=\s*"([A-Z]{2}[A-Z0-9]{10})"', html, flags=re.I)
        if m:
            isin = m.group(1).upper()

    if not ticker:
        for pat in [
            r'"mnemo"\s*:\s*"([^"]+)"',
            r'"mnemonic"\s*:\s*"([^"]+)"',
            r'"symbol"\s*:\s*"([^"]+)"',
            r'"ticker"\s*:\s*"([^"]+)"',
        ]:
            m = re.search(pat, html, flags=re.I)
            if m:
                ticker = m.group(1).strip().upper()
                break

    # 3) Text fallback
    if not isin or not ticker:
        text = soup.get_text(" ", strip=True)

        if not isin:
            m = re.search(r"\bISIN\b\s*[:\-]?\s*([A-Z]{2}[A-Z0-9]{10})\b", text, flags=re.I)
            if m:
                isin = m.group(1).upper()

        if not ticker:
            m = re.search(
                r"\b(Mnemo|Mnemonic|Ticker|Symbol)\b\s*[:\-]?\s*([A-Z0-9\.\-]{1,12})\b",
                text,
                flags=re.I,
            )
            if m:
                ticker = m.group(2).upper()

    ticker = re.sub(r"[^A-Z0-9\.\-]", "", ticker).strip().upper()
    isin = re.sub(r"[^A-Z0-9]", "", isin).strip().upper()
    return ticker, isin


def resolve_missing_tickers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # apply cache first
    cache = load_ticker_cache(CACHE_PATH)
    cache_map = {r["ms_link"]: (r.get("ticker", ""), r.get("isin", "")) for _, r in cache.iterrows()}

    for i in range(len(df)):
        link = str(df.at[i, "ms_link"]).strip()
        if link and link in cache_map:
            t, isin = cache_map[link]
            df.at[i, "ticker"] = (t or "").strip().upper()
            df.at[i, "isin"] = (isin or "").strip().upper()

    need = df["ticker"].astype(str).str.strip().eq("") & df["ms_link"].astype(str).str.strip().ne("")
    missing_idx = df.index[need].tolist()

    if not missing_idx:
        print(f"✅ ticker cache 命中：不需補抓 (cache_rows={len(cache)})")
        return df

    todo = missing_idx[:RESOLVE_MAX]
    print(f"🔎 需要補抓 ticker：{len(missing_idx)} 筆，本次處理 {len(todo)} 筆 (FR_RESOLVE_MAX={RESOLVE_MAX})")

    new_cache_rows = []
    for k, i in enumerate(todo, 1):
        link = str(df.at[i, "ms_link"]).strip()
        name = str(df.at[i, "name"]).strip()
        print(f"  [{k}/{len(todo)}] resolve: {name} | {link}")

        try:
            html = _fetch_quote_html(link)

            # member wall / blocked detection
            low = html.lower()
            if ("must be a member" in low) or ("log in" in low and "sign up" in low):
                print("    ⚠️ member wall / blocked page, skip")
                continue

            ticker, isin = parse_ticker_isin_from_quote(html)

            if ticker:
                df.at[i, "ticker"] = ticker
            if isin:
                df.at[i, "isin"] = isin

            # ✅ IMPORTANT: only cache when we found something (avoid empty pollution)
            if (ticker or "").strip() or (isin or "").strip():
                new_cache_rows.append(
                    {
                        "asof_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "ms_link": link,
                        "ticker": ticker,
                        "isin": isin,
                    }
                )
            else:
                print("    ⚠️ parsed empty ticker/isin (skip cache)")

        except Exception as e:
            print(f"    ⚠️ resolve failed: {e}")

        time.sleep(RESOLVE_SLEEP)

    if new_cache_rows:
        cache2 = pd.concat([cache, pd.DataFrame(new_cache_rows)], ignore_index=True)
        save_ticker_cache(CACHE_PATH, cache2)
        print(f"💾 ticker cache 已更新：{CACHE_PATH} (+{len(new_cache_rows)})")
    else:
        print("ℹ️ 本次沒有成功解析到 ticker/isin，因此 cache 未更新")

    return df


# =============================================================================
# Overrides (optional)
# =============================================================================
def apply_overrides(df: pd.DataFrame, path: Path) -> pd.DataFrame:
    if not path.exists():
        return df

    try:
        ov = pd.read_csv(path)
    except Exception as e:
        print(f"⚠️ overrides 讀取失敗，略過: {path} err={e}")
        return df

    cols = {c.lower(): c for c in ov.columns}
    if "yf_symbol_override" not in cols:
        print(f"⚠️ overrides 缺少 yf_symbol_override 欄位，略過: {path}")
        return df

    override_col = cols["yf_symbol_override"]

    df = df.copy()
    df["yf_symbol_override"] = df.get("yf_symbol_override", "")
    df["yf_symbol_override"] = df["yf_symbol_override"].astype(str).str.strip()

    if "ms_link" in cols:
        key = cols["ms_link"]
        ov2 = ov[[key, override_col]].copy()
        ov2[key] = ov2[key].astype(str).str.strip()
        ov2[override_col] = ov2[override_col].astype(str).str.strip()
        ov2 = ov2[ov2[key].ne("") & ov2[override_col].ne("")]
        if not ov2.empty:
            m = dict(zip(ov2[key], ov2[override_col]))
            df["yf_symbol_override"] = df.apply(
                lambda r: (m.get(str(r.get("ms_link", "")).strip(), "") or str(r.get("yf_symbol_override", "")).strip()),
                axis=1,
            )
            print(f"✅ overrides (by ms_link) 套用 {len(ov2)} 筆：{path}")
            return df

    if "isin" in cols:
        key = cols["isin"]
        ov2 = ov[[key, override_col]].copy()
        ov2[key] = ov2[key].astype(str).str.strip().str.upper()
        ov2[override_col] = ov2[override_col].astype(str).str.strip()
        ov2 = ov2[ov2[key].ne("") & ov2[override_col].ne("")]
        if not ov2.empty:
            m = dict(zip(ov2[key], ov2[override_col]))
            df["yf_symbol_override"] = df.apply(
                lambda r: (m.get(str(r.get("isin", "")).strip().upper(), "") or str(r.get("yf_symbol_override", "")).strip()),
                axis=1,
            )
            print(f"✅ overrides (by isin) 套用 {len(ov2)} 筆：{path}")
            return df

    print(f"⚠️ overrides 未找到可用 key 欄位(ms_link/isin)，略過: {path}")
    return df


# =============================================================================
# Build yfinance symbols
# =============================================================================
def build_yf_symbols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

    df["yf_symbol_guess"] = df["ticker"].apply(lambda t: f"{t}.PA" if t else "")

    if "yf_symbol_override" not in df.columns:
        df["yf_symbol_override"] = ""
    df["yf_symbol_override"] = df["yf_symbol_override"].astype(str).str.strip()

    df["yf_symbol"] = df["yf_symbol_override"]
    mask = df["yf_symbol"].eq("")
    df.loc[mask, "yf_symbol"] = df.loc[mask, "yf_symbol_guess"]

    return df


# =============================================================================
# Main
# =============================================================================
def run():
    if not GDRIVE_FOLDER_ID:
        print("❌ 錯誤: 找不到 GDRIVE_FOLDER_ID / FR_STOCKLIST (Google Drive 資料夾 ID)")
        sys.exit(1)

    service = get_drive_service()

    print("=" * 70)
    print(f"FR Stock Master | pages={MAX_PAGES} | resolve_max={RESOLVE_MAX} | out={OUTPUT_NAME}")
    print(f"cache={CACHE_PATH}")
    print(f"overrides={OVERRIDE_PATH} (exists={OVERRIDE_PATH.exists()})")
    print("=" * 70)

    all_rows: List[Dict[str, str]] = []
    first_row_fingerprint: Optional[str] = None

    for p in range(1, MAX_PAGES + 1):
        data = scrape_page(p)
        if not data:
            print(f"⚠️  第 {p} 頁無資料，停止")
            break

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

    df = normalize_list_records(all_rows)

    empty_sector = int(df["sector"].astype(str).str.strip().eq("").sum())
    print("📊 list normalize 完成")
    print(f"   rows={len(df)} empty_sector={empty_sector}")

    df = resolve_missing_tickers(df)
    df = apply_overrides(df, OVERRIDE_PATH)
    df = build_yf_symbols(df)

    empty_ticker = int(df["ticker"].astype(str).str.strip().eq("").sum())
    empty_yf = int(df["yf_symbol"].astype(str).str.strip().eq("").sum())
    print("📊 resolve 完成")
    print(f"   empty_ticker={empty_ticker} empty_yf_symbol={empty_yf}")

    preferred = [
        "asof_utc",
        "ticker",
        "yf_symbol",
        "yf_symbol_guess",
        "yf_symbol_override",
        "isin",
        "name",
        "sector",
        "ms_link",
        "_raw",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    print("📤 上傳到 Google Drive ...")
    file_id = upload_csv_update_in_place(service, GDRIVE_FOLDER_ID, OUTPUT_NAME, df)

    print("✅ 任務完成！")
    print(f"   Drive fileId: {file_id}")
    print(f"   Rows: {len(df)}")
    print("   Head:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    run()
