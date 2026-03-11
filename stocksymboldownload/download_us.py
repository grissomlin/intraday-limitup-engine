# -*- coding: utf-8 -*-
"""
US SEC JSON updater:
- Download latest company_tickers.json from SEC
- Build company_tickers.meta.json
- Update local sec_industry_cache.json (missing/stale only) via SEC submissions API
- Upload all 3 JSON files to Google Drive (update in place)

Required env:
- GDRIVE_TOKEN_B64
- US_STOCKLIST or GDRIVE_FOLDER_ID

Optional env:
- US_SEC_LOCAL_DIR            default: data/us_sec_json
- US_SEC_COMPANY_TICKERS_NAME default: company_tickers.json
- US_SEC_META_NAME            default: company_tickers.meta.json
- US_SEC_CACHE_NAME           default: sec_industry_cache.json
- US_SEC_CACHE_TTL_DAYS       default: 30
- US_SEC_MAX_FETCH            default: 12000
- US_SEC_HTTP_TIMEOUT         default: 20
- US_SEC_SLEEP_SEC            default: 0.12
- US_SEC_USER_AGENT           default: GrissomQuantLab/1.0 (contact: you@example.com)
"""

from __future__ import annotations

import base64
import io
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


# =============================================================================
# Config
# =============================================================================
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID") or os.environ.get("US_STOCKLIST")

LOCAL_DIR = Path(os.environ.get("US_SEC_LOCAL_DIR", "data/us_sec_json"))
COMPANY_TICKERS_NAME = os.environ.get("US_SEC_COMPANY_TICKERS_NAME", "company_tickers.json")
META_NAME = os.environ.get("US_SEC_META_NAME", "company_tickers.meta.json")
CACHE_NAME = os.environ.get("US_SEC_CACHE_NAME", "sec_industry_cache.json")

TTL_DAYS = int(os.environ.get("US_SEC_CACHE_TTL_DAYS", "30"))
MAX_FETCH = int(os.environ.get("US_SEC_MAX_FETCH", "12000"))
HTTP_TIMEOUT = int(os.environ.get("US_SEC_HTTP_TIMEOUT", "20"))
SLEEP_SEC = float(os.environ.get("US_SEC_SLEEP_SEC", "0.12"))
USER_AGENT = os.environ.get(
    "US_SEC_USER_AGENT",
    "GrissomQuantLab/1.0 (contact: you@example.com)",
)

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik10}.json"


# =============================================================================
# Utils
# =============================================================================
def log(msg: str) -> None:
    print(f"{datetime.now():%H:%M:%S}: {msg}", flush=True)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def norm_symbol(sym: str) -> str:
    return (sym or "").strip().upper().replace("/", "-")


def cik10(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = "".join(ch for ch in str(v).strip() if ch.isdigit())
    if not s:
        return None
    return s.zfill(10)


# =============================================================================
# Google Drive
# =============================================================================
def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("❌ 找不到 GDRIVE_TOKEN_B64")

    try:
        decoded_data = base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8")
        token_info = json.loads(decoded_data)
    except Exception as e:
        raise ValueError(f"❌ Base64 解碼或 JSON 解析失敗: {e}")

    try:
        creds = Credentials.from_authorized_user_info(token_info)
        if creds.expired and creds.refresh_token:
            log("🔄 Token 已過期，嘗試自動刷新...")
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


def upload_json_update_in_place(service, folder_id: str, file_name: str, payload: Any) -> str:
    buf = io.BytesIO()
    raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    buf.write(raw)
    buf.seek(0)

    media = MediaIoBaseUpload(buf, mimetype="application/json", resumable=True)
    existing_id = find_file_id(service, file_name, folder_id)

    if existing_id:
        log(f"♻️ Drive 同名檔案已存在，update：{file_name} (fileId={existing_id})")
        service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        return existing_id

    log(f"📤 Drive 無同名檔案，create：{file_name}")
    meta = {"name": file_name, "parents": [folder_id]}
    created = (
        service.files()
        .create(body=meta, media_body=media, supportsAllDrives=True)
        .execute()
    )
    return created["id"]


# =============================================================================
# SEC fetch
# =============================================================================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }
    )
    return s


def fetch_company_tickers(session: requests.Session) -> Dict[str, Any]:
    log("📥 下載 SEC company_tickers.json ...")
    r = session.get(SEC_COMPANY_TICKERS_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_submissions_sic(session: requests.Session, cik_10: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    url = SEC_SUBMISSIONS_URL_TMPL.format(cik10=cik_10)
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None, None, f"http_{r.status_code}"
        j = r.json()
        sic = j.get("sic")
        sic_desc = j.get("sicDescription")
        sic_s = str(sic) if sic not in (None, "") else None
        sic_desc_s = str(sic_desc) if sic_desc not in (None, "") else None
        return sic_s, sic_desc_s, None
    except Exception as e:
        return None, None, f"exception:{e}"


# =============================================================================
# Cache
# =============================================================================
@dataclass
class IndustryInfo:
    cik: str
    sic: Optional[str]
    sicDescription: Optional[str]
    fetched_at: str


def load_cache(path: Path) -> Dict[str, IndustryInfo]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    out: Dict[str, IndustryInfo] = {}
    if not isinstance(raw, dict):
        return out

    for sym, v in raw.items():
        if not isinstance(v, dict):
            continue
        c = cik10(v.get("cik"))
        if not c:
            continue
        out[norm_symbol(sym)] = IndustryInfo(
            cik=c,
            sic=str(v.get("sic")) if v.get("sic") not in (None, "") else None,
            sicDescription=str(v.get("sicDescription")) if v.get("sicDescription") not in (None, "") else None,
            fetched_at=str(v.get("fetched_at") or ""),
        )
    return out


def save_cache(path: Path, cache: Dict[str, IndustryInfo]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = {sym: asdict(info) for sym, info in sorted(cache.items())}
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


def is_fresh(info: IndustryInfo, ttl_days: int) -> bool:
    if ttl_days <= 0:
        return True
    t = parse_iso(info.fetched_at)
    if not t:
        return False
    return datetime.now(timezone.utc) - t <= timedelta(days=ttl_days)


def normalize_company_tickers(raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for _, row in raw.items():
        if not isinstance(row, dict):
            continue
        sym = norm_symbol(str(row.get("ticker", "")))
        if not sym:
            continue
        out[sym] = row
    return out


# =============================================================================
# Main update logic
# =============================================================================
def build_meta(raw_tickers: Dict[str, Any]) -> Dict[str, Any]:
    count = len(raw_tickers) if isinstance(raw_tickers, dict) else 0
    return {
        "source": "SEC company_tickers.json",
        "source_url": SEC_COMPANY_TICKERS_URL,
        "downloaded_at_utc": now_utc_iso(),
        "ticker_count": count,
        "user_agent": USER_AGENT,
    }


def update_sec_industry_cache(
    company_map: Dict[str, Dict[str, Any]],
    cache_old: Dict[str, IndustryInfo],
    session: requests.Session,
) -> Tuple[Dict[str, IndustryInfo], Dict[str, Any]]:
    cache = dict(cache_old)
    stats = {
        "targets": len(company_map),
        "cache_hit_fresh": 0,
        "need_fetch": 0,
        "fetched_ok": 0,
        "fetched_failed": 0,
        "skipped_max_fetch": 0,
    }

    fetched = 0

    for i, (sym, row) in enumerate(company_map.items(), 1):
        old = cache.get(sym)
        if old and is_fresh(old, TTL_DAYS) and (old.sicDescription or old.sic):
            stats["cache_hit_fresh"] += 1
            continue

        stats["need_fetch"] += 1

        c10 = cik10(row.get("cik_str"))
        if not c10:
            continue

        if fetched >= MAX_FETCH:
            stats["skipped_max_fetch"] += 1
            continue

        sic, sic_desc, err = fetch_submissions_sic(session, c10)
        fetched += 1

        if err:
            stats["fetched_failed"] += 1
        else:
            cache[sym] = IndustryInfo(
                cik=c10,
                sic=sic,
                sicDescription=sic_desc,
                fetched_at=now_utc_iso(),
            )
            stats["fetched_ok"] += 1

        if i == 1 or i % 500 == 0:
            log(
                f"進度 {i}/{len(company_map)} | "
                f"fresh={stats['cache_hit_fresh']} need_fetch={stats['need_fetch']} "
                f"ok={stats['fetched_ok']} fail={stats['fetched_failed']}"
            )

        time.sleep(SLEEP_SEC)

    return cache, stats


def run():
    if not GDRIVE_FOLDER_ID:
        print("❌ 找不到 GDRIVE_FOLDER_ID / US_STOCKLIST")
        sys.exit(1)

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    company_tickers_path = LOCAL_DIR / COMPANY_TICKERS_NAME
    meta_path = LOCAL_DIR / META_NAME
    cache_path = LOCAL_DIR / CACHE_NAME

    service = get_drive_service()
    session = make_session()

    # 1) SEC company tickers
    raw_tickers = fetch_company_tickers(session)
    company_tickers_path.write_text(
        json.dumps(raw_tickers, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log(f"💾 已保存：{company_tickers_path}")

    # 2) meta
    meta = build_meta(raw_tickers)
    meta_path.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log(f"💾 已保存：{meta_path}")

    # 3) cache update
    company_map = normalize_company_tickers(raw_tickers)
    old_cache = load_cache(cache_path)
    log(f"🧠 舊 cache 筆數：{len(old_cache)}")

    new_cache, stats = update_sec_industry_cache(company_map, old_cache, session)
    save_cache(cache_path, new_cache)
    log(f"💾 已保存：{cache_path} | 新 cache 筆數：{len(new_cache)}")

    # 4) upload all 3 files to drive
    log("📤 上傳 company_tickers.json 到 Google Drive ...")
    company_file_id = upload_json_update_in_place(service, GDRIVE_FOLDER_ID, COMPANY_TICKERS_NAME, raw_tickers)

    log("📤 上傳 company_tickers.meta.json 到 Google Drive ...")
    meta_file_id = upload_json_update_in_place(service, GDRIVE_FOLDER_ID, META_NAME, meta)

    log("📤 上傳 sec_industry_cache.json 到 Google Drive ...")
    cache_payload = json.loads(cache_path.read_text(encoding="utf-8"))
    cache_file_id = upload_json_update_in_place(service, GDRIVE_FOLDER_ID, CACHE_NAME, cache_payload)

    print("=" * 80)
    print("✅ 任務完成")
    print(f"local_dir              : {LOCAL_DIR}")
    print(f"company_tickers rows   : {len(company_map)}")
    print(f"old_cache rows         : {len(old_cache)}")
    print(f"new_cache rows         : {len(new_cache)}")
    print(f"cache_hit_fresh        : {stats['cache_hit_fresh']}")
    print(f"need_fetch             : {stats['need_fetch']}")
    print(f"fetched_ok             : {stats['fetched_ok']}")
    print(f"fetched_failed         : {stats['fetched_failed']}")
    print(f"skipped_max_fetch      : {stats['skipped_max_fetch']}")
    print(f"Drive company fileId   : {company_file_id}")
    print(f"Drive meta fileId      : {meta_file_id}")
    print(f"Drive cache fileId     : {cache_file_id}")
    print("=" * 80)


if __name__ == "__main__":
    run()
