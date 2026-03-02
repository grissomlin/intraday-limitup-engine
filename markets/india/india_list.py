# markets/india/india_list.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import io
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Tuple

import pandas as pd

from .india_config import _master_csv_path, _yf_suffix, log


def _is_blankish(x: Any) -> bool:
    s = ("" if x is None else str(x)).strip()
    return (not s) or s in ("-", "—", "--", "－", "–", "nan", "None")


def _norm_text(x: Any, default: str) -> str:
    s = ("" if x is None else str(x)).strip()
    if _is_blankish(s):
        return default
    return s


def _to_yf_symbol(local_symbol: str) -> str:
    s = (local_symbol or "").strip().upper()
    if not s:
        return ""
    suf = _yf_suffix()
    if s.endswith(suf.upper()) or s.endswith(suf.lower()):
        return s
    return f"{s}{suf}"


# =============================================================================
# Optional: auto-fetch master CSV from Google Drive if missing
# =============================================================================
def _parse_bool_env(name: str, default: bool) -> bool:
    v = (os.getenv(name, "").strip() or ("1" if default else "0")).lower()
    return v in ("1", "true", "yes", "y", "on")


def _get_first_env(*names: str) -> str:
    """Return first non-empty env value from names (strip)."""
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v
    return ""


def _get_drive_service_from_token_b64(token_b64: str):
    # local import to avoid heavy deps unless needed
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    decoded = base64.b64decode(token_b64).decode("utf-8")
    token_info = json.loads(decoded)

    creds = Credentials.from_authorized_user_info(token_info)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def _find_file_id_in_folder(service, folder_id: str, file_name: str) -> Optional[str]:
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
    files = res.get("files", []) or []
    if not files:
        return None
    files.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return files[0]["id"]


def _download_drive_file(service, file_id: str, out_path: Path) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    out_path.parent.mkdir(parents=True, exist_ok=True)

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _status, done = downloader.next_chunk()


def _maybe_fetch_master_csv_from_drive(path: str) -> bool:
    """
    Try download NSE_Stock_Master_Data.csv from Google Drive if:
    - INDIA_MASTER_CSV_AUTO_FETCH=1 (default: True)
    - token exists (GDRIVE_TOKEN_B64 / GDRIVE_TOKEN_JSON_B64 / GDRIVE_TOKEN)
    - folder id exists (GDRIVE_FOLDER_ID / IN_STOCKLIST / GDRIVE_ROOT_FOLDER_ID / GDRIVE_PARENT_ID)
    Return True if downloaded, else False.
    """
    if not _parse_bool_env("INDIA_MASTER_CSV_AUTO_FETCH", True):
        return False

    # token candidates (you currently have GDRIVE_TOKEN_B64)
    token_b64 = _get_first_env("GDRIVE_TOKEN_B64", "GDRIVE_TOKEN_JSON_B64", "GDRIVE_TOKEN")
    # folder id candidates (you currently often only have GDRIVE_ROOT_FOLDER_ID)
    folder_id = _get_first_env("GDRIVE_FOLDER_ID", "IN_STOCKLIST", "GDRIVE_ROOT_FOLDER_ID", "GDRIVE_PARENT_ID")

    if not token_b64 or not folder_id:
        miss = []
        if not token_b64:
            miss.append("token(GDRIVE_TOKEN_B64/GDRIVE_TOKEN_JSON_B64/GDRIVE_TOKEN)")
        if not folder_id:
            miss.append("folder(GDRIVE_FOLDER_ID/IN_STOCKLIST/GDRIVE_ROOT_FOLDER_ID/GDRIVE_PARENT_ID)")
        log(f"⚠️ Drive fetch skipped: missing {', '.join(miss)}")
        return False

    file_name = (os.getenv("NSE_OUTPUT_NAME") or "NSE_Stock_Master_Data.csv").strip() or "NSE_Stock_Master_Data.csv"

    try:
        log(f"☁️ master CSV missing; try fetch from Drive | folder={folder_id} name={file_name}")
        svc = _get_drive_service_from_token_b64(token_b64)
        file_id = _find_file_id_in_folder(svc, folder_id, file_name)
        if not file_id:
            log(f"⚠️ Drive fetch skipped: file not found | folder={folder_id} name={file_name}")
            return False

        out_path = Path(path)
        _download_drive_file(svc, file_id, out_path)
        log(f"✅ Drive fetched: {file_name} (fileId={file_id}) -> {out_path}")
        return True
    except Exception as e:
        log(f"⚠️ Drive fetch failed: {e}")
        return False


def _load_master_csv(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        _maybe_fetch_master_csv_from_drive(path)

    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"INDIA master CSV not found: {path} (set INDIA_MASTER_CSV_PATH)")

    df = pd.read_csv(path)
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def get_india_stock_list(db_path: str) -> List[Tuple[str, str, str, str, str, str]]:
    """
    Returns list of tuples:
      (yf_symbol, local_symbol, name, industry, sector, market_detail)

    market_detail example:
      "NSE|band=20|remarks=-|src=master_csv"
    """
    path = _master_csv_path()
    log(f"📡 同步印度 NSE 名單 (master_csv) path={path}")

    df = _load_master_csv(path)

    for c in ["SYMBOL", "NAME OF COMPANY", "Band", "Remarks", "sector", "industry"]:
        if c not in df.columns:
            df[c] = None

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    df = df[df["SYMBOL"].notna() & (df["SYMBOL"].str.len() > 0)].copy()

    items: List[Tuple[str, str, str, str, str, str]] = []
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db_path)
    try:
        for _, r in df.iterrows():
            local_symbol = _norm_text(r.get("SYMBOL"), "")
            if not local_symbol:
                continue

            yf_symbol = _to_yf_symbol(local_symbol)
            if not yf_symbol:
                continue

            name = _norm_text(r.get("NAME OF COMPANY"), "Unknown")
            sector = _norm_text(r.get("sector"), "Unclassified")
            industry = _norm_text(r.get("industry"), "Unclassified")

            band = _norm_text(r.get("Band"), "")
            remarks = _norm_text(r.get("Remarks"), "")

            md = f"NSE|band={band}|remarks={remarks}|src=master_csv"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, local_symbol, name, industry, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (yf_symbol, local_symbol, name, industry, sector, "INDIA", md, now_s),
            )

            items.append((yf_symbol, local_symbol, name, industry, sector, md))

        conn.commit()
    finally:
        conn.close()

    log(f"✅ INDIA NSE 名單同步完成：共 {len(items)} 檔")
    return items


def get_india_stock_list_from_db(db_path: str) -> List[Tuple[str, str, str, str, str, str]]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT symbol, local_symbol, name, industry, sector, market_detail FROM stock_info WHERE market='INDIA'"
        ).fetchall()

        out: List[Tuple[str, str, str, str, str, str]] = []
        for sym, local_sym, name, industry, sector, md in rows:
            if not sym:
                continue
            out.append(
                (
                    str(sym),
                    str(local_sym or ""),
                    str(name or "Unknown"),
                    str(industry or "Unclassified"),
                    str(sector or "Unclassified"),
                    str(md or "unknown"),
                )
            )
        return out
    finally:
        conn.close()
