# markets/fr/fr_list.py
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

from .fr_config import _stocklist_path, _yf_suffix, log


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
# Optional: auto-fetch FR stocklist CSV from Google Drive if missing
# =============================================================================
def _parse_bool_env(name: str, default: bool) -> bool:
    v = (os.getenv(name, "").strip() or ("1" if default else "0")).lower()
    return v in ("1", "true", "yes", "y", "on")


def _get_first_env(*names: str) -> Tuple[str, str]:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v, n
    return "", ""


def _get_drive_service_from_token_b64(token_b64: str):
    # local import to avoid heavy deps unless needed
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    decoded = base64.b64decode(token_b64).decode("utf-8")
    token_info = json.loads(decoded)

    # NOTE: token_info must be "authorized_user_info" style
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


def _maybe_fetch_fr_stocklist_from_drive(local_path: str) -> bool:
    """
    Download FR stocklist CSV from Drive if missing.

    Env it will try:
      token:
        GDRIVE_TOKEN_B64 / GDRIVE_TOKEN_JSON_B64 / GDRIVE_TOKEN
      folder:
        FR_STOCKLIST_FOLDER_ID / GDRIVE_FOLDER_ID / FR_STOCKLIST / GDRIVE_ROOT_FOLDER_ID / GDRIVE_PARENT_ID
      drive filename:
        FR_MASTER_CSV_DRIVE_NAME / (default: FR_Stock_Master_Data.csv)
    """
    if not _parse_bool_env("FR_STOCKLIST_AUTO_FETCH", True):
        return False

    token_b64, token_key = _get_first_env("GDRIVE_TOKEN_B64", "GDRIVE_TOKEN_JSON_B64", "GDRIVE_TOKEN")
    folder_id, folder_key = _get_first_env(
        "FR_STOCKLIST_FOLDER_ID",
        "GDRIVE_FOLDER_ID",
        "FR_STOCKLIST",  # 你若真的把 folder_id 塞在 FR_STOCKLIST（不建議，但給你退路）
        "GDRIVE_ROOT_FOLDER_ID",
        "GDRIVE_PARENT_ID",
    )

    if not token_b64 or not folder_id:
        miss = []
        if not token_b64:
            miss.append("token(GDRIVE_TOKEN_B64/GDRIVE_TOKEN_JSON_B64/GDRIVE_TOKEN)")
        if not folder_id:
            miss.append("folder(FR_STOCKLIST_FOLDER_ID/GDRIVE_FOLDER_ID/...)")
        log(f"⚠️ Drive fetch skipped: missing {', '.join(miss)}")
        return False

    drive_name = (os.getenv("FR_MASTER_CSV_DRIVE_NAME") or "FR_Stock_Master_Data.csv").strip()
    if not drive_name:
        drive_name = "FR_Stock_Master_Data.csv"

    try:
        log(f"☁️ FR stocklist missing; try fetch from Drive | folder={folder_id}({folder_key}) name={drive_name} | token={token_key}")
        svc = _get_drive_service_from_token_b64(token_b64)

        file_id = _find_file_id_in_folder(svc, folder_id, drive_name)
        if not file_id:
            log(f"⚠️ Drive fetch skipped: file not found | folder={folder_id}({folder_key}) name={drive_name}")
            return False

        out_path = Path(local_path)
        _download_drive_file(svc, file_id, out_path)
        log(f"✅ Drive fetched: {drive_name} (fileId={file_id}) -> {out_path}")
        return True
    except Exception as e:
        log(f"⚠️ Drive fetch failed: {e}")
        return False


def _load_stocklist_csv(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        _maybe_fetch_fr_stocklist_from_drive(path)

    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"FR stocklist CSV not found: {path} (set FR_STOCKLIST)")

    df = pd.read_csv(path)
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


# =============================================================================
# Public API
# =============================================================================
def get_fr_stock_list(db_path: str, refresh_list: bool = True) -> List[Tuple[str, str, str]]:
    """
    Returns list of tuples:
      (yf_symbol, name, sector)

    CSV expected columns (you already have):
      - yf_symbol   (preferred) or yfinance_ticker
      - company_name (or name)
      - sector (optional)
      - symbol (optional; local code)
    """
    path = _stocklist_path()
    log(f"📡 同步法國 FR 名單 (stocklist) path={path}")

    df = _load_stocklist_csv(path)

    # column normalize
    for c in ["yf_symbol", "yfinance_ticker", "company_name", "name", "sector", "symbol", "ticker"]:
        if c not in df.columns:
            df[c] = None

    # choose yf
    df["yf_symbol"] = df["yf_symbol"].where(~df["yf_symbol"].isna(), df["yfinance_ticker"])
    df["yf_symbol"] = df["yf_symbol"].astype(str).str.strip()
    df = df[df["yf_symbol"].notna() & (df["yf_symbol"].str.len() > 0)].copy()

    # if some rows have only local symbol, convert to .PA
    # (e.g. symbol='MC' but yf_symbol blank)
    need = df["yf_symbol"].isin(["nan", "None", ""])
    if need.any():
        df.loc[need, "yf_symbol"] = df.loc[need, "symbol"].apply(lambda s: _to_yf_symbol(str(s)))

    df["yf_symbol"] = df["yf_symbol"].astype(str).str.strip()
    df = df[df["yf_symbol"].str.len() > 0].copy()

    items: List[Tuple[str, str, str]] = []
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db_path)
    try:
        for _, r in df.iterrows():
            yf_symbol = _norm_text(r.get("yf_symbol"), "").upper()
            if not yf_symbol:
                continue

            name = _norm_text(r.get("company_name"), "")
            if not name:
                name = _norm_text(r.get("name"), "Unknown")

            sector = _norm_text(r.get("sector"), "Unknown")

            # market_detail 你也可以塞一些來源資訊
            md = "EURONEXT|src=stocklist_csv"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, name, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (yf_symbol, name, sector, "FR", md, now_s),
            )

            items.append((yf_symbol, name, sector))

        conn.commit()
    finally:
        conn.close()

    log(f"✅ FR 名單同步完成：共 {len(items)} 檔")
    return items
