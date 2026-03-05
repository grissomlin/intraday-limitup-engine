# markets/fr/fr_master.py
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

from .fr_config import log, _get_first_env, _parse_bool_env, master_csv_local_path
from .fr_db import init_db


def _get_drive_service_from_token_b64(token_b64: str):
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


def maybe_fetch_master_csv_from_drive(local_path: str) -> bool:
    """
    Download master CSV from Drive if missing.

    IMPORTANT:
    - FR_STOCKLIST is Drive FOLDER ID (secret), not local path.
    """
    if not _parse_bool_env("FR_MASTER_CSV_AUTO_FETCH", True):
        return False

    token_b64, token_key = _get_first_env("GDRIVE_TOKEN_B64", "GDRIVE_TOKEN_JSON_B64", "GDRIVE_TOKEN")

    # Folder ID priority:
    # 1) GDRIVE_FOLDER_ID (generic)
    # 2) FR_STOCKLIST (your secret folder id)
    # 3) GDRIVE_ROOT_FOLDER_ID / GDRIVE_PARENT_ID (fallbacks)
    folder_id, folder_key = _get_first_env("GDRIVE_FOLDER_ID", "FR_STOCKLIST", "GDRIVE_ROOT_FOLDER_ID", "GDRIVE_PARENT_ID")

    if not token_b64 or not folder_id:
        miss = []
        if not token_b64:
            miss.append("token(GDRIVE_TOKEN_B64/GDRIVE_TOKEN_JSON_B64/GDRIVE_TOKEN)")
        if not folder_id:
            miss.append("folder(GDRIVE_FOLDER_ID/FR_STOCKLIST/GDRIVE_ROOT_FOLDER_ID/GDRIVE_PARENT_ID)")
        log(f"⚠️ Drive fetch skipped: missing {', '.join(miss)}")
        return False

    drive_name = (os.getenv("FR_MASTER_CSV_DRIVE_NAME") or "FR_Stock_Master_Data.csv").strip() or "FR_Stock_Master_Data.csv"

    try:
        log(f"☁️ master CSV missing; try fetch from Drive | folder={folder_id}({folder_key}) name={drive_name} | token={token_key}")
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


def load_master_csv() -> pd.DataFrame:
    path = master_csv_local_path()

    if not os.path.exists(path):
        maybe_fetch_master_csv_from_drive(path)

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"FR master CSV not found: {path}. "
            f"Set FR_MASTER_CSV_PATH and/or provide Drive folder id via FR_STOCKLIST secret."
        )

    df = pd.read_csv(path)
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _norm_col(df: pd.DataFrame, name: str, aliases: List[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    if name.lower() in cols:
        return cols[name.lower()]
    for a in aliases:
        if a.lower() in cols:
            return cols[a.lower()]
    return ""


def _coerce_symbol(x: Any) -> str:
    s = ("" if x is None else str(x)).strip().upper()
    if s in ("", "NAN", "NONE", "-", "—", "--"):
        return ""
    return s


def _coerce_text(x: Any, default: str = "") -> str:
    s = ("" if x is None else str(x)).strip()
    return s if s else default


def refresh_stock_info_from_master(db_path: str, refresh_list: bool = True) -> List[Tuple[str, str]]:
    """
    Returns [(yf_symbol, name), ...] and writes stock_info.
    If refresh_list=False and DB already has FR stock_info, use DB.
    """
    init_db(db_path)

    if (not refresh_list) and os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            df0 = pd.read_sql_query("SELECT symbol, name FROM stock_info WHERE market='FR'", conn)
            if not df0.empty:
                items = [(str(r["symbol"]), str(r["name"])) for _, r in df0.iterrows() if str(r["symbol"]).strip()]
                log(f"✅ 使用 DB stock_info 既有 FR 清單: {len(items)} 檔")
                return items
        finally:
            conn.close()

    df = load_master_csv()

    c_yf = _norm_col(df, "yf_symbol", ["yf_ticker", "yf", "ticker_yf"])
    c_sym = _norm_col(df, "symbol", ["local_symbol", "ticker", "code"])
    c_name = _norm_col(df, "company_name", ["name", "issuer name", "issuer_name", "company"])
    c_sector = _norm_col(df, "sector", ["Sector", "icb sector", "gics sector"])
    c_ind = _norm_col(df, "industry", ["Industry", "subindustry", "sub_industry", "icb industry"])

    if not c_yf:
        raise RuntimeError(f"FR master CSV missing yf symbol column. Columns={list(df.columns)}")

    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    items: List[Tuple[str, str]] = []

    conn = sqlite3.connect(db_path)
    try:
        for _, r in df.iterrows():
            yf_symbol = _coerce_symbol(r.get(c_yf))
            if not yf_symbol:
                continue

            local_symbol = _coerce_symbol(r.get(c_sym)) if c_sym else ""
            name = _coerce_text(r.get(c_name), default=yf_symbol) if c_name else yf_symbol
            sector = _coerce_text(r.get(c_sector), default="Unknown") if c_sector else "Unknown"
            industry = _coerce_text(r.get(c_ind), default="Unknown") if c_ind else "Unknown"

            md = "EURONEXT_PARIS|src=master_csv"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, local_symbol, name, sector, industry, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (yf_symbol, local_symbol, name, sector, industry, "FR", md, now_s),
            )
            items.append((yf_symbol, name))
        conn.commit()
    finally:
        conn.close()

    log(f"✅ FR 名單同步完成：共 {len(items)} 檔")
    return items
