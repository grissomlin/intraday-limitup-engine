# scripts/fetch_nse_master_from_drive.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import io
import json
import os
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


def _get_env(name: str) -> str:
    v = (os.environ.get(name) or "").strip()
    if not v:
        raise SystemExit(f"❌ missing env: {name}")
    return v


def get_drive_service():
    token_b64 = _get_env("GDRIVE_TOKEN_B64")
    try:
        decoded = base64.b64decode(token_b64).decode("utf-8")
        token_info = json.loads(decoded)
    except Exception as e:
        raise SystemExit(f"❌ invalid GDRIVE_TOKEN_B64 (base64/json): {e}")

    creds = Credentials.from_authorized_user_info(token_info)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def find_latest_file_id(service, folder_id: str, file_name: str) -> Optional[str]:
    q = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    res = (
        service.files()
        .list(
            q=q,
            fields="files(id,name,modifiedTime)",
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


def download_file(service, file_id: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)

    fh = io.FileIO(out_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _status, done = downloader.next_chunk()


def main():
    folder_id = _get_env("GDRIVE_FOLDER_ID")  # 你那個 NSE master 存放資料夾
    file_name = os.environ.get("NSE_OUTPUT_NAME", "NSE_Stock_Master_Data.csv").strip() or "NSE_Stock_Master_Data.csv"
    out_path = Path(os.environ.get("INDIA_MASTER_CSV_PATH", "data/nse/NSE_Stock_Master_Data.csv"))

    service = get_drive_service()
    file_id = find_latest_file_id(service, folder_id, file_name)
    if not file_id:
        raise SystemExit(f"❌ file not found on Drive: folder={folder_id} name={file_name}")

    download_file(service, file_id, out_path)
    print(f"✅ downloaded: {file_name} (fileId={file_id}) -> {out_path}")


if __name__ == "__main__":
    main()
