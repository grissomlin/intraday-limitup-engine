# download_subtitle_pack_from_drive.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

# 你現在已經用過的 env keys（跟 upload_payload_to_drive.py 同一套）
SCOPES = ["https://www.googleapis.com/auth/drive"]

ENV_DRIVE_ROOT = "GDRIVE_ROOT_FOLDER_ID"
ENV_TOKEN_JSON = "GDRIVE_TOKEN_JSON"
ENV_CLIENT_SECRET_JSON = "GDRIVE_CLIENT_SECRET_JSON"

DEFAULT_DRIVE_ROOT_FOLDER_ID = "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"
TOKEN_FILE = "token.json"
CLIENT_SECRET_FILE = "credentials.json"


def repo_root() -> Path:
    return Path(__file__).resolve().parent


def _write_temp_json(content: str, filename_hint: str) -> Path:
    d = Path(tempfile.mkdtemp(prefix="gdrive_"))
    p = d / filename_hint
    p.write_text(content, encoding="utf-8")
    return p


def resolve_token_path() -> Path:
    env_json = os.getenv(ENV_TOKEN_JSON, "").strip()
    if env_json:
        return _write_temp_json(env_json, "token.json")
    return repo_root() / TOKEN_FILE


def resolve_client_secret_path() -> Path:
    env_json = os.getenv(ENV_CLIENT_SECRET_JSON, "").strip()
    if env_json:
        return _write_temp_json(env_json, "credentials.json")
    p = repo_root() / CLIENT_SECRET_FILE
    if not p.exists():
        raise FileNotFoundError(
            f"找不到 {CLIENT_SECRET_FILE}，也沒有 {ENV_CLIENT_SECRET_JSON}。"
        )
    return p


def get_drive_service():
    token_path = resolve_token_path()
    client_secret_path = resolve_client_secret_path()

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise RuntimeError(
                "目前沒有可 refresh 的 token。請先在本機用 OAuth 產生 token.json，"
                "或把 token.json 內容放到環境變數 GDRIVE_TOKEN_JSON。"
            )

    return build("drive", "v3", credentials=creds)


def _escape(s: str) -> str:
    return (s or "").replace("'", "\\'")


def find_folder(service, parent_id: str, name: str) -> str | None:
    q = (
        "mimeType='application/vnd.google-apps.folder' and "
        f"name='{_escape(name)}' and "
        f"'{parent_id}' in parents and trashed=false"
    )
    res = service.files().list(
        q=q,
        fields="files(id,name)",
        pageSize=10,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def find_file(service, parent_id: str, filename: str) -> str | None:
    q = (
        f"'{parent_id}' in parents and "
        f"name='{_escape(filename)}' and trashed=false"
    )
    res = service.files().list(
        q=q,
        fields="files(id,name,mimeType,modifiedTime,size)",
        pageSize=5,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def download_file_bytes(service, file_id: str) -> bytes:
    req = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    data = req.execute()
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pack", required=True, help="字幕檔檔名，例如 tang300.txt")
    ap.add_argument("--subdir", default="subtitles/public_domain", help="在 Drive root 底下的子路徑")
    ap.add_argument("--out", default="", help="輸出本機檔案路徑（可選）")
    args = ap.parse_args()

    drive_root = os.getenv(ENV_DRIVE_ROOT, DEFAULT_DRIVE_ROOT_FOLDER_ID).strip()
    service = get_drive_service()

    # 走資料夾路徑：例如 subtitles/public_domain
    cur = drive_root
    for part in [p for p in args.subdir.split("/") if p.strip()]:
        nxt = find_folder(service, cur, part)
        if not nxt:
            raise RuntimeError(f"Drive 找不到資料夾：{args.subdir}（缺少：{part}）")
        cur = nxt

    file_id = find_file(service, cur, args.pack)
    if not file_id:
        raise RuntimeError(f"Drive 找不到字幕檔：{args.subdir}/{args.pack}")

    content = download_file_bytes(service, file_id)

    out_path = Path(args.out) if args.out else (repo_root() / "media" / "subtitles" / "cache" / args.pack)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(content)

    print(f"✅ 已從 Drive 下載：{args.subdir}/{args.pack}")
    print(f"✅ 已寫入本機：{out_path}")


if __name__ == "__main__":
    main()
