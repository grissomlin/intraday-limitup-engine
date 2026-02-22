# scripts/sync_subtitles_from_drive.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Optional, List, Dict, Any

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request

# ✅ 用 full drive scope，與你現有 token.json 相容（避免 invalid_scope）
SCOPES = ["https://www.googleapis.com/auth/drive"]

TOKEN_FILE = "token.json"

# 你現在的根資料夾（漲停板影音）
DRIVE_ROOT_FOLDER_ID = "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"

# 本機輸出目錄
LOCAL_SUBTITLE_DIR = Path("media/subtitles/public_domain")

# 只下載這些副檔名
ALLOW_EXT = {".txt"}

# 避免下載到 requirements.txt 這種非字幕檔
DENY_NAMES = {"requirements.txt"}


def _get_drive_service():
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError(f"找不到 {TOKEN_FILE}，請先 OAuth 產生 token.json")

    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise RuntimeError("token 無效且不能 refresh，請刪 token.json 重新授權")

    return build("drive", "v3", credentials=creds)


def _find_child_folder(service, parent_id: str, child_name: str) -> Optional[str]:
    q = (
        f"'{parent_id}' in parents and "
        f"name='{child_name}' and "
        "mimeType='application/vnd.google-apps.folder' and "
        "trashed=false"
    )
    res = service.files().list(q=q, fields="files(id,name)").execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def _list_child_folders(service, parent_id: str) -> List[Dict[str, Any]]:
    q = (
        f"'{parent_id}' in parents and "
        "mimeType='application/vnd.google-apps.folder' and "
        "trashed=false"
    )
    res = service.files().list(q=q, fields="files(id,name)").execute()
    return res.get("files", [])


def _list_files(service, folder_id: str) -> List[Dict[str, Any]]:
    q = f"'{folder_id}' in parents and trashed=false"
    res = service.files().list(q=q, fields="files(id,name,mimeType,size)").execute()
    return res.get("files", [])


def _download_file(service, file_id: str, out_path: Path) -> None:
    req = service.files().get_media(fileId=file_id)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _resolve_public_domain_folder(service) -> str:
    """
    固定尋找：
      root/subtitles/public_domain

    如果 public_domain 不存在，會列出 subtitles 底下的子資料夾，讓你直接看到要用哪個 id。
    """
    subtitles_id = _find_child_folder(service, DRIVE_ROOT_FOLDER_ID, "subtitles")
    if not subtitles_id:
        children = _list_child_folders(service, DRIVE_ROOT_FOLDER_ID)
        names = [f"{x['name']} ({x['id']})" for x in children]
        raise RuntimeError(
            "找不到 root/subtitles。\n"
            "root 底下資料夾：\n  - " + "\n  - ".join(names)
        )

    public_id = _find_child_folder(service, subtitles_id, "public_domain")
    if not public_id:
        children = _list_child_folders(service, subtitles_id)
        names = [f"{x['name']} ({x['id']})" for x in children]
        raise RuntimeError(
            "找不到 subtitles/public_domain。\n"
            "subtitles 底下資料夾：\n  - " + "\n  - ".join(names)
        )

    return public_id


def main():
    service = _get_drive_service()

    public_id = _resolve_public_domain_folder(service)
    print(f"✅ public_domain folder id = {public_id}")

    files = _list_files(service, public_id)

    # 只挑 .txt，且排除 requirements.txt
    picked = []
    for f in files:
        name = f["name"]
        if name in DENY_NAMES:
            continue
        if Path(name).suffix.lower() not in ALLOW_EXT:
            continue
        picked.append(f)

    if not picked:
        # 把 folder 內容列給你看，避免你不知道裡面到底放了什麼
        print("⚠️ public_domain 內沒有可用的字幕 .txt")
        print("📄 該資料夾內容：")
        for f in sorted(files, key=lambda x: x["name"]):
            print(f"  - {f['name']} ({f['id']}) mime={f['mimeType']} size={f.get('size','')}")
        raise RuntimeError("找不到字幕檔（請把 *.txt 字幕放進 subtitles/public_domain）")

    LOCAL_SUBTITLE_DIR.mkdir(parents=True, exist_ok=True)

    for f in sorted(picked, key=lambda x: x["name"]):
        name = f["name"]
        file_id = f["id"]
        out_path = LOCAL_SUBTITLE_DIR / name

        print(f"⬇️ 下載 {name} ...")
        _download_file(service, file_id, out_path)
        print(f"✅ 已下載 → {out_path}")

    print("🎉 字幕同步完成！")
    print(f"📁 本機位置：{LOCAL_SUBTITLE_DIR.resolve()}")


if __name__ == "__main__":
    main()
