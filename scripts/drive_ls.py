# scripts/drive_ls.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import argparse
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/drive"]
TOKEN_FILE = "token.json"

def get_service():
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError("找不到 token.json")
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise RuntimeError("token 無效且不能 refresh")
    return build("drive", "v3", credentials=creds)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder", required=True, help="Google Drive folder id")
    ap.add_argument("--name", default="", help="filter by name contains")
    args = ap.parse_args()

    svc = get_service()

    q = f"'{args.folder}' in parents and trashed=false"
    if args.name.strip():
        # name contains（不分大小寫不保證，但夠用）
        q += f" and name contains '{args.name.strip()}'"

    res = svc.files().list(
        q=q,
        fields="files(id,name,mimeType,size)"
    ).execute()

    files = res.get("files", [])
    if not files:
        print("（空）")
        return

    # 先列資料夾，再列檔案
    folders = [f for f in files if f["mimeType"] == "application/vnd.google-apps.folder"]
    others  = [f for f in files if f["mimeType"] != "application/vnd.google-apps.folder"]

    if folders:
        print("📁 Folders:")
        for f in sorted(folders, key=lambda x: x["name"]):
            print(f"  - {f['name']} ({f['id']})")
    if others:
        print("📄 Files:")
        for f in sorted(others, key=lambda x: x["name"]):
            sz = f.get("size", "")
            print(f"  - {f['name']} ({f['id']}) mime={f['mimeType']} size={sz}")

if __name__ == "__main__":
    main()
