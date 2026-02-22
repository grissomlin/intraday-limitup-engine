from __future__ import print_function

import os
import mimetypes

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request

# ===== 設定 =====
FOLDER_ID = "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"  # 漲停板影音
SCOPES = ["https://www.googleapis.com/auth/drive"]

CLIENT_SECRET_FILE = "client_secret_120088082173-o54bji11oh6hd8snv2ldhm06bp10r28i.apps.googleusercontent.com.json"
TOKEN_FILE = "token.json"


def get_drive_service():
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRET_FILE, SCOPES
            )
            creds = flow.run_local_server(
                port=0,
                access_type="offline",
                prompt="consent",
            )

        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def find_file_in_folder(service, folder_id: str, filename: str):
    # ✅ 先處理單引號，避免 Drive query 爆掉
    safe_name = filename.replace("'", "\\'")

    q = (
        f"'{folder_id}' in parents and "
        f"name = '{safe_name}' and "
        f"trashed = false"
    )

    resp = service.files().list(
        q=q,
        fields="files(id, name, mimeType, modifiedTime)",
        pageSize=5,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    files = resp.get("files", [])
    return files[0] if files else None


def upload_or_replace(local_path: str):
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"找不到檔案：{local_path}")

    service = get_drive_service()
    filename = os.path.basename(local_path)

    mime_type, _ = mimetypes.guess_type(local_path)
    if not mime_type:
        mime_type = "application/octet-stream"

    existing = find_file_in_folder(service, FOLDER_ID, filename)
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)

    if existing:
        file_id = existing["id"]
        updated = service.files().update(
            fileId=file_id,
            media_body=media,
            fields="id, name, modifiedTime",
            supportsAllDrives=True,
        ).execute()
        print(f"✅ 已覆蓋：{updated['name']}  id={updated['id']}")
        return updated["id"]
    else:
        file_metadata = {
            "name": filename,
            "parents": [FOLDER_ID],
        }
        created = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, name",
            supportsAllDrives=True,
        ).execute()
        print(f"✅ 已上傳：{created['name']}  id={created['id']}")
        return created["id"]


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("用法：python upload_to_drive_folder.py <本機檔案路徑>")
        raise SystemExit(1)

    upload_or_replace(sys.argv[1])
