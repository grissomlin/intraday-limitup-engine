import base64
import json
import io
import os
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
import sys

# 變數設定 (從 GitHub Secrets 讀取)
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_ROOT_FOLDER_ID = os.environ.get("IN_STOCKLIST") 

FILES_TO_PROCESS = {
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv": "EQUITY_L.csv",
    "https://nsearchives.nseindia.com/content/equities/sec_list.csv": "sec_list.csv"
}

def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("缺少 GDRIVE_TOKEN_B64 Secrets")
    token_json = json.loads(base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8"))
    creds = Credentials.from_authorized_user_info(token_json, scopes=["https://www.googleapis.com/auth/drive"])
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)

def delete_existing_file(service, file_name, folder_id):
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    for item in results.get('files', []):
        print(f"正在清理舊檔案: {item['name']}")
        service.files().delete(fileId=item['id'], supportsAllDrives=True).execute()

def run():
    try:
        if not GDRIVE_ROOT_FOLDER_ID:
            print("❌ 錯誤: 找不到 IN_STOCKLIST 資料夾 ID")
            sys.exit(1)

        service = get_drive_service()
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}

        for url, file_name in FILES_TO_PROCESS.items():
            print(f"\n處理中: {file_name}")
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            
            delete_existing_file(service, file_name, GDRIVE_ROOT_FOLDER_ID)
            
            media = MediaIoBaseUpload(io.BytesIO(resp.content), mimetype='text/csv')
            file_metadata = {'name': file_name, 'parents': [GDRIVE_ROOT_FOLDER_ID]}
            service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
            print(f"✅ {file_name} 上傳成功")
    except Exception as e:
        print(f"❌ 發生錯誤: {str(e)}")
        sys.exit(1)

run()
