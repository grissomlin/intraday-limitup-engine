import base64
import json
import io
import os
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request

# --- 1. 從環境變數讀取設定 ---
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_ROOT_FOLDER_ID = os.environ.get("IN_STOCKLIST") 

# 只針對這兩個連結進行處理
FILES_TO_PROCESS = {
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv": "EQUITY_L.csv",
    "https://nsearchives.nseindia.com/content/equities/sec_list.csv": "sec_list.csv"
}

# --- 2. 認證與建立服務 ---
def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("找不到 GDRIVE_TOKEN_B64")

    token_json = json.loads(base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8"))
    creds = Credentials.from_authorized_user_info(token_json, scopes=["https://www.googleapis.com/auth/drive"])
    
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        
    return build("drive", "v3", credentials=creds)

# --- 3. 刪除舊檔邏輯 ---
def delete_existing_file(service, file_name, folder_id):
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query, 
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    items = results.get('files', [])
    for item in items:
        print(f"清理舊檔: {item['name']}")
        service.files().delete(fileId=item['id'], supportsAllDrives=True).execute()

# --- 4. 主執行流程 ---
def run():
    try:
        if not GDRIVE_ROOT_FOLDER_ID:
            raise ValueError("找不到 IN_STOCKLIST ID")

        service = get_drive_service()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.nseindia.com/"
        }

        for url, file_name in FILES_TO_PROCESS.items():
            print(f"\n處理: {file_name}")
            
            # 下載
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            
            # 刪除舊的
            delete_existing_file(service, file_name, GDRIVE_ROOT_FOLDER_ID)
            
            # 上傳
            media = MediaIoBaseUpload(io.BytesIO(resp.content), mimetype='text/csv')
            file_metadata = {'name': file_name, 'parents': [GDRIVE_ROOT_FOLDER_ID]}
            
            service.files().create(
                body=file_metadata,
                media_body=media,
                supportsAllDrives=True
            ).execute()
            
            print(f"✅ {file_name} 已同步至 Google Drive")

    except Exception as e:
        print(f"❌ 錯誤: {str(e)}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    run()
