import base64
import json
import io
import os
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request

# --- 1. 從 GitHub Actions 環境變數讀取設定 ---
# 請確保在 GitHub Repository 的 Settings > Secrets 中已設定這兩個變數
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_ROOT_FOLDER_ID = os.environ.get("IN_STOCKLIST") 

# 定義要下載的兩個 NSE 檔案
FILES_TO_PROCESS = {
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv": "EQUITY_L.csv",
    "https://nsearchives.nseindia.com/content/equities/sec_list.csv": "sec_list.csv"
}

# --- 2. 認證與建立服務 ---
def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("找不到 GDRIVE_TOKEN_B64 環境變數，請檢查 GitHub Secrets 設定")

    # 解碼 Base64 並讀取 Token JSON
    token_json = json.loads(base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8"))
    
    # 建立認證物件
    creds = Credentials.from_authorized_user_info(token_json, scopes=["https://www.googleapis.com/auth/drive"])
    
    # 如果 Token 過期且有 refresh_token，則自動刷新
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        
    return build("drive", "v3", credentials=creds)

# --- 3. 刪除舊檔邏輯 ---
def delete_existing_file(service, file_name, folder_id):
    """在指定的資料夾中搜尋同名檔案並刪除，避免檔案堆積"""
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query, 
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    items = results.get('files', [])
    for item in items:
        print(f"清理雲端舊檔: {item['name']} (ID: {item['id']})")
        service.files().delete(fileId=item['id'], supportsAllDrives=True).execute()

# --- 4. 主執行流程 ---
def run():
    try:
        if not GDRIVE_ROOT_FOLDER_ID:
            raise ValueError("找不到 IN_STOCKLIST 資料夾 ID")

        service = get_drive_service()
        
        # NSE 網站需要模擬瀏覽器的 Header，否則會被拒絕訪問
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://www.nseindia.com/"
        }

        print(f"--- 開始處理印度 NSE 資料至資料夾: {GDRIVE_ROOT_FOLDER_ID} ---")

        for url, file_name in FILES_TO_PROCESS.items():
            print(f"\n正在下載: {file_name}...")
            
            # 1. 下載最新檔案
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            print(f"   下載成功 ({len(resp.content)} bytes)")
            
            # 2. 刪除雲端硬碟上的同名舊檔
            delete_existing_file(service, file_name, GDRIVE_ROOT_FOLDER_ID)
            
            # 3. 上傳新檔案
            media = MediaIoBaseUpload(io.BytesIO(resp.content), mimetype='text/csv')
            file_metadata = {
                'name': file_name, 
                'parents': [GDRIVE_ROOT_FOLDER_ID]
            }
            
            new_file = service.files().create(
                body=file_metadata,
                media_body=media,
                supportsAllDrives=True
            ).execute()
            
            print(f"✅ 上傳成功！新 File ID: {new_file.get('id')}")

        print("\n--- 所有工作執行完畢 ---")

    except Exception as e:
        print(f"❌ 發生錯誤: {str(e)}")
        # 在 GitHub Actions 中，非 0 的退出碼會標示任務失敗
        import sys
        sys.exit(1)

if __name__ == "__main__":
    run()
