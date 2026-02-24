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
import pandas as pd

# --- 1. 設定變數 ---
# 請確保在 GitHub Secrets 設定了 CN_STOCKLIST 這個資料夾 ID
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_ROOT_FOLDER_ID = os.environ.get("CN_STOCKLIST") 
FILE_NAME = "china_equities_list.csv"

# --- 2. 認證服務 ---
def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("缺少 GDRIVE_TOKEN_B64 Secrets")
    token_json = json.loads(base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8"))
    creds = Credentials.from_authorized_user_info(token_json, scopes=["https://www.googleapis.com/auth/drive"])
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)

# --- 3. 刪除雲端舊檔 ---
def delete_existing_file(service, file_name, folder_id):
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    for item in results.get('files', []):
        print(f"正在清理雲端舊檔案: {item['name']}")
        service.files().delete(fileId=item['id'], supportsAllDrives=True).execute()

# --- 4. 抓取大陸 A 股清單 (AkShare) ---
def fetch_china_stock_list():
    import akshare as ak
    print("📡 正在從 AkShare 獲取大陸 A 股清單...")
    try:
        # 優先使用 code_name 接口
        df = ak.stock_info_a_code_name()
        # 標準化代碼為 6 位數
        df['code'] = df['code'].astype(str).str.zfill(6)
        # 標註交易所後綴 (Yahoo 格式)
        df['symbol'] = df['code'].apply(lambda x: f"{x}.SS" if x.startswith('6') else f"{x}.SZ")
        return df
    except Exception as e:
        print(f"⚠️ 抓取失敗，嘗試備援接口: {e}")
        df_spot = ak.stock_zh_a_spot_em()
        df_spot['symbol'] = df_spot['代码'].astype(str).str.zfill(6).apply(lambda x: f"{x}.SS" if x.startswith('6') else f"{x}.SZ")
        return df_spot

# --- 5. 執行流程 ---
def run():
    try:
        if not GDRIVE_ROOT_FOLDER_ID:
            print("❌ 錯誤: 找不到 CN_STOCKLIST 資料夾 ID")
            sys.exit(1)

        # A. 抓取資料並轉為 CSV Buffer
        df = fetch_china_stock_list()
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        csv_content = csv_buffer.getvalue()

        # B. 認證並清理雲端
        service = get_drive_service()
        delete_existing_file(service, FILE_NAME, GDRIVE_ROOT_FOLDER_ID)

        # C. 上傳新檔
        media = MediaIoBaseUpload(io.BytesIO(csv_content), mimetype='text/csv')
        file_metadata = {'name': FILE_NAME, 'parents': [GDRIVE_ROOT_FOLDER_ID]}
        service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
        
        print(f"✅ 大陸股票清單 ({len(df)} 檔) 已成功同步至 Google Drive")

    except Exception as e:
        print(f"❌ 發生錯誤: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run()
