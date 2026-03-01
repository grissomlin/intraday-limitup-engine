import base64
import json
import io
import os
import requests
import pandas as pd
import yfinance as yf
import time
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
import sys

# 變數設定
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_ROOT_FOLDER_ID = os.environ.get("IN_STOCKLIST") 

# NSE 檔案連結
URL_EQUITY_L = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
URL_SEC_LIST = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"

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
        service = get_drive_service()
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}

        # 1. 下載基本資訊 (EQUITY_L)
        print("下載 EQUITY_L.csv...")
        res1 = requests.get(URL_EQUITY_L, headers=headers)
        df_base = pd.read_csv(io.BytesIO(res1.content))
        df_base.columns = df_base.columns.str.strip()

        # 2. 下載漲跌幅限制 (sec_list)
        print("下載 sec_list.csv (Price Band)...")
        res2 = requests.get(URL_SEC_LIST, headers=headers)
        df_band = pd.read_csv(io.BytesIO(res2.content))
        df_band.columns = df_band.columns.str.strip()

        # 3. 合併基本資料與漲跌幅
        # 使用 Symbol 作為連結鍵
        df_merged = pd.merge(df_base[['SYMBOL', 'NAME OF COMPANY', 'ISIN NUMBER']], 
                             df_band[['Symbol', 'Band', 'Remarks']], 
                             left_on='SYMBOL', right_on='Symbol', how='left')
        df_merged.drop(columns=['Symbol'], inplace=True)

        # 4. 抓取 Yahoo 行業分類 (為了節省 Actions 時間，這裡演示抓取邏輯)
        print(f"開始抓取行業資料，總數: {len(df_merged)}")
        industry_data = []
        
        # 建立 ticker 欄位
        df_merged["ticker"] = df_merged["SYMBOL"].astype(str) + ".NS"

        # 這裡建議在 Actions 執行時可以考慮分批或限制數量，若要全抓需約 20 分鐘
        for i, row in df_merged.iterrows():
            ticker = row["ticker"]
            if i % 100 == 0: print(f"已處理 {i} 檔...")
            
            try:
                # 僅獲取必要資訊
                info = yf.Ticker(ticker).info
                industry_data.append({
                    "SYMBOL": row["SYMBOL"],
                    "sector": info.get("sector", "N/A"),
                    "industry": info.get("industry", "N/A")
                })
            except:
                industry_data.append({"SYMBOL": row["SYMBOL"], "sector": "Error", "industry": "Error"})
            time.sleep(0.1) # 稍微節流

        df_industry = pd.DataFrame(industry_data)
        df_final = pd.merge(df_merged, df_industry, on="SYMBOL", how="left")

        # 5. 儲存並上傳至 Google Drive
        final_file_name = "NSE_Master_Stock_List.csv"
        print(f"準備上傳: {final_file_name}")
        
        delete_existing_file(service, final_file_name, GDRIVE_ROOT_FOLDER_ID)
        
        csv_buffer = io.BytesIO()
        df_final.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        csv_buffer.seek(0)
        
        media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv')
        file_metadata = {'name': final_file_name, 'parents': [GDRIVE_ROOT_FOLDER_ID]}
        service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
        
        print("✅ 任務成功完成！")

    except Exception as e:
        print(f"❌ 錯誤: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run()
