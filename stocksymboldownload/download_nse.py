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

# 變數設定 (從 GitHub Secrets 或環境變數讀取)
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_ROOT_FOLDER_ID = os.environ.get("IN_STOCKLIST") 

# NSE 資料來源
EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("缺少 GDRIVE_TOKEN_B64 Secrets")
    token_json = json.loads(base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8"))
    creds = Credentials.from_authorized_user_info(token_json, scopes=["https://www.googleapis.com/auth/drive"])
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)

def delete_existing_file(service, file_name, folder_id):
    """刪除指定資料夾中同名的舊檔案"""
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    for item in results.get('files', []):
        print(f"正在清理舊檔案: {item['name']} (ID: {item['id']})")
        service.files().delete(fileId=item['id'], supportsAllDrives=True).execute()

def fetch_stock_details(df):
    """使用 yfinance 抓取行業資料"""
    print(f"開始抓取 {len(df)} 檔股票的行業資料...")
    results = []
    
    # 建立 Yahoo 格式 ticker
    df["ticker"] = df["SYMBOL"].astype(str).str.strip() + ".NS"
    
    for i, row in df.iterrows():
        ticker = row["ticker"]
        symbol_only = row["SYMBOL"]
        
        if i % 50 == 0:
            print(f"進度: {i}/{len(df)}")
            
        try:
            # 僅抓取必要資訊以加快速度
            stock = yf.Ticker(ticker)
            info = stock.info
            
            sector = info.get("sector", "Unclassified")
            industry = info.get("industry", "Unclassified")
            
            results.append({
                "SYMBOL": symbol_only,
                "ticker": ticker,
                "sector": sector,
                "industry": industry,
                "website": info.get("website", "N/A"),
                "longBusinessSummary": info.get("longBusinessSummary", "N/A")[:200] + "..." # 摘要
            })
        except Exception:
            results.append({
                "SYMBOL": symbol_only,
                "ticker": ticker,
                "sector": "Error/Timeout",
                "industry": "Error/Timeout",
                "website": "N/A",
                "longBusinessSummary": "N/A"
            })
        
        # 節流避免被 Yahoo 封鎖
        time.sleep(0.2)
        
    return pd.DataFrame(results)

def run():
    try:
        if not GDRIVE_ROOT_FOLDER_ID:
            print("❌ 錯誤: 找不到 IN_STOCKLIST 資料夾 ID")
            sys.exit(1)

        service = get_drive_service()
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}

        # 1. 下載 NSE 原始清單
        print("正在從 NSE 下載最新股票清單...")
        resp = requests.get(EQUITY_LIST_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        df_nse = pd.read_csv(io.BytesIO(resp.content))
        
        # 2. 抓取 Yahoo 行業資料
        # 注意：若股票太多(2000+)，測試時建議先用 df_nse.head(50) 測試
        df_details = fetch_stock_details(df_nse)
        
        # 3. 合併資料 (將行業資料併回原始 NSE 清單)
        df_final = pd.merge(df_nse, df_details, on="SYMBOL", how="left")
        
        # 4. 準備上傳
        final_file_name = "NSE_Market_Sector_Data.csv"
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, index=False, encoding='utf-8-sig') # 使用 utf-8-sig 確保 Excel 開啟不亂碼
        
        # 5. 雲端同步：先刪舊的，再傳新的
        delete_existing_file(service, final_file_name, GDRIVE_ROOT_FOLDER_ID)
        
        media = MediaIoBaseUpload(
            io.BytesIO(csv_buffer.getvalue().encode('utf-8-sig')), 
            mimetype='text/csv', 
            resumable=True
        )
        file_metadata = {'name': final_file_name, 'parents': [GDRIVE_ROOT_FOLDER_ID]}
        
        print(f"正在上傳整合後的檔案: {final_file_name}")
        service.files().create(
            body=file_metadata, 
            media_body=media, 
            supportsAllDrives=True
        ).execute()
        
        print(f"✅ 整合檔案處理完成並已同步至 Google Drive")

    except Exception as e:
        print(f"❌ 發生錯誤: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run()
