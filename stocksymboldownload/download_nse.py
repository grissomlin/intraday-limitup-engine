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

# 環境變數設定
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64")
GDRIVE_ROOT_FOLDER_ID = os.environ.get("IN_STOCKLIST") 

# NSE 檔案來源
URL_EQUITY_L = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
URL_SEC_LIST = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"

def get_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise ValueError("❌ 找不到 GDRIVE_TOKEN_B64 環境變數")

    try:
        decoded_data = base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8")
        token_info = json.loads(decoded_data)
    except Exception as e:
        raise ValueError(f"❌ Base64 解碼或 JSON 解析失敗: {e}")

    # 【核心修正】不手動指定 Scopes，直接讀取 Token 檔案內建的權限
    # 這樣可以避開 invalid_scope 報錯
    try:
        creds = Credentials.from_authorized_user_info(token_info)
        
        if creds.expired and creds.refresh_token:
            print("🔄 Token 已過期，嘗試自動刷新...")
            try:
                creds.refresh(Request())
            except Exception as refresh_err:
                print(f"❌ Token 刷新失敗，請檢查 Client ID/Secret 是否正確: {refresh_err}")
                raise
                
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"❌ 憑證初始化失敗: {e}")
        raise

def delete_existing_file(service, file_name, folder_id):
    """清理資料夾內同名的舊檔案，確保只保留最新版"""
    try:
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)", 
                                     supportsAllDrives=True, 
                                     includeItemsFromAllDrives=True).execute()
        for item in results.get('files', []):
            print(f"正在清理舊檔案: {item['name']} (ID: {item['id']})")
            service.files().delete(fileId=item['id'], supportsAllDrives=True).execute()
    except Exception as e:
        print(f"⚠️ 清理舊檔案時發生輕微錯誤 (可能無舊檔): {e}")

def run():
    try:
        if not GDRIVE_ROOT_FOLDER_ID:
            print("❌ 錯誤: 找不到 IN_STOCKLIST 資料夾 ID")
            sys.exit(1)

        service = get_drive_service()
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}

        # 1. 下載資料
        print("📥 正在從 NSE 下載原始清單...")
        res_base = requests.get(URL_EQUITY_L, headers=headers, timeout=30)
        res_band = requests.get(URL_SEC_LIST, headers=headers, timeout=30)
        
        df_base = pd.read_csv(io.BytesIO(res_base.content))
        df_band = pd.read_csv(io.BytesIO(res_band.content))
        
        df_base.columns = df_base.columns.str.strip()
        df_band.columns = df_band.columns.str.strip()

        # 2. 合併資料 (Merge)
        print("🔗 正在整合漲跌幅限制 (Price Band)...")
        df_merged = pd.merge(
            df_base[['SYMBOL', 'NAME OF COMPANY']], 
            df_band[['Symbol', 'Band', 'Remarks']], 
            left_on='SYMBOL', right_on='Symbol', how='left'
        ).drop(columns=['Symbol'])

        # 3. 抓取 yfinance 行業資訊
        print(f"🔍 開始抓取行業資訊 (總計 {len(df_merged)} 檔)...")
        industry_data = []
        for i, row in df_merged.iterrows():
            ticker = f"{row['SYMBOL']}.NS"
            if i % 100 == 0: print(f"進度: {i}/{len(df_merged)}")
            
            try:
                # 僅抓取基礎 info
                info = yf.Ticker(ticker).info
                industry_data.append({
                    "SYMBOL": row['SYMBOL'],
                    "sector": info.get("sector", "Unclassified"),
                    "industry": info.get("industry", "Unclassified")
                })
            except:
                industry_data.append({"SYMBOL": row['SYMBOL'], "sector": "Error", "industry": "Error"})
            
            time.sleep(0.15) # 稍微節流避免被 Yahoo 封鎖

        df_industry = pd.DataFrame(industry_data)
        df_final = pd.merge(df_merged, df_industry, on="SYMBOL", how="left")

        # 4. 上傳至 Google Drive
        final_file_name = "NSE_Stock_Master_Data.csv"
        delete_existing_file(service, final_file_name, GDRIVE_ROOT_FOLDER_ID)

        csv_buffer = io.BytesIO()
        df_final.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        csv_buffer.seek(0)

        media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv', resumable=True)
        file_metadata = {'name': final_file_name, 'parents': [GDRIVE_ROOT_FOLDER_ID]}
        
        print(f"📤 正在上傳最終整合檔案...")
        service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
        
        print("✅ 任務完成！")

    except Exception as e:
        print(f"❌ 執行過程中發生錯誤: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()
