from __future__ import print_function
import os.path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/drive']

def main():
    creds = None
    
    # token.json 存在就讀取
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # 如果沒有憑證、憑證無效或過期
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print("Refresh token 失敗，可能 scope 已變更 → 將自動重新授權")
                print(e)
                creds = None  # 強制重新授權
        
        # 如果還是沒有有效憑證 → 重新跑 OAuth 流程
        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret_120088082173-o54bji11oh6hd8snv2ldhm06bp10r28i.apps.googleusercontent.com.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # 儲存新的 token
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)
    
    # 列出前 10 個檔案
    results = service.files().list(
        pageSize=10, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    if not items:
        print('沒有找到檔案。')
    else:
        print('檔案：')
        for item in items:
            print(f"{item['name']} ({item['id']})")

if __name__ == '__main__':
    main()