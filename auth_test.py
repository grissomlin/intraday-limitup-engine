import os
import google_auth_oauthlib.flow
import googleapiclient.discovery

# 權限範圍：上傳影片
scopes = ["https://www.googleapis.com/auth/youtube.upload"]

def main():
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        "client_secrets.json", scopes)
    # 這行會彈出瀏覽器視窗
    credentials = flow.run_local_server(port=0)
    
    # 儲存長期通行證
    with open("token.json", "w") as token:
        token.write(credentials.to_json())
    print("授權成功！token.json 已產生。")

if __name__ == "__main__":
    main()