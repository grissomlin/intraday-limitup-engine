import requests
import os

# --- 設定區 ---
ACCESS_TOKEN = 'act.JPazCJVSfKkn1xROkUJilmG0XsOvXMAedgUJQ4kn1uSeAZORgUQDgOQ0RIrP!6294.va'

VIDEO_PATH = 'C:/code/intraday-limitup-engine/media/videos/2026-01-23_midday.mp4'
CHUNK_SIZE = 10 * 1024 * 1024 

def upload_video():
    # 1. 取得檔案大小
    file_size = os.path.getsize(VIDEO_PATH)
    
    # 2. 初始化上傳 (Initialize)
    init_url = "https://open.tiktokapis.com/v2/post/publish/video/init/"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json; charset=UTF-8"
    }
    
    # 這裡加入了 post_info，並設定為私密影片 (SELF_ONLY)
    init_data = {
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": file_size,
            "chunk_size": file_size,
            "total_chunk_count": 1
        },
        "post_info": {
            "title": "Sandbox Test Video",
            "privacy_level": "SELF_ONLY"  # 解決 unaudited_client 錯誤的關鍵
        }
    }

    print("🚀 正在初始化上傳...")
    init_res = requests.post(init_url, headers=headers, json=init_data)
    
    if init_res.status_code != 200:
        print(f"❌ 初始化失敗: {init_res.text}")
        return

    res_data = init_res.json().get('data', {})
    upload_url = res_data.get('upload_url')
    publish_id = res_data.get('publish_id')

    if not upload_url:
        print(f"❌ 無法取得上傳網址: {init_res.json()}")
        return

    # 3. 執行二進位上傳 (Binary Upload)
    print("📤 正在傳送影片位元組...")
    with open(VIDEO_PATH, 'rb') as f:
        video_binary = f.read()
        
    upload_headers = {
        "Content-Type": "video/mp4",
        "Content-Range": f"bytes 0-{file_size-1}/{file_size}"
    }
    
    upload_res = requests.put(upload_url, headers=upload_headers, data=video_binary)

    if upload_res.status_code in [200, 201]:
        print(f"✅ 上傳成功！")
        print(f"Publish ID: {publish_id}")
        print("提示：影片已成功上傳至私密狀態 (僅自己可見)。")
    else:
        print(f"❌ 上傳位元組失敗: {upload_res.status_code}")
        print(upload_res.text)

if __name__ == "__main__":
    if os.path.exists(VIDEO_PATH):
        upload_video()
    else:
        print(f"找不到檔案: {VIDEO_PATH}")