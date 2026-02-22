import requests

# 填入你的資料
CLIENT_KEY = 'sbaw9onc2bhkc7zuad'
CLIENT_SECRET = 'EbfiiSMkNiZFGuPKq1eOnrhAa90izsu4'
AUTH_CODE = 'E5RrDCLRoXsvjze0f4ibp1qxRUxmoAGLcLqp24nGuVxVM0fdrl2MVqMiydSMQn9RhOvL4fTnY42HV6yWbc_C5cfwmD8HnpRoptMUu5-FdcddhFLUoO-DSh-sUfsH-3wpLhlsL1qYOSAORrlvuFxrGAzNeMfLyr_2MMMAEAj00dgpdbDv6F0AAIqvdB0ZNv-bLqPDtCTwETbyJcRi*v!6298.va'
REDIRECT_URI = 'https://grissomlin.github.io/'

url = "https://open.tiktokapis.com/v2/oauth/token/"

headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
}

data = {
    "client_key": CLIENT_KEY,
    "client_secret": CLIENT_SECRET,
    "code": AUTH_CODE,
    "grant_type": "authorization_code",
    "redirect_uri": REDIRECT_URI,
}

response = requests.post(url, headers=headers, data=data)

if response.status_code == 200:
    token_info = response.json()
    print("✅ 成功取得 Access Token!")
    print(f"Access Token: {token_info.get('access_token')}")
    print(f"Open ID: {token_info.get('open_id')}")
    print(f"Token 有效期 (秒): {token_info.get('expires_in')}")
else:
    print("❌ 換取失敗:")
    print(response.text)