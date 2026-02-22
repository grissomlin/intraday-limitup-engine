# scripts/youtube_auth.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

# ✅ upload + publish/update 都夠用（publish 需要 youtube scope）
SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]

def main():
    ap = argparse.ArgumentParser(description="YouTube OAuth: generate token json (upload+publish)")
    ap.add_argument("--client", default="secrets/youtube_client_secret.json", help="Path to OAuth client json")
    ap.add_argument("--out", default="secrets/youtube_token.upload.json", help="Output token json path")
    args = ap.parse_args()

    client_path = Path(args.client).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not client_path.exists():
        raise FileNotFoundError(f"找不到 OAuth client 檔：{client_path}")

    flow = InstalledAppFlow.from_client_secrets_file(str(client_path), SCOPES)

    # 兩個參數非常重要：確保拿到 refresh_token
    creds = flow.run_local_server(
        port=0,
        access_type="offline",
        prompt="consent",
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(creds.to_json(), encoding="utf-8")
    print(f"✅ 已寫出 token：{out_path}")

if __name__ == "__main__":
    main()
