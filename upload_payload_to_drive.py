# upload_payload_to_drive.py
# -*- coding: utf-8 -*-
"""
Upload main.py generated payload.json to Google Drive.
Supports:
- Local (VSCode): .env or local files
- GitHub Actions: Secrets (env vars)

Expected local cache path:
  data/cache/<market>/<ymd>/<slot>.payload.json

Drive target path (auto-created):
  <DRIVE_ROOT>/payload/<market>/<ymd>/<slot>.payload.json

Usage:
  python upload_payload_to_drive.py --market tw --slot midday
  python upload_payload_to_drive.py --market tw --slot close --ymd 2026-01-23
  python upload_payload_to_drive.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from datetime import datetime

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request

# =========================
# Defaults / Env Keys
# =========================
SCOPES = ["https://www.googleapis.com/auth/drive"]

# Fallbacks (used only if env not provided)
DEFAULT_DRIVE_ROOT_FOLDER_ID = "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"
DEFAULT_CLIENT_SECRET_FILE = "credentials.json"
TOKEN_FILE = "token.json"

ENV_DRIVE_ROOT = "GDRIVE_ROOT_FOLDER_ID"
ENV_CLIENT_SECRET_JSON = "GDRIVE_CLIENT_SECRET_JSON"
ENV_TOKEN_JSON = "GDRIVE_TOKEN_JSON"


# =========================
# Helpers
# =========================
def is_github_actions() -> bool:
    return os.getenv("GITHUB_ACTIONS", "").lower() == "true"


def repo_root() -> Path:
    return Path(__file__).resolve().parent


def today_ymd_local() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _escape_query_value(s: str) -> str:
    return (s or "").replace("'", "\\'")


def _write_temp_json(content: str, filename_hint: str) -> Path:
    d = Path(tempfile.mkdtemp(prefix="gdrive_"))
    p = d / filename_hint
    p.write_text(content, encoding="utf-8")
    return p


def resolve_client_secret_path(cli_value: str) -> Path:
    env_json = os.getenv(ENV_CLIENT_SECRET_JSON, "").strip()
    if env_json:
        return _write_temp_json(env_json, "credentials.json")

    p = repo_root() / cli_value
    if not p.exists():
        raise FileNotFoundError(
            f"找不到 OAuth client secrets：{p}\n"
            f"- 本機：請把 {cli_value} 放 repo 根目錄 或用 .env\n"
            f"- GitHub Actions：請設 Secret {ENV_CLIENT_SECRET_JSON}"
        )
    return p


def resolve_token_path() -> Path:
    env_json = os.getenv(ENV_TOKEN_JSON, "").strip()
    if env_json:
        return _write_temp_json(env_json, "token.json")

    return repo_root() / TOKEN_FILE


def get_drive_service(client_secret_path: Path, token_path: Path):
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if is_github_actions():
                raise RuntimeError(
                    "GitHub Actions 無法互動授權。\n"
                    "請先在本機跑 OAuth 取得 token.json，並把內容放到 Secret："
                    f"{ENV_TOKEN_JSON}"
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(client_secret_path), SCOPES)
            creds = flow.run_local_server(
                port=0,
                access_type="offline",
                prompt="consent",
            )
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds)


def find_child_folder(service, parent_id: str, name: str) -> str | None:
    q = (
        "mimeType='application/vnd.google-apps.folder' and "
        f"name='{_escape_query_value(name)}' and "
        f"'{parent_id}' in parents and trashed=false"
    )
    res = service.files().list(
        q=q,
        fields="files(id,name)",
        pageSize=10,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def ensure_folder(service, parent_id: str, name: str) -> str:
    fid = find_child_folder(service, parent_id, name)
    if fid:
        return fid
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(
        body=metadata,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return folder["id"]


def find_file_in_folder(service, parent_id: str, filename: str) -> str | None:
    q = (
        f"'{parent_id}' in parents and "
        f"name='{_escape_query_value(filename)}' and trashed=false"
    )
    res = service.files().list(
        q=q,
        fields="files(id,name,modifiedTime)",
        pageSize=5,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def upload_or_replace_file(service, parent_id: str, local_path: Path, drive_name: str) -> str:
    media = MediaFileUpload(str(local_path), resumable=True)
    existing_id = find_file_in_folder(service, parent_id, drive_name)
    if existing_id:
        updated = service.files().update(
            fileId=existing_id,
            media_body=media,
            fields="id,name,modifiedTime",
            supportsAllDrives=True,
        ).execute()
        print(f"✅ 已覆蓋：{updated['name']}  id={updated['id']}")
        return updated["id"]
    created = service.files().create(
        body={"name": drive_name, "parents": [parent_id]},
        media_body=media,
        fields="id,name",
        supportsAllDrives=True,
    ).execute()
    print(f"✅ 已上傳：{created['name']}  id={created['id']}")
    return created["id"]


def resolve_payload_path(market: str, slot: str, ymd: str) -> Path:
    return repo_root() / "data" / "cache" / market / ymd / f"{slot}.payload.json"


# =========================
# Main
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", default="tw")
    ap.add_argument("--slot", default="midday", choices=["midday", "close"])
    ap.add_argument("--ymd", default="")
    ap.add_argument(
        "--drive-root",
        default=os.getenv(ENV_DRIVE_ROOT, DEFAULT_DRIVE_ROOT_FOLDER_ID),
        help="Drive 根資料夾 ID（預設讀 env）",
    )
    ap.add_argument(
        "--client-secret",
        default=DEFAULT_CLIENT_SECRET_FILE,
        help="本機 OAuth client 檔名（env 會優先）",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    market = (args.market or "").strip().lower()
    slot = (args.slot or "").strip().lower()
    ymd = (args.ymd or "").strip() or today_ymd_local()

    payload_path = resolve_payload_path(market, slot, ymd)
    if not payload_path.exists():
        raise FileNotFoundError(
            f"找不到 payload：{payload_path}\n"
            f"請先跑：python main.py --market {market} --slot {slot} --force"
        )

    # Basic JSON sanity check
    json.loads(payload_path.read_text(encoding="utf-8"))

    client_secret_path = resolve_client_secret_path(args.client_secret)
    token_path = resolve_token_path()

    print("------------------------------------------------------------")
    print(f"📄 Local payload : {payload_path}")
    print(f"☁️ Drive root    : {args.drive_root}")
    print(f"📁 Drive path    : payload/{market}/{ymd}/{slot}.payload.json")
    print(f"🤖 Env mode      : {'GitHub Actions' if is_github_actions() else 'Local/VSCode'}")
    print("------------------------------------------------------------")

    if args.dry_run:
        print("🧪 dry-run：不會真的上傳。")
        return

    service = get_drive_service(client_secret_path, token_path)

    # Build folders: <root>/payload/<market>/<ymd>/
    folder_payload = ensure_folder(service, args.drive_root, "payload")
    folder_market = ensure_folder(service, folder_payload, market)
    folder_ymd = ensure_folder(service, folder_market, ymd)

    upload_or_replace_file(
        service,
        folder_ymd,
        payload_path,
        drive_name=f"{slot}.payload.json",
    )


if __name__ == "__main__":
    main()
