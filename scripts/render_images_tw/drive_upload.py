# scripts/render_images_tw/drive_upload.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from scripts.utils.drive_uploader import ensure_folder, get_drive_service, upload_dir
from .utils_tw import make_drive_subfolder_name


def upload_pngs_to_drive(
    *,
    outdir: Path,
    payload: Dict[str, Any],
    root_folder_id: str,
    market_name: str,
    drive_client_secret: Optional[str],
    drive_token: Optional[str],
    drive_subfolder: Optional[str],
    drive_workers: int,
    drive_no_concurrent: bool,
    drive_no_overwrite: bool,
    drive_quiet: bool,
) -> int:
    svc = get_drive_service(
        client_secret_file=drive_client_secret,
        token_file=drive_token,
    )

    root_id = str(root_folder_id).strip()
    market_name = str(market_name or "TW").strip().upper()

    market_folder_id = ensure_folder(svc, root_id, market_name)

    if drive_subfolder:
        subfolder = str(drive_subfolder).strip()
    else:
        subfolder = make_drive_subfolder_name(payload, market=market_name)

    print(f"📁 Target Drive folder: root/{market_name}/{subfolder}/")

    uploaded = upload_dir(
        svc,
        market_folder_id,
        outdir,
        pattern="*.png",
        recursive=False,
        overwrite=(not drive_no_overwrite),
        verbose=(not drive_quiet),
        concurrent=(not drive_no_concurrent),
        workers=int(drive_workers),
        subfolder_name=subfolder,
    )
    return int(uploaded)
