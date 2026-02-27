# dashboard/app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import gradio as gr

from scripts.utils.drive_uploader import get_drive_service  # reuse your auth + refresh


MARKETS = ["us", "tw", "jp", "kr", "th", "cn", "ca", "uk", "au"]
SLOTS = ["open", "midday", "close"]

ROOT_ID = (os.getenv("GDRIVE_ROOT_FOLDER_ID") or "").strip()


def _iso_to_dt(s: str) -> datetime:
    ss = (s or "").strip()
    if ss.endswith("Z"):
        ss = ss[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ss)
    except Exception:
        return datetime.min


def _fmt_time(s: str) -> str:
    dt = _iso_to_dt(s)
    if dt == datetime.min:
        return s or ""
    return dt.isoformat(timespec="seconds")


def _md_link(url: str, text: str) -> str:
    if not url:
        return text
    return f"[{text}]({url})"


def _drive_list(service, *, parent_id: str, q_extra: str, fields: str, page_size: int = 200) -> List[dict]:
    out: List[dict] = []
    page_token = None
    base_q = f"'{parent_id}' in parents and trashed = false"
    q = f"{base_q} and ({q_extra})" if q_extra else base_q

    while True:
        resp = (
            service.files()
            .list(
                q=q,
                fields=f"nextPageToken,files({fields})",
                pageSize=page_size,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        out.extend(resp.get("files", []) or [])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out


def _list_child_folders(service, parent_id: str) -> List[dict]:
    return _drive_list(
        service,
        parent_id=parent_id,
        q_extra="mimeType = 'application/vnd.google-apps.folder'",
        fields="id,name,modifiedTime,webViewLink",
        page_size=200,
    )


def _list_child_files(service, parent_id: str) -> List[dict]:
    return _drive_list(
        service,
        parent_id=parent_id,
        q_extra="mimeType != 'application/vnd.google-apps.folder'",
        fields="id,name,mimeType,modifiedTime,size,webViewLink",
        page_size=500,
    )


def _find_folder_id_by_name(service, parent_id: str, name: str) -> Optional[str]:
    name_l = (name or "").strip().lower()
    for f in _list_child_folders(service, parent_id):
        if str(f.get("name") or "").strip().lower() == name_l:
            return str(f.get("id") or "")
    return None


def _find_file_by_name(service, parent_id: str, filename: str) -> Optional[dict]:
    fn_l = (filename or "").strip().lower()
    files = _list_child_files(service, parent_id)
    for f in files:
        if str(f.get("name") or "").strip().lower() == fn_l:
            return f
    return None


def _read_json_file_content(service, file_id: str) -> Optional[dict]:
    """
    Downloads file content (assumes it's small JSON).
    """
    try:
        data = service.files().get_media(fileId=file_id, supportsAllDrives=True).execute()
        if not data:
            return None
        if isinstance(data, (bytes, bytearray)):
            s = data.decode("utf-8", errors="replace")
        else:
            # sometimes google lib returns str
            s = str(data)
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


@dataclass
class LatestRow:
    market: str
    slot: str
    video_name: str
    video_link: str
    video_mtime: str
    video_size: str
    meta_youtube_id: str
    meta_privacy: str
    meta_link: str
    folder_path: str


def _resolve_latest_row(service, market: str, slot: str) -> LatestRow:
    """
    Direct path:
      ROOT/{MARKET}/Latest/{slot}/
        - latest_{slot}.mp4
        - latest_{slot}_images.zip (optional)
        - latest_meta.json (recommended)
    """
    market_u = market.upper()
    slot_l = slot.lower()

    if not ROOT_ID:
        return LatestRow(
            market=market_u,
            slot=slot_l,
            video_name="",
            video_link="",
            video_mtime="",
            video_size="",
            meta_youtube_id="",
            meta_privacy="",
            meta_link="",
            folder_path="(missing GDRIVE_ROOT_FOLDER_ID)",
        )

    # 1) ROOT -> MARKET
    market_folder = _find_folder_id_by_name(service, ROOT_ID, market)
    if not market_folder:
        return LatestRow(
            market=market_u,
            slot=slot_l,
            video_name="",
            video_link="",
            video_mtime="",
            video_size="",
            meta_youtube_id="",
            meta_privacy="",
            meta_link="",
            folder_path=f"{market_u}/(missing)",
        )

    # 2) MARKET -> Latest
    latest_folder = _find_folder_id_by_name(service, market_folder, "Latest")
    if not latest_folder:
        return LatestRow(
            market=market_u,
            slot=slot_l,
            video_name="",
            video_link="",
            video_mtime="",
            video_size="",
            meta_youtube_id="",
            meta_privacy="",
            meta_link="",
            folder_path=f"{market_u}/Latest(missing)",
        )

    # 3) Latest -> slot
    slot_folder = _find_folder_id_by_name(service, latest_folder, slot_l)
    if not slot_folder:
        return LatestRow(
            market=market_u,
            slot=slot_l,
            video_name="",
            video_link="",
            video_mtime="",
            video_size="",
            meta_youtube_id="",
            meta_privacy="",
            meta_link="",
            folder_path=f"{market_u}/Latest/{slot_l}(missing)",
        )

    folder_path = f"{market_u}/Latest/{slot_l}"

    # 4) find latest_{slot}.mp4
    video_fn = f"latest_{slot_l}.mp4"
    vf = _find_file_by_name(service, slot_folder, video_fn)

    video_name = str(vf.get("name") or "") if vf else ""
    video_link = str(vf.get("webViewLink") or "") if vf else ""
    video_mtime = str(vf.get("modifiedTime") or "") if vf else ""
    video_size = str(vf.get("size") or "") if vf else ""

    # 5) optional meta: latest_meta.json
    meta_fn = "latest_meta.json"
    mf = _find_file_by_name(service, slot_folder, meta_fn)

    ytid = ""
    privacy = ""
    meta_link = ""
    if mf and mf.get("id"):
        meta_link = str(mf.get("webViewLink") or "")
        obj = _read_json_file_content(service, str(mf["id"]))
        if obj:
            ytid = str(obj.get("youtube_video_id") or obj.get("video_id") or "").strip()
            privacy = str(obj.get("privacy") or "").strip()

    return LatestRow(
        market=market_u,
        slot=slot_l,
        video_name=video_name,
        video_link=video_link,
        video_mtime=video_mtime,
        video_size=video_size,
        meta_youtube_id=ytid,
        meta_privacy=privacy,
        meta_link=meta_link,
        folder_path=folder_path,
    )


def _render_md(rows: List[LatestRow]) -> str:
    md = []
    md.append("### ✅ Latest pointer (direct read)\n")
    md.append("| Market | Slot | Video | ModifiedTime | Size | YouTube ID | Privacy | Meta | Folder |")
    md.append("|---|---|---|---|---|---|---|---|---|")

    for r in rows:
        video_cell = _md_link(r.video_link, r.video_name or "❌ missing")
        meta_cell = _md_link(r.meta_link, "latest_meta.json") if r.meta_link else ""
        md.append(
            "| "
            + " | ".join(
                [
                    r.market,
                    r.slot,
                    video_cell,
                    _fmt_time(r.video_mtime),
                    (r.video_size or ""),
                    (r.meta_youtube_id or ""),
                    (r.meta_privacy or ""),
                    meta_cell,
                    (r.folder_path or ""),
                ]
            )
            + " |"
        )

    md.append("\n**Note:** YouTube ID / Privacy will appear after you upload `latest_meta.json` into each slot folder.")
    return "\n".join(md)


def refresh(slot_pick: str):
    if not ROOT_ID:
        return (
            "❌ Missing env `GDRIVE_ROOT_FOLDER_ID`.\n\n"
            "Set it to the same Drive root folder your pipeline uses."
        )

    svc = get_drive_service()
    slot_l = (slot_pick or "midday").strip().lower()

    rows = []
    for m in MARKETS:
        rows.append(_resolve_latest_row(svc, m, slot_l))

    return _render_md(rows)


def main():
    with gr.Blocks(title="Drive Latest Dashboard") as demo:
        gr.Markdown(
            "# 📊 Drive Latest Dashboard\n"
            "Direct read: `ROOT/{MARKET}/Latest/{slot}/latest_{slot}.mp4`\n\n"
            "Also reads optional `latest_meta.json` in the same folder for:\n"
            "- `youtube_video_id`\n"
            "- `privacy`\n"
        )

        slot_pick = gr.Dropdown([s for s in SLOTS], value="midday", label="Slot")
        btn = gr.Button("🔄 Refresh", variant="primary")
        out = gr.Markdown("Click Refresh.")

        btn.click(refresh, inputs=[slot_pick], outputs=[out])
        demo.load(refresh, inputs=[slot_pick], outputs=[out])

    demo.launch(server_name="0.0.0.0", server_port=int(os.getenv("PORT") or "7860"))


if __name__ == "__main__":
    main()
