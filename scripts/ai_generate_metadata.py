# scripts/ai_generate_metadata.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8")

def render_template(tpl: str, ctx: Dict[str, str]) -> str:
    # very small templating: {{key}}
    for k, v in ctx.items():
        tpl = tpl.replace("{{" + k + "}}", v)
    return tpl

def infer_date_slot_from_video(video_path: Path) -> tuple[str, str]:
    name = video_path.stem
    # e.g. 2026-01-23_midday_typed / 2026-01-23_close
    m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
    date = m.group(1) if m else datetime.now().strftime("%Y-%m-%d")
    slot = "盤中" if ("midday" in name or "intraday" in name) else ("收盤" if ("close" in name or "after" in name) else "")
    return date, slot

def load_payload_summary(payload_path: Optional[Path]) -> Dict[str, Any]:
    if not payload_path:
        return {}
    if not payload_path.exists():
        return {}
    try:
        d = json.loads(payload_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    stats = d.get("stats", {}) if isinstance(d, dict) else {}
    leader = None
    # best-effort leader: sector_summary[0]
    ss = d.get("sector_summary") or d.get("sectors") or []
    if isinstance(ss, list) and ss:
        leader = ss[0]

    return {"stats": stats, "leader": leader}

def ai_make_bullets_openai(*, prompt: str) -> Optional[str]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None

    model = os.getenv("OPENAI_MODEL", "").strip()  # 建議你在 GitHub Secrets/Vars 設定
    if not model:
        # 沒給 model 就不冒險亂猜，走保底
        return None

    try:
        r = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "input": [
                    {"role": "system", "content": "你是股市影片文案助理。輸出需簡短、保守、避免投資建議口吻。"},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.4,
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        # Responses API: 取 output_text（保守處理）
        text = data.get("output_text")
        if isinstance(text, str) and text.strip():
            return text.strip()
    except Exception:
        return None
    return None

def main():
    ap = argparse.ArgumentParser(description="Generate YouTube metadata (title/description/tags) from templates + optional AI")
    ap.add_argument("--video", required=True, help="Path to mp4")
    ap.add_argument("--payload", default="", help="Optional payload json to summarize (best-effort)")
    ap.add_argument("--out", default="outputs/metadata.json", help="Output metadata json")
    ap.add_argument("--title-tpl", default="templates/youtube_title.txt")
    ap.add_argument("--desc-tpl", default="templates/youtube_description.md")
    ap.add_argument("--tags", default="templates/youtube_tags.txt")
    args = ap.parse_args()

    video_path = Path(args.video).expanduser().resolve()
    payload_path = Path(args.payload).expanduser().resolve() if args.payload else None

    date, slot = infer_date_slot_from_video(video_path)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    payload_info = load_payload_summary(payload_path)
    stats = payload_info.get("stats") or {}
    leader = payload_info.get("leader")

    # 保底 bullets（沒有 AI 也能跑）
    bullets: List[str] = []
    if leader:
        # leader 結構不一定一致，盡量抓
        leader_name = leader.get("sector") or leader.get("name") or leader.get("industry")
        leader_cnt = leader.get("limitup_count") or leader.get("limitups") or leader.get("count")
        if leader_name:
            if leader_cnt is not None:
                bullets.append(f"- 領先產業：{leader_name}（漲停 {leader_cnt}）")
            else:
                bullets.append(f"- 領先產業：{leader_name}")

    if "limitup_total" in stats:
        bullets.append(f"- 今日漲停總數：{stats.get('limitup_total')}")
    if "sectors_limitup_count" in stats:
        bullets.append(f"- 有漲停產業數：{stats.get('sectors_limitup_count')}")

    if not bullets:
        bullets = ["- 產業輪動快照（自動化產圖/上傳測試）"]

    # 嘗試用 AI 把 bullets 精煉成 2~4 行（選配）
    ai_prompt = (
        "請把下面資訊整理成 2~4 行 bullet（每行以「- 」開頭），語氣保守，不要投資建議。\n\n"
        f"日期：{date}\n"
        f"時段：{slot}\n"
        f"初稿 bullets：\n" + "\n".join(bullets)
    )
    ai_bullets = ai_make_bullets_openai(prompt=ai_prompt)
    summary_bullets = ai_bullets if ai_bullets else "\n".join(bullets)

    ctx = {
        "date": date,
        "slot": slot,
        "datetime": f"{date} {now_str.split(' ')[1]}",
        "summary_bullets": summary_bullets,
    }

    title_tpl = read_text(Path(args.title_tpl))
    desc_tpl = read_text(Path(args.desc_tpl))
    tags_lines = read_text(Path(args.tags)).splitlines()

    title = render_template(title_tpl, ctx).strip()
    description = render_template(desc_tpl, ctx).strip()
    tags = [t.strip() for t in tags_lines if t.strip() and not t.strip().startswith("#")]

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"title": title, "description": description, "tags": tags}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ metadata written: {out_path}")

if __name__ == "__main__":
    main()
