# scripts/render_images/utils_plotly.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import plotly.graph_objects as go


def _guess_chrome_path_windows() -> Optional[str]:
    candidates = [
        os.getenv("BROWSER_PATH", ""),
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    ]
    for p in candidates:
        if p and Path(p).exists():
            return p
    return None


def write_image_safe(
    fig: go.Figure,
    out_path: Path,
    *,
    width: int = 1080,
    height: int = 1920,
    scale: float = 2.0,
) -> bool:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # ✅ Kaleido v1 needs Chrome/Chromium
    # Plotly uses BROWSER_PATH if provided (as you were told earlier)
    if os.name == "nt":
        chrome = _guess_chrome_path_windows()
        if chrome:
            os.environ["BROWSER_PATH"] = chrome

    try:
        fig.write_image(str(out_path), width=width, height=height, scale=scale)
        print(f"✅ {out_path.name}")
        return True
    except Exception as e:
        print(f"❌ write_image 失敗：{out_path.name}")
        print("   常見原因：")
        print("   - 未安裝 Chrome / Chromium（Kaleido v1 需要）")
        print("   - BROWSER_PATH 未指定或路徑不對")
        print("   - 防毒或權限阻擋無頭瀏覽器")
        print(f"   Error: {e}")
        return False
