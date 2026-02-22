# effects/utils.py
from PIL import ImageFont
from pathlib import Path
import os

def load_font(size: int):
    candidates = [
        os.getenv("SUBTITLE_FONT", ""),
        r"C:\Windows\Fonts\msjh.ttc",
        r"C:\Windows\Fonts\mingliu.ttc",
    ]
    for fp in candidates:
        if fp and Path(fp).exists():
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                pass
    return ImageFont.load_default()

def with_position(clip, pos):
    return clip.set_position(pos) if hasattr(clip, "set_position") else clip.with_position(pos)

def with_mask(clip, mask):
    return clip.set_mask(mask) if hasattr(clip, "set_mask") else clip.with_mask(mask)
