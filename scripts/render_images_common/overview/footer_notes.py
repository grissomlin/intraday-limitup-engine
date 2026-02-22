# scripts/render_images_common/overview/footer_notes.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Tuple

from .footer_i18n import is_zh_any, is_zh_cn


def note_lines(market: str, lang: str, *, note_mode: str = "exclusive") -> List[str]:
    """
    note_mode:
      - "exclusive": 10%+ excludes stop/touch  (overview / sector pages)
      - "inclusive": 10%+ includes stop/touch  (gain-bins page)
    """
    m = (market or "").upper()
    mode = (note_mode or "exclusive").strip().lower()
    lng = (lang or "").strip().lower()

    if m == "TH":
        note = (
            "※ 10%+ = ≥ +10% (รวม แตะซิลลิ่ง/ติดซิลลิ่ง)"
            if mode == "inclusive"
            else "※ 10%+ = ปิด ≥ +10% (ไม่รวม แตะซิลลิ่ง/ติดซิลลิ่ง)"
        )
        disclaimer = "คำเตือน: เพื่อการเรียนรู้ ไม่ใช่คำแนะนำการลงทุน"
        return [note, "", disclaimer]

    if m == "KR":
        note = (
            "※ 10%+ = 상승률 10% 이상 (상한가/터치 포함)"
            if mode == "inclusive"
            else "※ 10%+ = 종가 +10% 이상 (상한가/터치 제외)"
        )
        disclaimer = "면책: 학습용이며 투자 조언이 아닙니다"
        return [note, "", disclaimer]

    if m == "JP":
        note = (
            "※ 10%+ = 上昇率 10%以上（ストップ高/タッチ含む）"
            if mode == "inclusive"
            else "※ 10%+ = 終値 +10%以上（ストップ高/タッチ除外）"
        )
        disclaimer = "免責：学習用であり投資助言ではありません"
        return [note, "", disclaimer]

    if m in {"TW", "CN", "HK", "MO"} or is_zh_any(lng):
        is_cn = is_zh_cn(lng, m)
        if mode == "inclusive":
            note = "※ 10%+ = 涨幅 ≥ +10%（含涨停/触及）" if is_cn else "※ 10%+ = 漲幅 ≥ +10%（含漲停/觸及）"
        else:
            note = "※ 10%+ = 收盘 ≥ +10%（不含涨停/触及）" if is_cn else "※ 10%+ = 收盤 ≥ +10%（不含漲停/觸及）"
        disclaimer = "免责声明：仅供学习参考，非投资建议" if is_cn else "免責：僅供學習參考，非投資建議"
        return [note, "", disclaimer]

    return []


def pack_4(lines: List[str]) -> Tuple[str, str, str, str]:
    l = list(lines or [])
    while len(l) < 4:
        l.append("")
    return (l[0], l[1], l[2], l[3])
