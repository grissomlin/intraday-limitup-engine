# scripts/render_images_common/overview/strings.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any


# =============================================================================
# Title
# =============================================================================
def title_for_metric(metric: str, market: str, lang: str) -> str:
    m = (metric or "").lower()
    mkt = (market or "").upper().strip()

    # -----------------------------
    # Thai
    # -----------------------------
    if lang == "th":
        if m == "gainbins":
            return "จำนวนหุ้นตามช่วงผลตอบแทน\n(10%+ / ไม่รวมขาลง)"
        if m == "mix":
            return "หุ้นเด่นตามกลุ่มอุตสาหกรรม\n(10%+ / ติดซิลลิ่ง / แตะซิลลิ่ง)"
        if m == "locked":
            return "หุ้นติดซิลลิ่งตามกลุ่มฯ"
        if m == "touched":
            return "หุ้นแตะซิลลิ่งตามกลุ่มฯ"
        if m == "bigmove10":
            return "หุ้น +10% ตามกลุ่มอุตสาหกรรม"
        if m in ("locked+touched", "locked_plus_touched"):
            return "หุ้นซิลลิ่ง (ติด/แตะ) ตามกลุ่มฯ"
        return "จำนวนหุ้นตามกลุ่มอุตสาหกรรม"

    # -----------------------------
    # Korean
    # -----------------------------
    if lang == "ko":
        if m == "gainbins":
            return "상승률 구간별 종목 수\n(10%+ / 하락 제외)"
        if m == "mix":
            return "업종별 강세 종목 수\n(10%+ / 상한가 / 터치)"
        if m == "locked":
            return "업종별 상한가 종목 수"
        if m == "touched":
            return "업종별 상한가 터치 종목 수"
        if m == "bigmove10":
            return "업종별 10%+ 상승 종목 수"
        if m in ("locked+touched", "locked_plus_touched"):
            return "업종별 상한가(터치 포함) 종목 수"
        return "업종별 종목 수"

    # -----------------------------
    # Japanese
    # -----------------------------
    if lang == "ja":
        if m == "gainbins":
            return "上昇率レンジ別 銘柄数\n(10%+ / 下落は除外)"
        if m == "mix":
            return "業種別 強い銘柄数\n(10%+ / ストップ高 / 到達)"
        if m == "bigmove10":
            return "業種別 10%+ 上昇 銘柄数"
        if m == "locked":
            return "業種別 ストップ高 銘柄数"
        if m == "touched":
            return "業種別 ストップ高到達 銘柄数"
        if m in ("locked+touched", "locked_plus_touched"):
            return "業種別 ストップ高（到達含む）銘柄数"
        return "業種別 銘柄数"

    # -----------------------------
    # English
    # -----------------------------
    if lang == "en":
        # ✅ Always show country/market prefix for English markets
        prefix_map = {
            "US": "US ",
            "CA": "Canada ",
            "AU": "Australia ",
            "UK": "UK ",
            "CN": "China ",
            "HK": "Hong Kong ",
            "JP": "Japan ",
            "KR": "Korea ",
            "TW": "Taiwan ",
            "TH": "Thailand ",
            "EU": "EU ",
        }
        prefix = prefix_map.get(mkt, f"{mkt} " if mkt else "")

        if m == "gainbins":
            return f"{prefix}Gainers by Return Band\n(10%+ / no decliners)"
        if m == "bigmove10":
            return f"{prefix}Top Sectors\n(10%+ Movers)"
        if m == "locked":
            return f"{prefix}Sector Limit-Up Count"
        if m == "touched":
            return f"{prefix}Sector Touched Limit-Up Count"
        if m in ("locked+touched", "locked_plus_touched"):
            return f"{prefix}Sector Limit-Up Count\n(locked + touched)"
        if m == "mix":
            return f"{prefix}Sector Momentum Count\n(10%+ / limit-up / touched)"
        return f"{prefix}Sector Count"

    # -----------------------------
    # Simplified Chinese
    # -----------------------------
    if lang == "zh-cn":
        prefix_map_cn = {
            "CN": "中国",
            "TW": "台湾",
            "HK": "香港",
            "JP": "日本",
            "KR": "韩国",
            "TH": "泰国",
            "US": "美国",
            "CA": "加拿大",
            "AU": "澳大利亚",
            "UK": "英国",
            "EU": "欧盟",
        }
        cn_prefix = prefix_map_cn.get(mkt, mkt) if mkt else ""
        pre = f"{cn_prefix}" if cn_prefix else ""

        if m == "gainbins":
            return f"{pre}涨幅分箱家数\n(10%+ / 不含下跌)"
        if m == "locked":
            return f"{pre}行业别涨停家数(10%+)"
        if m == "touched":
            return f"{pre}行业炸板家数"  # 你原本就用“炸板”，保留
        if m in ("locked+touched", "locked_plus_touched"):
            return f"{pre}行业涨停+炸板家数"
        if m == "bigmove10":
            return f"{pre}行业大涨10%+家数"
        if m == "mix":
            # ✅ 觸及 -> 涨停失败（命名一致）
            return f"{pre}行业热度家数\n(10%+ / 涨停 / 涨停失败)"
        return f"{pre}行业家数统计"

    # -----------------------------
    # zh-tw (default)
    # -----------------------------
    prefix_map_tw = {
        "TW": "台灣",
        "CN": "中國",
        "HK": "香港",
        "JP": "日本",
        "KR": "韓國",
        "TH": "泰國",
        "US": "美國",
        "CA": "加拿大",
        "AU": "澳洲",
        "UK": "英國",
        "EU": "歐盟",
    }
    tw_prefix = prefix_map_tw.get(mkt, mkt) if mkt else ""
    pre = f"{tw_prefix}" if tw_prefix else ""

    if m == "gainbins":
        return f"{pre}漲幅分箱家數\n(10%+ / 不含下跌)"
    if m == "locked":
        return f"{pre}行業別漲停家數(10%+)"
    if m == "touched":
        # ✅ 觸及 -> 漲停失敗（命名一致）
        return f"{pre}行業別漲停失敗家數"
    if m == "bigmove10":
        return f"{pre}行業別大漲 10%+ 家數"
    if m in ("locked+touched", "locked_plus_touched"):
        return f"{pre}行業別漲停（含漲停失敗）家數"
    if m == "mix":
        # ✅ 觸及 -> 漲停失敗（命名一致）
        return f"{pre}行業別熱度家數\n(10%+ / 漲停 / 漲停失敗)"
    return f"{pre}行業別家數統計"


# =============================================================================
# Footer Right
# =============================================================================
def footer_right_for_market(market: str, lang: str, normalize_market) -> str:
    market = normalize_market(market)
    if market == "CN":
        return ""

    if lang == "th":
        return "สแนปช็อต"

    if lang == "ko":
        return "장중 스냅샷"
    if lang == "ja":
        return "スナップショット"
    if lang == "en":
        return "snapshot"
    return ""


# =============================================================================
# Footer Note (only for bigmove/mix)
# =============================================================================
def footer_note(metric: str, market: str, lang: str, normalize_market) -> str:
    market = normalize_market(market)
    m = (metric or "").lower()

    if m in ("locked", "touched", "locked+touched", "locked_plus_touched", "gainbins"):
        return ""

    EN_BIGMOVE_MARKETS = {"US", "CA", "AU", "UK"}

    FOOTNOTE_RULES = {
        ("bigmove10", "*"): {
            "en": "10%+ gainers (proxy for limit-up)\n(no daily price limit)",
            "zh": "註：10%+ 代表強勢股\n（無固定漲停制度）",
            "ja": "注：10%+ 上昇は強い銘柄の目安\n（値幅制限のない市場向け）",
            "ko": "주: 10%+ 상승은 강세 종목의 지표\n(상한가 제도 없는 시장용)",
            "th": "หมายเหตุ: 10%+ = หุ้นเด่น\n(ตลาดนี้ไม่มีเพดานรายวัน)",
        },
        ("mix", "KR"): {
            "en": "10%+ movers + limit-up + touched\n(KR limit-up = 30%)",
            "zh": "註：韓國漲停為 30%\n本榜含 10%+ / 漲停 / 觸及",
            "ja": "注：韓国のストップ高は 30%\n本榜は 10%+ / ストップ高 / 到達 を含む",
            "ko": "주: 한국 상한가는 30%\n본 목록은 10%+ / 상한가 / 터치 포함",
            "th": "หมายเหตุ: เกาหลีเพดาน 30%\nรวม 10%+ / ติดซิลลิ่ง / แตะซิลลิ่ง",
        },
    }

    # ✅ keep original rule: bigmove10 note only for no-limit markets
    if m == "bigmove10" and market not in EN_BIGMOVE_MARKETS:
        return ""

    rule = FOOTNOTE_RULES.get((m, market)) or FOOTNOTE_RULES.get((m, "*"))
    if not rule:
        return ""

    if lang == "th":
        return rule.get("th", "")
    if lang == "en":
        return rule.get("en", "")
    if lang.startswith("zh"):
        return rule.get("zh", "")
    if lang == "ja":
        return rule.get("ja", "")
    if lang == "ko":
        return rule.get("ko", "")
    return ""


# =============================================================================
# Empty text
# =============================================================================
def empty_text_for_metric(metric: str, lang: str) -> str:
    m = (metric or "").lower()

    if lang == "th":
        if m == "gainbins":
            return "วันนี้ไม่มีหุ้นบวก"
        if m == "mix":
            return "วันนี้ไม่มีกลุ่มเด่น"
        if m == "locked":
            return "วันนี้ไม่มีหุ้นติดซิลลิ่ง"
        if m == "touched":
            return "วันนี้ไม่มีหุ้นแตะซิลลิ่ง"
        if m == "bigmove10":
            return "วันนี้ไม่มีกลุ่ม 10%+"
        return "วันนี้ไม่มีข้อมูล"

    if lang == "ko":
        if m == "gainbins":
            return "오늘 상승 종목 없음"
        if m == "mix":
            return "오늘 강세 업종 없음"
        if m == "locked":
            return "오늘 상한가 업종 없음"
        if m == "touched":
            return "오늘 상한가 터치 업종 없음"
        if m == "bigmove10":
            return "오늘 10%+ 업종 없음"
        return "오늘 데이터 없음"

    if lang == "ja":
        if m == "gainbins":
            return "本日 上昇銘柄なし"
        if m == "locked":
            return "本日ストップ高の業種なし"
        if m == "touched":
            return "本日ストップ高到達の業種なし"
        if m == "bigmove10":
            return "本日10%+の業種なし"
        if m == "mix":
            return "本日強い業種なし"
        return "本日データなし"

    if lang == "en":
        if m == "gainbins":
            return "No gainers today"
        if m == "bigmove10":
            return "No 10%+ sectors today"
        if m == "mix":
            return "No momentum sectors today"
        return "No data today"

    if lang == "zh-cn":
        if m == "gainbins":
            return "今日无上涨股票"
        if m == "locked":
            return "今日无涨停行业"
        if m == "touched":
            return "今日无炸板行业"
        if m == "bigmove10":
            return "今日无大涨10%+行业"
        if m == "mix":
            return "今日无热度行业"
        return "今日无上榜行业"

    # zh-tw default
    if m == "gainbins":
        return "今日無上漲股票"
    if m == "locked":
        return "今日無漲停（鎖死）行業"
    if m == "touched":
        return "今日無漲停失敗行業"
    if m == "bigmove10":
        return "今日無大漲 10%+ 行業"
    if m == "mix":
        return "今日無熱度（10%+ / 漲停 / 漲停失敗）行業"
    return "今日無上榜行業"


# =============================================================================
# Breadth legend (pct meaning)
# =============================================================================
def breadth_legend_text(lang: str, metric: str = "") -> str:
    m = (metric or "").lower()
    if m == "gainbins":
        if lang == "th":
            return "สัดส่วน = จำนวนในช่วง ÷ ทั้งตลาด"
        if lang == "zh-cn":
            return "占比 = 该区间家数 ÷ 全市场总数"
        if lang == "zh-tw":
            return "佔比 = 該區間家數 ÷ 全市場總數"
        if lang == "ja":
            return "割合 = そのレンジの銘柄数 ÷ 全市場総数"
        if lang == "ko":
            return "비중 = 구간 종목수 ÷ 전체 시장"
        return "% of market = band count ÷ market total"

    if lang == "th":
        return "สัดส่วน = หุ้นเด่น ÷ ทั้งกลุ่ม"
    if lang == "zh-cn":
        return "占比 = 强势股 ÷ 行业总数"
    if lang == "zh-tw":
        return "佔比 = 強勢股 ÷ 行業總數"  # ✅ typo fix
    if lang == "ja":
        return "割合 = 強い銘柄 ÷ 業種総数"
    if lang == "ko":
        return "비중 = 강세종목 ÷ 업종총수"
    return "% of sector = movers ÷ sector total"


# =============================================================================
# Disclaimer (one line)
# =============================================================================
def disclaimer_one_line(lang: str) -> str:
    if lang == "th":
        return "คำเตือน: เพื่อการเรียนรู้ ไม่ใช่คำแนะนำการลงทุน"
    if lang == "zh-cn":
        return "免责声明：仅供学习交流，不构成投资建议"
    if lang == "zh-tw":
        return "免責聲明：僅供學習交流，不構成投資建議"
    if lang == "ja":
        return "免責：学習目的であり投資助言ではありません"
    if lang == "ko":
        return "면책: 학습용이며 투자 조언이 아닙니다"
    return "Disclaimer: For learning only. Not financial advice."


def safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""