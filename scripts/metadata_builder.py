# scripts/metadata_builder.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict


# =============================================================================
# Helpers
# =============================================================================
def clean_tag(s: str) -> str:
    """Remove spaces / full-width spaces, keep tags compact."""
    return (s or "").replace(" ", "").replace("\u3000", "").strip()


def _safe_desc(s: str, *, limit: int = 4900) -> str:
    """
    YouTube description hard limit is 5000 chars.
    Keep it safely under 5000 and normalize newlines.
    Also avoid CRLF issues (Windows) by converting to LF.
    """
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(s) > limit:
        s = s[:limit].rstrip()
    return s


def _norm_market(market: str) -> str:
    """
    Normalize market code (aliases -> canonical).

    Canonical keys used across:
      - metadata_builder market field
      - youtube_playlists.json keys
      - latest_meta.json market field
    """
    m = (market or "").upper().strip()

    alias = {
        # India
        "IN": "INDIA",
        "IND": "INDIA",
        "NSE": "INDIA",

        # France (allow a few aliases just in case)
        "FRANCE": "FR",
        "PARIS": "FR",

        # Taiwan / China common variants (optional safety net)
        "TWSE": "TW",
        "TPEX": "TW",
        "CHINA": "CN",
        "A": "CN",

        # US common variants
        "USA": "US",
        "UNITEDSTATES": "US",
    }
    return alias.get(m, m)


# =============================================================================
# Description templates
# =============================================================================
def _desc_th() -> str:
    return _safe_desc(
        "สรุปหุ้นที่เคลื่อนไหวแรง (ชนเพดานราคา/เพิ่มขึ้นมาก) จัดกลุ่มตามอุตสาหกรรม\n"
        "\n"
        "--- Note ---\n"
        "This is an experiment: auto-generate Shorts from market data.\n"
        "\n"
        "--- Disclaimer ---\n"
        "For data organization only. NOT investment advice.\n"
        "Real-time calculations may be delayed or inaccurate. Please verify with official exchange sources.\n"
        "\n"
        "--- Data Quality ---\n"
        "Outliers (e.g. extreme % moves) can happen due to data glitches, corporate actions (splits), low liquidity, or symbol mapping.\n"
    )


def _desc_zh_tw() -> str:
    return _safe_desc(
        "整理盤中強勢異動個股與產業分布 (Shorts)\n"
        "\n"
        "--- Note ---\n"
        "This is an experiment: auto-generate Shorts from market data.\n"
        "\n"
        "--- Disclaimer ---\n"
        "僅供資料整理參考，非投資建議。\n"
        "盤中即時計算可能延遲或誤差，請以交易所官方資訊為準。\n"
        "\n"
        "--- Data Quality ---\n"
        "偶爾可能出現極端漲跌幅，可能來自資料異常、除權息/拆併股、低流動性成交或代碼對應問題。\n"
    )


def _desc_zh_cn() -> str:
    return _safe_desc(
        "汇总盘中强势异动个股与行业分布 (Shorts)\n"
        "\n"
        "--- Note ---\n"
        "This is an experiment: auto-generate Shorts from market data.\n"
        "\n"
        "--- Disclaimer ---\n"
        "仅供数据整理参考，不构成任何投资建议。\n"
        "盘中实时计算可能存在延迟或误差，请以交易所官方信息为准。\n"
        "\n"
        "--- Data Quality ---\n"
        "偶尔可能出现极端涨跌幅，可能来自数据异常、除权息/拆并股、低流动性成交或代码映射问题。\n"
    )


def _desc_jp() -> str:
    return _safe_desc(
        "本日の値動きが大きい銘柄を業種別にまとめます (Shorts)\n"
        "\n"
        "--- Note ---\n"
        "This is an experiment: auto-generate Shorts from market data.\n"
        "\n"
        "--- Disclaimer ---\n"
        "投資助言ではありません。参考情報です。\n"
        "リアルタイム計算は遅延や誤差が出る場合があります。公式発表でご確認ください。\n"
        "\n"
        "--- Data Quality ---\n"
        "極端な変動率はデータ不具合、株式分割/併合、流動性、シンボル対応などが原因の可能性があります。\n"
    )


def _desc_kr() -> str:
    return _safe_desc(
        "당일 변동 폭이 큰 종목을 업종별로 요약합니다 (Shorts)\n"
        "\n"
        "--- Note ---\n"
        "This is an experiment: auto-generate Shorts from market data.\n"
        "\n"
        "--- Disclaimer ---\n"
        "투자 조언이 아닙니다. 참고용입니다.\n"
        "실시간 계산은 지연/오류가 있을 수 있으니 공식 자료로 확인하세요.\n"
        "\n"
        "--- Data Quality ---\n"
        "극단적인 변동률은 데이터 오류, 액면분할/병합, 유동성, 심볼 매핑 이슈 등으로 발생할 수 있습니다.\n"
    )


def _desc_en(region_name: str) -> str:
    return _safe_desc(
        f"Highlights large movers in the {region_name} market, grouped by sector.\n"
        "\n"
        "--- Note ---\n"
        "This is an experiment: auto-generate Shorts from market-mover data.\n"
        "\n"
        "--- Disclaimer ---\n"
        "For data organization only. NOT investment advice.\n"
        "Real-time calculations may be delayed or inaccurate. Please verify with official exchange sources.\n"
        "\n"
        "--- Data Quality ---\n"
        "Outliers (e.g. extreme % moves) can occur due to data glitches, corporate actions (splits), low-liquidity prints, or symbol mapping.\n"
    )


def _desc_en_ca(region_name: str) -> str:
    """
    Canada-specific note:
    TSXV contains many microcaps where very low liquidity can distort % moves.
    """
    return _safe_desc(
        f"Highlights large movers in the {region_name} market, grouped by sector.\n"
        "\n"
        "--- Note ---\n"
        "This is an experiment: auto-generate Shorts from market-mover data.\n"
        "\n"
        "--- Disclaimer ---\n"
        "For data organization only. NOT investment advice.\n"
        "Real-time calculations may be delayed or inaccurate. Please verify with official exchange sources.\n"
        "\n"
        "--- Data Quality ---\n"
        "Outliers (e.g. extreme % moves) can occur due to data glitches, corporate actions (splits), low-liquidity prints, or symbol mapping.\n"
        "\n"
        "--- Canada (TSXV) Filter ---\n"
        "To reduce low-liquidity distortions, some TSXV microcaps may be excluded based on minimum liquidity and/or market cap.\n"
    )


def _desc_en_india() -> str:
    return _safe_desc(
        "Highlights large movers in the India (NSE) market, grouped by sector.\n"
        "\n"
        "--- Note ---\n"
        "This is an experiment: auto-generate Shorts from market-mover data.\n"
        "\n"
        "--- Disclaimer ---\n"
        "For data organization only. NOT investment advice.\n"
        "Real-time calculations may be delayed or inaccurate. Please verify with official exchange sources.\n"
        "\n"
        "--- Data Quality ---\n"
        "Outliers (e.g. extreme % moves) can occur due to data glitches, corporate actions (splits), low-liquidity prints, or symbol mapping.\n"
    )


def _desc_fr(region_name: str = "France") -> str:
    """
    France (Euronext Paris) – French copy.
    Keep it simple and consistent with other markets.
    """
    return _safe_desc(
        f"Résumé des plus fortes variations sur le marché {region_name}, regroupées par secteur.\n"
        "\n"
        "--- Note ---\n"
        "Ceci est une expérimentation : génération automatique de Shorts à partir des données de marché.\n"
        "\n"
        "--- Avertissement ---\n"
        "Contenu informatif uniquement. Ce n'est PAS un conseil en investissement.\n"
        "Les calculs quasi temps réel peuvent être retardés ou imprécis. Veuillez vérifier via les sources officielles.\n"
        "\n"
        "--- Qualité des données ---\n"
        "Des valeurs aberrantes (variations extrêmes) peuvent survenir à cause d'erreurs de données, d'opérations sur titres (split),\n"
        "de faibles volumes/liquidité, ou de problèmes de correspondance des symboles.\n"
    )


# =============================================================================
# Public API
# =============================================================================
def build_metadata(market: str, ymd: str, slot: str) -> Dict[str, Any]:
    """
    Centralized YouTube metadata builder.
    Keep descriptions short, safe, and under 5000 chars.
    """
    m_in = (market or "").upper().strip()
    m = _norm_market(m_in)
    slot = (slot or "midday").strip()

    if m == "TW":
        title = f"TW｜台股異動速報｜{ymd} {slot}"
        desc = _desc_zh_tw()
        tags = ["TW", "台股", "異動速報", "Shorts"]

    elif m == "CN":
        title = f"CN｜A股异动速报｜{ymd} {slot}"
        desc = _desc_zh_cn()
        tags = ["CN", "A股", "异动速报", "Shorts"]

    elif m == "JP":
        title = f"JP｜日本株 異動速報｜{ymd} {slot}"
        desc = _desc_jp()
        tags = ["JP", "日本株", "異動速報", "Shorts"]

    elif m == "KR":
        title = f"KR｜한국 주식 급등락 속보｜{ymd} {slot}"
        desc = _desc_kr()
        tags = ["KR", "한국주식", "급등락", "속보", "Shorts"]

    elif m == "TH":
        title = f"TH｜หุ้นไทย ด่วนเคลื่อนไหว｜{ymd} {slot}"
        desc = _desc_th()
        tags = ["TH", "หุ้นไทย", "Shorts"]

    elif m == "INDIA":
        title = f"INDIA｜NSE Market Movers｜{ymd} {slot}"
        desc = _desc_en_india()
        tags = ["INDIA", "NSE", "MarketMovers", "Shorts"]

    elif m == "FR":
        # ✅ France in French
        title = f"FR｜Marché français : fortes variations｜{ymd} {slot}"
        desc = _desc_fr("France")
        tags = ["FR", "France", "Euronext", "Paris", "Variations", "Shorts"]

    elif m in ("US", "CA", "UK", "AU"):
        name_map = {
            "US": "U.S.",
            "CA": "Canadian",
            "UK": "UK",
            "AU": "Australian",
        }
        region = name_map[m]
        title = f"{m}｜{region} Market Movers｜{ymd} {slot}"

        # ✅ CA-specific description
        if m == "CA":
            desc = _desc_en_ca(region)
        else:
            desc = _desc_en(region)

        tags = [m, "MarketMovers", "Shorts"]

    else:
        raise ValueError(f"Unsupported market code: {m}")

    tags = [clean_tag(t) for t in tags if clean_tag(t)]

    return {
        "market": m,  # ✅ canonical (IN -> INDIA)
        "title": title,
        "description": desc,
        "tags": tags,
    }
