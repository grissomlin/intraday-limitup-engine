# dashboard/components/pages/errors.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, Any
import pandas as pd
import streamlit as st


def render_page(*, errors_df: pd.DataFrame, payload: Dict[str, Any]):
    st.subheader("Errors / Debug")

    st.markdown("### errors（抓不到/缺資料/制度疑似日）")
    if errors_df is None or errors_df.empty:
        st.info("errors 為空")
    else:
        st.dataframe(errors_df, use_container_width=True, height=520)

    st.markdown("### payload 原始資訊")
    st.json({k: payload.get(k) for k in ["market", "ymd", "slot", "asof", "generated_at", "rules", "filters", "stats"]})
