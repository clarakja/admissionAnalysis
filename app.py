"""
🎓 입시 홍보 분석 대시보드 — 단일 파일 Streamlit 앱

최근 3개년 지원자 데이터를 분석해 입시 홍보 전략을 돕는 웹 대시보드.

서사 구조:
    🏠 홈 → 📊 현황 진단 → 🎓 학과 진단 → 🏫 피더 스쿨
         → 🎯 전략 발굴 ⭐ → 🔽 Funnel → 🔍 심층 → 📝 공유

실행:
    # 로컬
    pip install streamlit plotly pandas openpyxl xlrd xlsxwriter
    streamlit run app.py

    # Streamlit Cloud 배포
    GitHub 에 app.py + requirements.txt 푸시 → share.streamlit.io 연결
"""
from __future__ import annotations

import io
import os
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go


# ═══════════════════════════════════════════════════════════════════════════
#  페이지 설정
# ═══════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="입시 홍보 분석",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .main .block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1400px; }
  h1, h2, h3 { letter-spacing: -0.02em; }
  .stDataFrame { font-size: 0.9rem; }
  .kpi-card {
      background: #ffffff; padding: 1rem 1.25rem; border-radius: 8px;
      border-left: 4px solid var(--primary-color, #0066cc);
      box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  .kpi-title { font-size: 0.85rem; color: #666; margin-bottom: 0.3rem; }
  .kpi-value { font-size: 1.8rem; font-weight: 700; line-height: 1; }
  .kpi-delta-up { color: #2e7d32; font-size: 0.9rem; }
  .kpi-delta-down { color: #c62828; font-size: 0.9rem; }
  div[data-testid="stSidebarNav"] { display: none; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
#  [1/3]  분석 엔진 import + 캐시 래핑
# ═══════════════════════════════════════════════════════════════════════════
#
#  분석 로직은 analysis_engine.py 에 있습니다.
#  여기서는 Streamlit 캐시를 적용한 wrapper 함수들을 만듭니다.
#
import hashlib
import analysis_engine as ae

# 엔진 모듈 소스의 해시 — 엔진이 바뀌면 캐시가 자동 무효화됨
try:
    with open(ae.__file__, "rb") as _f:
        ENGINE_VERSION = hashlib.md5(_f.read()).hexdigest()[:8]
except Exception:
    ENGINE_VERSION = "dev"

# 데이터 로드 — 파일 바이트 기준으로 캐시 (같은 파일은 한 번만 파싱)
@st.cache_data(show_spinner="📊 데이터 로드·정제 중...")
def load_data(file_bytes: bytes, filename: str, _v: str = ENGINE_VERSION):
    return ae.load_and_clean(file_bytes)


# 분석 함수들 — 모두 @st.cache_data 로 래핑
#   _v 파라미터로 엔진 버전을 캐시 키에 섞음 (엔진 수정 시 자동 무효화)
@st.cache_data
def a_overview_by_year(df, _v=ENGINE_VERSION): return ae.a_overview_by_year(df)

@st.cache_data
def a_overview_year_admtype(df, _v=ENGINE_VERSION): return ae.a_overview_year_admtype(df)

@st.cache_data
def a_region_by_year(df, _v=ENGINE_VERSION): return ae.a_region_by_year(df)

@st.cache_data
def a_school_type_by_year(df, _v=ENGINE_VERSION): return ae.a_school_type_by_year(df)

@st.cache_data
def a_school_establishment_by_year(df, _v=ENGINE_VERSION): return ae.a_school_establishment_by_year(df)

@st.cache_data
def a_admission_type_by_year(df, _v=ENGINE_VERSION): return ae.a_admission_type_by_year(df)

@st.cache_data
def a_unit_3year_total(df, _v=ENGINE_VERSION): return ae.a_unit_3year_total(df)

@st.cache_data
def a_unit_by_year(df, _v=ENGINE_VERSION): return ae.a_unit_by_year(df)

@st.cache_data
def a_unit_low_pass_rate(df, top_n, min_apply, _v=ENGINE_VERSION):
    return ae.a_unit_low_pass_rate(df, top_n, min_apply)

@st.cache_data
def a_unit_high_fill(df, top_n, min_pass, _v=ENGINE_VERSION):
    return ae.a_unit_high_fill(df, top_n, min_pass)

@st.cache_data
def a_unit_low_fill(df, top_n, min_pass, _v=ENGINE_VERSION):
    return ae.a_unit_low_fill(df, top_n, min_pass)

@st.cache_data
def a_feeder_apply_top(df, top_n, _v=ENGINE_VERSION): return ae.a_feeder_apply_top(df, top_n)

@st.cache_data
def a_feeder_registered_top(df, top_n, _v=ENGINE_VERSION): return ae.a_feeder_registered_top(df, top_n)

@st.cache_data
def a_school_3yr_increase(df, top_n, _v=ENGINE_VERSION): return ae.a_school_3yr_increase(df, top_n)

@st.cache_data
def a_school_3yr_decrease(df, top_n, _v=ENGINE_VERSION): return ae.a_school_3yr_decrease(df, top_n)

@st.cache_data
def a_school_surge(df, threshold, top_n, _v=ENGINE_VERSION): return ae.a_school_surge(df, threshold, top_n)

@st.cache_data
def a_school_drop(df, threshold, top_n, _v=ENGINE_VERSION): return ae.a_school_drop(df, threshold, top_n)

@st.cache_data
def a_gap_high_apply_low_reg(df, top_n, min_apply, _v=ENGINE_VERSION):
    return ae.a_gap_high_apply_low_reg(df, top_n, min_apply)

@st.cache_data
def a_gap_low_apply_high_reg(df, top_n, min_reg, _v=ENGINE_VERSION):
    return ae.a_gap_low_apply_high_reg(df, top_n, min_reg)

@st.cache_data
def a_conversion_high(df, top_n, min_pass, _v=ENGINE_VERSION):
    return ae.a_conversion_high(df, top_n, min_pass)

@st.cache_data
def a_conversion_low(df, top_n, min_pass, _v=ENGINE_VERSION):
    return ae.a_conversion_low(df, top_n, min_pass)

@st.cache_data
def a_school_matrix(df, min_apply, _v=ENGINE_VERSION):
    return ae.a_school_matrix(df, min_apply)

@st.cache_data
def a_size_apply_ratio(df, top_n, min_size, _v=ENGINE_VERSION):
    return ae.a_size_apply_ratio(df, top_n, min_size)

@st.cache_data
def a_size_reg_ratio(df, top_n, min_size, _v=ENGINE_VERSION):
    return ae.a_size_reg_ratio(df, top_n, min_size)

@st.cache_data
def a_funnel_overall(df, _v=ENGINE_VERSION): return ae.a_funnel_overall(df)

@st.cache_data
def a_funnel_by_year(df, _v=ENGINE_VERSION): return ae.a_funnel_by_year(df)

@st.cache_data
def a_funnel_by_region(df, _v=ENGINE_VERSION): return ae.a_funnel_by_region(df)

@st.cache_data
def a_funnel_by_admtype(df, top_n, _v=ENGINE_VERSION): return ae.a_funnel_by_admtype(df, top_n)

@st.cache_data
def a_fill_round(df, _v=ENGINE_VERSION): return ae.a_fill_round(df)

@st.cache_data
def a_deep_unit(df, unit_name, _v=ENGINE_VERSION): return ae.a_deep_unit(df, unit_name)

@st.cache_data
def a_deep_school(df, school_name, _v=ENGINE_VERSION): return ae.a_deep_school(df, school_name)

@st.cache_data
def a_deep_region(df, region, _v=ENGINE_VERSION): return ae.a_deep_region(df, region)

@st.cache_data
def a_insight_report(df, _v=ENGINE_VERSION): return ae.a_insight_report(df)

# 홈 전용 헬퍼는 캐시 불필요 (단순 계산)
def home_kpis(df): return ae.home_kpis(df)
def home_insights(df, k=3): return ae.home_insights(df, k)


# ═══════════════════════════════════════════════════════════════════════════
#  [2/3]  공용 UI 유틸
# ═══════════════════════════════════════════════════════════════════════════

def kpi_card_html(title: str, value, delta=None, unit=""):
    """HTML KPI 카드. st.markdown(unsafe_allow_html=True) 로 렌더링."""
    if isinstance(value, (int, float)):
        val_str = f"{int(value):,}" if abs(value) >= 100 else f"{value:.1f}"
    else:
        val_str = str(value)
    val_str += unit

    delta_html = ""
    if delta is not None:
        cls = "kpi-delta-up" if delta >= 0 else "kpi-delta-down"
        arrow = "▲" if delta >= 0 else "▼"
        sign = "+" if delta >= 0 else ""
        delta_html = f'<span class="{cls}"> {arrow} {sign}{delta:.1f}%</span>'

    return f"""
<div class="kpi-card">
  <div class="kpi-title">{title}</div>
  <div class="kpi-value">{val_str}{delta_html}</div>
</div>"""


def save_to_store(key: str, df: pd.DataFrame, description: str = ""):
    """분석 결과를 세션 저장소에 담기"""
    st.session_state.results[key] = {
        "df": df,
        "description": description,
        "saved_at": dt.datetime.now().strftime("%H:%M:%S"),
    }


def show_result(df: pd.DataFrame, height: int = 420):
    """DataFrame 을 Streamlit dataframe 으로 표시"""
    st.dataframe(df, use_container_width=True, height=height)


# ═══════════════════════════════════════════════════════════════════════════
#  [3/3]  세션 상태 + 사이드바
# ═══════════════════════════════════════════════════════════════════════════

if "df" not in st.session_state:
    st.session_state.df = None
    st.session_state.filename = None
if "results" not in st.session_state:
    st.session_state.results = {}

with st.sidebar:
    st.markdown("## 🎓 입시 홍보 분석")
    st.caption("최근 3개년 지원자 데이터 기반 홍보 전략 도구")
    st.markdown("---")

    # 데이터 업로드
    uploaded = st.file_uploader(
        "📤 엑셀 업로드 (.xls / .xlsx)",
        type=["xls", "xlsx"],
        help="시트명 '3개년데이터' 가 포함된 파일")

    if uploaded is not None:
        try:
            st.session_state.df = load_data(
                uploaded.getvalue(), uploaded.name)
            st.session_state.filename = uploaded.name
            st.success(f"✅ {len(st.session_state.df):,}건 로드됨")
        except Exception as e:
            st.error(f"로드 실패: {e}")

    if st.session_state.df is None:
        st.info("⬆ 먼저 파일을 업로드해 주세요")
        st.markdown("""
        ### 📋 서사 구조
        - 🏠 홈 — 전체 현황 요약
        - 📊 **진단** — 우리는 지금 어떤가?
        - 🎓 **학과** — 학과별 차이
        - 🏫 **피더** — 어디서 오는가
        - 🎯 **전략** ⭐ — 어디에 집중?
        - 🔽 **Funnel** — 단계별 이탈
        - 🔍 **심층** — 특정 대상 자세히
        - 📝 **공유** — 리포트·엑셀

        **실행**:  `streamlit run app.py`
        """)
        st.stop()

    df = st.session_state.df
    st.caption(f"📁 {st.session_state.filename} · "
               f"{df['입시년도'].nunique()}개년 · "
               f"{df['모집단위명'].nunique()}개 학과 · "
               f"{df['고등학교명'].nunique():,}개 고교")

    st.markdown("---")
    st.markdown("### 📋 분석 메뉴")
    group = st.radio(
        "그룹 선택",
        ["🏠 홈",
         "📊 현황 진단",
         "🎓 학과 진단",
         "🏫 피더 스쿨",
         "🎯 전략 발굴 ⭐",
         "🔽 Funnel",
         "🔍 심층 분석",
         "📝 공유·리포트"],
        label_visibility="collapsed",
    )

    st.markdown("---")

    # 저장소 상태
    n = len(st.session_state.results)
    st.markdown(f"### 💾 저장소 ({n}개)")
    if n > 0:
        with st.expander("저장된 분석 목록"):
            for k, v in st.session_state.results.items():
                st.caption(f"• {k} ({v['saved_at']})")

        if st.button("🗑 저장소 비우기", use_container_width=True):
            st.session_state.results.clear()
            st.rerun()

        # 엑셀 일괄 다운로드
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            meta = pd.DataFrame([
                {"키": k, "설명": v["description"],
                 "행": len(v["df"]), "열": v["df"].shape[1],
                 "저장시각": v["saved_at"]}
                for k, v in st.session_state.results.items()
            ])
            meta.to_excel(writer, sheet_name="_META", index=False)
            for key, item in st.session_state.results.items():
                sheet = key[:31]
                for bad in '[]:*?/\\':
                    sheet = sheet.replace(bad, "_")
                item["df"].to_excel(writer, sheet_name=sheet, index=True)
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M")
        st.download_button(
            f"📥 엑셀 {n}개 일괄 다운로드",
            data=buf.getvalue(),
            file_name=f"입시분석_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
        )
    else:
        st.caption("각 분석 화면의 💾 버튼으로 담아주세요")


# ═══════════════════════════════════════════════════════════════════════════
#  페이지 라우팅
# ═══════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────
#  🏠 홈
# ─────────────────────────────────────────────────────────────
if group.startswith("🏠"):
    st.markdown("## 🏠 홈 대시보드")
    st.caption(f"{st.session_state.filename} · "
               f"연도 {sorted(df['입시년도'].unique().tolist())} · "
               f"총 {len(df):,}건")

    k = home_kpis(df)
    insights = home_insights(df, 3)

    # KPI 카드 6개
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(kpi_card_html(
            f"{k['last_year']}학년도 지원자", k['apply_last'],
            k.get('apply_delta'), "명"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card_html(
            f"{k['last_year']}학년도 발표합격", k['pass_last'],
            k.get('pass_delta'), "명"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card_html(
            f"{k['last_year']}학년도 최종등록", k['reg_last'],
            k.get('reg_delta'), "명"), unsafe_allow_html=True)

    st.markdown("")
    c4, c5, c6 = st.columns(3)
    with c4:
        st.markdown(kpi_card_html(
            "합격률", k['pass_rate_last'], None, "%"),
            unsafe_allow_html=True)
    with c5:
        st.markdown(kpi_card_html(
            "등록률 (합격→등록)", k['reg_rate_last'], None, "%"),
            unsafe_allow_html=True)
    with c6:
        st.markdown(kpi_card_html(
            "지원대비 등록률",
            round(k['reg_last'] / k['apply_last'] * 100, 1)
            if k['apply_last'] else 0, None, "%"),
            unsafe_allow_html=True)

    st.markdown("")
    st.markdown("### ✨ 오늘의 인사이트")
    for i in insights:
        st.markdown(f"- {i}")

    st.markdown("### 📈 전체 추이")
    ov = a_overview_by_year(df).reset_index()
    col1, col2 = st.columns([7, 5])
    with col1:
        fig = px.line(
            ov.melt(id_vars="입시년도",
                    value_vars=["지원", "발표합격", "최종등록"],
                    var_name="구분", value_name="인원"),
            x="입시년도", y="인원", color="구분", markers=True,
            title="연도별 지원·합격·등록 추이", height=350)
        fig.update_layout(legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fn = a_funnel_overall(df)
        fig = go.Figure(go.Funnel(
            y=fn["단계"], x=fn["인원수"],
            textposition="inside",
            textinfo="value+percent initial",
            marker={"color": ["#4C78A8", "#F58518", "#54A24B"]},
        ))
        fig.update_layout(title="전체 Funnel", height=350)
        st.plotly_chart(fig, use_container_width=True)

    st.info("💡 **분석 흐름**: 좌측 메뉴에서 진단 → 학과 → 피더 → "
            "**전략 발굴** 순서로 따라가면 홍보 계획 수립에 도움이 됩니다.")


# ─────────────────────────────────────────────────────────────
#  📊 현황 진단
# ─────────────────────────────────────────────────────────────
elif group.startswith("📊"):
    st.markdown("## 📊 현황 진단")
    st.caption("우리 대학은 지금 어떤 상태인가?")

    sub = st.radio(
        "세부 분석",
        ["1-1. 연도별 전체 추이",
         "1-2. 모집구분(수시/정시) × 연도",
         "1-3. 지역별 × 연도별",
         "1-4. 고교유형별 × 연도별",
         "1-5. 설립구분별 × 연도별",
         "1-6. 전형구분 연도 추이"],
        horizontal=True, label_visibility="collapsed")

    if sub.startswith("1-1"):
        r = a_overview_by_year(df)
        chart = st.selectbox("차트 유형",
            ["막대 (지원·합격·등록)", "선 (추이)", "막대 (비율)"])
        rr = r.reset_index()

        if chart.startswith("막대 (비율"):
            m = rr.melt(id_vars="입시년도",
                        value_vars=["합격률(%)", "등록률(%)", "지원대비등록(%)"],
                        var_name="지표", value_name="값")
            fig = px.bar(m, x="입시년도", y="값", color="지표",
                         barmode="group", text="값",
                         title="연도별 비율 지표 (%)", height=420)
        elif chart.startswith("선"):
            m = rr.melt(id_vars="입시년도",
                        value_vars=["지원", "발표합격", "최종등록"],
                        var_name="구분", value_name="인원")
            fig = px.line(m, x="입시년도", y="인원", color="구분",
                          markers=True, title="연도별 추이", height=420)
        else:
            m = rr.melt(id_vars="입시년도",
                        value_vars=["지원", "발표합격", "최종등록"],
                        var_name="구분", value_name="인원")
            fig = px.bar(m, x="입시년도", y="인원", color="구분",
                         barmode="group", text="인원",
                         title="연도별 지원·합격·등록", height=420)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s_1_1"):
            save_to_store("1-1_연도별전체", r, "연도별 전체 지원·합격·등록")
            st.success(f"저장됨 (저장소 {len(st.session_state.results)}개)")

    elif sub.startswith("1-2"):
        r = a_overview_year_admtype(df)
        chart = st.selectbox("차트 유형", ["묶음 막대", "누적 막대", "히트맵"])
        rr = r.reset_index()
        if chart == "히트맵":
            pv = rr.pivot(index="입시년도", columns="모집구분", values="지원")
            fig = px.imshow(pv.values, x=pv.columns, y=pv.index,
                            text_auto=True, aspect="auto",
                            color_continuous_scale="Blues",
                            title="연도 × 모집구분 (히트맵)", height=400)
        else:
            barmode = "stack" if chart.startswith("누적") else "group"
            fig = px.bar(rr, x="입시년도", y="지원", color="모집구분",
                         barmode=barmode, text="지원",
                         title="연도별 모집구분 지원자", height=420)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s_1_2"):
            save_to_store("1-2_모집구분×연도", r, "연도별 모집구분 지원/등록")
            st.success(f"저장됨 (저장소 {len(st.session_state.results)}개)")

    elif sub.startswith("1-3"):
        top_n = st.slider("표시 지역 수", 3, 17, 10)
        r = a_region_by_year(df)
        totals = r.groupby(level=0)["지원"].sum().sort_values(
            ascending=False).head(top_n)
        r_top = r.loc[totals.index].reset_index()
        fig = px.bar(r_top, x="고교소재지", y="지원", color="입시년도",
                     barmode="group", text="지원",
                     title=f"시도별 × 연도별 지원자 (상위 {top_n})", height=460)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s_1_3"):
            save_to_store("1-3_지역×연도", r, "지역별 × 연도별 지원/합격/등록")
            st.success(f"저장됨 (저장소 {len(st.session_state.results)}개)")

    elif sub.startswith("1-4"):
        r = a_school_type_by_year(df)
        rr = r.reset_index()
        fig = px.bar(rr, x="입시년도", y="지원", color="고교특성",
                     barmode="group", text="지원",
                     title="고교특성 × 연도별 (일반/자율/특목/특성화)",
                     height=420)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s_1_4"):
            save_to_store("1-4_고교유형×연도", r, "고교특성별 × 연도별")
            st.success(f"저장됨 (저장소 {len(st.session_state.results)}개)")

    elif sub.startswith("1-5"):
        r = a_school_establishment_by_year(df)
        rr = r.reset_index()
        fig = px.bar(rr, x="입시년도", y="지원", color="설립구분",
                     barmode="group", text="지원",
                     title="설립구분 × 연도별 (공립/사립/국립)", height=420)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s_1_5"):
            save_to_store("1-5_설립×연도", r, "설립구분별 × 연도별")
            st.success(f"저장됨 (저장소 {len(st.session_state.results)}개)")

    else:   # 1-6
        top_n = st.slider("표시 전형 수", 3, 20, 10)
        r = a_admission_type_by_year(df)
        totals = r.groupby(level=0)["지원"].sum().sort_values(
            ascending=False).head(top_n)
        r_top = r.loc[totals.index].reset_index()
        fig = px.line(r_top, x="입시년도", y="지원", color="전형구분",
                      markers=True,
                      title=f"전형구분별 연도 추이 (상위 {top_n})", height=480)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s_1_6"):
            save_to_store("1-6_전형×연도", r, "전형구분 × 연도 추이")
            st.success(f"저장됨 (저장소 {len(st.session_state.results)}개)")


# ─────────────────────────────────────────────────────────────
#  🎓 학과 진단
# ─────────────────────────────────────────────────────────────
elif group.startswith("🎓"):
    st.markdown("## 🎓 학과 진단")
    st.caption("학과별로 어떻게 다른가? 어느 학과가 잘 나가고, 어디가 어려운가?")

    sub = st.radio("세부 분석",
        ["2-1. 3년 누적 요약",
         "2-2. 연도별 추이",
         "2-3. 경쟁 치열 학과",
         "2-4. 추가합격 많은 학과 (이탈)",
         "2-5. 추가합격 적은 학과 (안정)",
         "2-6. 학과 심층 분석"],
        horizontal=True, label_visibility="collapsed")

    if sub.startswith("2-1"):
        c1, c2 = st.columns([1, 2])
        with c1:
            top_n = st.slider("표시 학과 수", 5, 36, 20, key="s2_1_n")
        with c2:
            chart = st.selectbox("차트 유형",
                ["가로 막대 (지원·합격·등록)", "가로 막대 (등록률)",
                 "산점도 (지원 vs 등록)"], key="s2_1_c")

        r = a_unit_3year_total(df).head(top_n)
        rr = r.reset_index()

        if chart.startswith("가로 막대 (등록률)"):
            fig = px.bar(rr, x="등록률(%)", y="모집단위명", orientation="h",
                         title=f"학과별 합격→등록 전환율 (Top {top_n})",
                         height=max(400, top_n * 25))
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        elif chart.startswith("산점도"):
            fig = px.scatter(rr, x="지원", y="최종등록",
                             size="발표합격", hover_name="모집단위명",
                             color="등록률(%)", color_continuous_scale="RdYlGn",
                             title="학과 분포 (크기=합격, 색=등록률)",
                             height=520)
        else:
            m = rr.melt(id_vars="모집단위명",
                        value_vars=["지원", "발표합격", "최종등록"],
                        var_name="구분", value_name="인원")
            fig = px.bar(m, x="인원", y="모집단위명", color="구분",
                         orientation="h", barmode="group",
                         title=f"학과별 지원·합격·등록 (Top {top_n})",
                         height=max(400, top_n * 25))
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s2_1_save"):
            save_to_store(f"2-1_학과3년누적_Top{top_n}", r,
                          f"학과별 3년 합계 Top {top_n}")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("2-2"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("표시 학과 수", 5, 30, 15, key="s2_2_n")
        with c2:
            metric = st.selectbox("추적 지표",
                ["지원자 수", "합격률(%)", "등록률(%)", "지원대비등록(%)"],
                key="s2_2_m")

        r = a_unit_by_year(df)
        top_units = (df.groupby("모집단위명").size()
                       .sort_values(ascending=False).head(top_n).index)
        r_top = r.loc[top_units]
        col = "지원" if metric == "지원자 수" else metric
        pv = r_top[col].unstack("입시년도")
        m = pv.reset_index().melt(id_vars="모집단위명",
                                   var_name="입시년도", value_name=col)
        fig = px.line(m, x="입시년도", y=col, color="모집단위명",
                      markers=True, title=f"{metric} — 주요 {top_n}개 학과",
                      height=520)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s2_2_save"):
            save_to_store("2-2_학과연도별", r, "학과 × 연도별 상세")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("2-3"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 5, 30, 15, key="s2_3_n")
        with c2:
            min_apply = st.slider("최소 지원자 수", 20, 300, 50, step=10,
                                   key="s2_3_m")
        r = a_unit_low_pass_rate(df, top_n, min_apply)
        rr = r.reset_index()
        fig = px.bar(rr, x="합격률(%)", y="모집단위명", orientation="h",
                     color="지원", color_continuous_scale="Reds_r",
                     title=f"합격률 낮은 학과 (지원 {min_apply}명↑, Top {top_n})",
                     height=max(400, top_n * 30))
        fig.update_layout(yaxis={'categoryorder': 'total descending'})
        st.plotly_chart(fig, use_container_width=True)
        st.info("💡 **해석**: 합격률이 낮을수록 경쟁이 치열한 학과입니다. "
                "입시 커트라인이 높고 성적 우수자가 지원합니다.")
        show_result(r)
        if st.button("💾 저장소에 담기", key="s2_3_save"):
            save_to_store(f"2-3_경쟁치열_Top{top_n}", r,
                          f"합격률 낮은 학과 (지원 {min_apply}↑)")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("2-4"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 5, 30, 20, key="s2_4_n")
        with c2:
            min_pass = st.slider("최소 합격자 수", 20, 300, 50, step=10,
                                  key="s2_4_m")
        r = a_unit_high_fill(df, top_n, min_pass)
        rr = r.reset_index()
        fig = px.bar(rr, x="추가합격률(%)", y="모집단위명", orientation="h",
                     color="총합격", color_continuous_scale="Oranges",
                     title=f"추가합격 많은 학과 (합격 {min_pass}↑, Top {top_n})",
                     height=max(400, top_n * 30))
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        st.info("💡 **해석**: 추가합격률 = **전체 합격자 중 추가(충원)합격자가 차지하는 비율** "
                "(대학알리미의 '신입생 충원율'과 다른 지표).  \n"
                "추가합격이 **많다** = 최초합격자 상당수가 다른 대학으로 "
                "이탈해 2차·3차 추가합격까지 간다는 뜻입니다.")
        show_result(r)
        if st.button("💾 저장소에 담기", key="s2_4_save"):
            save_to_store(f"2-4_추가합격많음_Top{top_n}", r,
                          f"추가합격 많은 학과 (합격 {min_pass}↑)")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("2-5"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 5, 30, 20, key="s2_5_n")
        with c2:
            min_pass = st.slider("최소 합격자 수", 20, 300, 50, step=10,
                                  key="s2_5_m")
        r = a_unit_low_fill(df, top_n, min_pass)
        rr = r.reset_index()
        fig = px.bar(rr, x="추가합격률(%)", y="모집단위명", orientation="h",
                     color="총합격", color_continuous_scale="Greens_r",
                     title=f"추가합격 적은 학과 (합격 {min_pass}↑, Top {top_n})",
                     height=max(400, top_n * 30))
        fig.update_layout(yaxis={'categoryorder': 'total descending'})
        st.plotly_chart(fig, use_container_width=True)
        st.info("💡 **해석**: 추가합격률 = **전체 합격자 중 추가(충원)합격자가 차지하는 비율** "
                "(대학알리미의 '신입생 충원율'과 다른 지표).  \n"
                "추가합격이 **적다** = 최초합격자 대부분이 등록하는 "
                "안정적으로 마감되는 학과입니다.")
        show_result(r)
        if st.button("💾 저장소에 담기", key="s2_5_save"):
            save_to_store(f"2-5_추가합격적음_Top{top_n}", r,
                          f"추가합격 적은 학과 (합격 {min_pass}↑)")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    else:   # 2-6
        units = sorted(df["모집단위명"].dropna().unique())
        unit = st.selectbox("모집단위 선택", units)
        res = a_deep_unit(df, unit)
        if res is None:
            st.warning("데이터 없음")
        else:
            # 연도별 차트
            ry = res["by_year"].reset_index().melt(
                id_vars="입시년도",
                value_vars=["지원", "발표합격", "최종등록"],
                var_name="구분", value_name="인원")
            fig = px.bar(ry, x="입시년도", y="인원", color="구분",
                         barmode="group", text="인원",
                         title=f"{unit} — 연도별", height=400)
            st.plotly_chart(fig, use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown("#### 연도별 상세")
                show_result(res["by_year"], height=200)
                st.markdown("#### 전형구분별 분포")
                show_result(res["by_admtype"], height=250)
            with c2:
                st.markdown("#### 지역별 분포")
                show_result(res["by_region"], height=250)

            st.markdown(f"#### 주요 지원 고교 Top 20")
            fig2 = px.bar(
                res["by_school"].reset_index().rename(
                    columns={"index": "고등학교명"}),
                x="지원수", y="고등학교명", orientation="h",
                title="", height=500)
            fig2.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig2, use_container_width=True)
            show_result(res["by_school"], height=300)

            if st.button("💾 이 학과 저장", key="s2_6_save"):
                save_to_store(f"2-6_{unit}_연도별", res["by_year"],
                              f"{unit} 심층 - 연도별")
                save_to_store(f"2-6_{unit}_고교", res["by_school"],
                              f"{unit} 심층 - 주요 고교")
                st.success(f"저장됨 ({len(st.session_state.results)}개)")


# ─────────────────────────────────────────────────────────────
#  🏫 피더 스쿨
# ─────────────────────────────────────────────────────────────
elif group.startswith("🏫"):
    st.markdown("## 🏫 피더 스쿨")
    st.caption("어디에서 지원자가 오는가? — 관계 관리의 출발점")

    sub = st.radio("세부 분석",
        ["3-1. 지원자 Top",
         "3-2. 등록자 Top",
         "3-3. 연속 증가",
         "3-4. 연속 감소",
         "3-5. 전년대비 급증",
         "3-6. 전년대비 급감",
         "3-7. 고교 심층 (검색)"],
        horizontal=True, label_visibility="collapsed")

    # 지역 공통 필터 — 이 탭의 모든 분석에 적용
    with st.container(border=True):
        col_rgn, col_info = st.columns([2, 3])
        with col_rgn:
            region_filter = st.selectbox(
                "🗺 지역 필터",
                ["전체", "강원", "서울", "경기", "인천", "기타지방"],
                index=0,
                help="분석 대상을 특정 지역 고교로 한정합니다. "
                     "'기타지방' = 강원·서울·경기·인천을 제외한 전국")
        # 필터 적용된 df 생성
        if region_filter == "전체":
            df_filt = df
        elif region_filter == "기타지방":
            df_filt = df[~df["고교소재지"].isin(["강원", "서울", "경기", "인천"])
                         & df["고교소재지"].notna()]
        else:
            df_filt = df[df["고교소재지"] == region_filter]

        # 대상 고교 수 안내
        n_schools = df_filt[~df_filt["검정고시여부"]
                            & df_filt["고등학교명"].notna()]["고등학교명"].nunique()
        n_records = len(df_filt)
        with col_info:
            st.caption(f"**대상 고교 {n_schools:,}개** · 레코드 {n_records:,}건  "
                       f"— 아래 모든 분석에 공통 적용됩니다")

    if sub.startswith("3-1"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 10, 300, 20, step=10, key="s3_1_n")
        with c2:
            chart = st.selectbox("차트 유형",
                ["가로 막대 (지원·합격·등록)", "산점도 (지원 vs 등록률)"],
                key="s3_1_c")

        r = a_feeder_apply_top(df_filt, top_n)
        rr = r.reset_index()
        rgn_suffix = f" · {region_filter}" if region_filter != "전체" else ""
        if chart.startswith("산점도"):
            fig = px.scatter(rr, x="총지원", y="지원대비등록(%)",
                             size="총등록", hover_name="고등학교명",
                             color="고교소재지",
                             title=f"지원 규모 vs 등록률 (Top {top_n}{rgn_suffix})",
                             height=520)
        else:
            m = rr.melt(id_vars="고등학교명",
                        value_vars=["총지원", "총합격", "총등록"],
                        var_name="구분", value_name="인원")
            fig = px.bar(m, x="인원", y="고등학교명", color="구분",
                         orientation="h", barmode="group",
                         title=f"3년 누적 지원자 Top {top_n}{rgn_suffix}",
                         height=max(400, min(top_n * 25, 12000)))
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        show_result(r, height=min(600, max(300, top_n * 35)))
        if st.button("💾 저장소에 담기", key="s3_1_save"):
            save_to_store(f"3-1_지원Top{top_n}{rgn_suffix}", r,
                          f"지원자 Top {top_n}{rgn_suffix}")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("3-2"):
        top_n = st.slider("상위 N", 10, 300, 20, step=10, key="s3_2_n")
        rgn_suffix = f" · {region_filter}" if region_filter != "전체" else ""
        r = a_feeder_registered_top(df_filt, top_n)
        rr = r.reset_index().melt(
            id_vars="고등학교명",
            value_vars=["총지원", "총합격", "총등록"],
            var_name="구분", value_name="인원")
        fig = px.bar(rr, x="인원", y="고등학교명", color="구분",
                     orientation="h", barmode="group",
                     title=f"3년 누적 등록자 Top {top_n}{rgn_suffix}",
                     height=max(400, min(top_n * 25, 12000)))
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        show_result(r, height=min(600, max(300, top_n * 35)))
        if st.button("💾 저장소에 담기", key="s3_2_save"):
            save_to_store(f"3-2_등록Top{top_n}{rgn_suffix}", r,
                          f"등록자 Top {top_n}{rgn_suffix}")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("3-3"):
        top_n = st.slider("표시 고교 수 (0=전체)", 0, 300, 20, step=10,
                          key="s3_3_n")
        rgn_suffix = f" · {region_filter}" if region_filter != "전체" else ""
        r = a_school_3yr_increase(df_filt, top_n if top_n > 0 else None)
        if len(r) == 0:
            st.info("3년 연속 증가 고교가 없습니다.")
        else:
            years = sorted([c for c in r.columns
                            if isinstance(c, (int, np.integer))])
            top_plot = r.head(12)[years].T
            top_plot.index.name = "입시년도"
            rr = top_plot.reset_index().melt(
                id_vars="입시년도",
                var_name="고등학교명", value_name="지원자")
            fig = px.line(rr, x="입시년도", y="지원자", color="고등학교명",
                          markers=True,
                          title=f"3년 연속 증가 Top 12 — 연도별 추이{rgn_suffix}",
                          height=500)
            st.plotly_chart(fig, use_container_width=True)
            st.info("💡 **해석**: 우리 대학과의 관계가 깊어지는 학교들. "
                    "관리 강화 대상.")
            show_result(r, height=min(600, max(300, len(r) * 35)))
            if st.button("💾 저장소에 담기", key="s3_3_save"):
                save_to_store(f"3-3_연속증가_{top_n or '전체'}{rgn_suffix}", r,
                              f"3년 연속 증가{rgn_suffix}")
                st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("3-4"):
        top_n = st.slider("표시 고교 수 (0=전체)", 0, 300, 20, step=10,
                          key="s3_4_n")
        rgn_suffix = f" · {region_filter}" if region_filter != "전체" else ""
        r = a_school_3yr_decrease(df_filt, top_n if top_n > 0 else None)
        if len(r) == 0:
            st.info("3년 연속 감소 고교가 없습니다.")
        else:
            years = sorted([c for c in r.columns
                            if isinstance(c, (int, np.integer))])
            top_plot = r.head(12)[years].T
            top_plot.index.name = "입시년도"
            rr = top_plot.reset_index().melt(
                id_vars="입시년도",
                var_name="고등학교명", value_name="지원자")
            fig = px.line(rr, x="입시년도", y="지원자", color="고등학교명",
                          markers=True,
                          title=f"3년 연속 감소 Top 12 — 연도별 추이{rgn_suffix}",
                          height=500)
            st.plotly_chart(fig, use_container_width=True)
            st.info("💡 **해석**: 이탈 조짐이 있는 학교들. 방문·조사 필요.")
            show_result(r, height=min(600, max(300, len(r) * 35)))
            if st.button("💾 저장소에 담기", key="s3_4_save"):
                save_to_store(f"3-4_연속감소_{top_n or '전체'}{rgn_suffix}", r,
                              f"3년 연속 감소{rgn_suffix}")
                st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("3-5"):
        c1, c2 = st.columns(2)
        with c1:
            th = st.slider("증가 기준 (명 이상)", 5, 50, 15, step=5,
                           key="s3_5_t")
        with c2:
            top_n = st.slider("상위 N (0=전체)", 0, 300, 20, step=10,
                              key="s3_5_n")
        rgn_suffix = f" · {region_filter}" if region_filter != "전체" else ""
        r = a_school_surge(df_filt, th, top_n if top_n > 0 else None)
        if len(r) == 0:
            st.info("조건 불충족")
        else:
            # 차트는 최대 30개까지만 표시 (너무 많으면 가독성↓)
            chart_n = min(len(r), 30)
            rr = r.reset_index().head(chart_n)
            fig = px.bar(rr, x="증감", y="고등학교명", orientation="h",
                         color="증감률(%)", color_continuous_scale="Greens",
                         hover_data=["고교소재지", "증감률(%)"],
                         title=f"전년대비 {th}명↑ 급증 Top {chart_n}{rgn_suffix}",
                         height=max(500, chart_n * 25))
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
            show_result(r, height=min(600, max(300, len(r) * 35)))
            if st.button("💾 저장소에 담기", key="s3_5_save"):
                save_to_store(f"3-5_급증_{th}명↑{rgn_suffix}", r,
                              f"전년대비 {th}명 이상 급증{rgn_suffix}")
                st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("3-6"):
        c1, c2 = st.columns(2)
        with c1:
            th = st.slider("감소 기준 (명 이상)", 5, 50, 15, step=5,
                           key="s3_6_t")
        with c2:
            top_n = st.slider("상위 N (0=전체)", 0, 300, 20, step=10,
                              key="s3_6_n")
        rgn_suffix = f" · {region_filter}" if region_filter != "전체" else ""
        r = a_school_drop(df_filt, th, top_n if top_n > 0 else None)
        if len(r) == 0:
            st.info("조건 불충족")
        else:
            chart_n = min(len(r), 30)
            rr = r.reset_index().head(chart_n)
            fig = px.bar(rr, x="증감", y="고등학교명", orientation="h",
                         color="증감률(%)", color_continuous_scale="Reds_r",
                         hover_data=["고교소재지", "증감률(%)"],
                         title=f"전년대비 {th}명↓ 급감 Top {chart_n}{rgn_suffix}",
                         height=max(500, chart_n * 25))
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
            show_result(r, height=min(600, max(300, len(r) * 35)))
            if st.button("💾 저장소에 담기", key="s3_6_save"):
                save_to_store(f"3-6_급감_{th}명↓{rgn_suffix}", r,
                              f"전년대비 {th}명 이상 급감{rgn_suffix}")
                st.success(f"저장됨 ({len(st.session_state.results)}개)")

    else:   # 3-7
        keyword = st.text_input("고교 이름 검색 (일부만 입력 가능)",
                                 placeholder="예: 강릉, 광문, 판곡")
        # 검색 후보는 지역 필터 반영
        all_schools = sorted([s for s in df_filt["고등학교명"].dropna().unique()
                              if "검정고시" not in str(s)])
        if keyword:
            matched = [s for s in all_schools if keyword in str(s)]
        else:
            matched = all_schools

        if len(matched) == 0:
            st.warning(f"일치하는 학교가 없습니다 "
                       f"(현재 지역 필터: {region_filter}).")
        else:
            school = st.selectbox(
                f"결과 ({len(matched)}개)",
                matched[:300] if len(matched) > 300 else matched)
            # 실제 심층 분석은 df 전체로 (해당 학교의 모든 레코드 사용)
            res = a_deep_school(df, school)
            if res:
                ry = res["by_year"].reset_index().melt(
                    id_vars="입시년도",
                    value_vars=["지원", "발표합격", "최종등록"],
                    var_name="구분", value_name="인원")
                fig = px.bar(ry, x="입시년도", y="인원", color="구분",
                             barmode="group", text="인원",
                             title=f"{school} — 연도별", height=400)
                st.plotly_chart(fig, use_container_width=True)

                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("#### 연도별 상세")
                    show_result(res["by_year"], height=200)
                with c2:
                    st.markdown("#### 전형구분별")
                    show_result(res["by_admtype"], height=250)

                st.markdown("#### 모집단위별 지원")
                show_result(res["by_unit"], height=400)

                if st.button("💾 이 학교 저장", key="s3_7_save"):
                    save_to_store(f"3-7_{school}_연도별", res["by_year"],
                                  f"{school} 심층 - 연도별")
                    save_to_store(f"3-7_{school}_학과", res["by_unit"],
                                  f"{school} 심층 - 학과별")
                    st.success(f"저장됨 ({len(st.session_state.results)}개)")


# ─────────────────────────────────────────────────────────────
#  🎯 전략 발굴 ⭐
# ─────────────────────────────────────────────────────────────
elif group.startswith("🎯"):
    st.markdown("## 🎯 전략 발굴 ⭐")
    st.caption("예산·인력이 한정된 상황에서 어디에 집중해야 성과가 날까?")

    st.info("**입시 홍보 전략의 핵심 페이지입니다.** "
            "4-1(매트릭스)부터 순서대로 살펴보시면 "
            "학교별 전략 방향이 명확해집니다.")

    sub = st.radio("세부 분석",
        ["4-1. 4분류 매트릭스 ⭐",
         "4-2. 지원↑·등록↓ (개선)",
         "4-3. 지원↓·등록↑ (잠재)",
         "4-4. 전환율 Top (충성)",
         "4-5. 전환율 Bottom (이탈)",
         "4-6. 규모대비 지원율",
         "4-7. 규모대비 등록률"],
        horizontal=True, label_visibility="collapsed")

    if sub.startswith("4-1"):
        min_apply = st.slider("최소 지원자 수", 10, 100, 30, step=5,
                              key="s4_1_m")
        summary, detail = a_school_matrix(df, min_apply)
        if len(summary) == 0:
            st.warning("조건 불충족")
        else:
            apply_med = summary.attrs.get("apply_median", 0)
            reg_med = summary.attrs.get("reg_median", 0)

            # 분류별 카드 4개
            c1, c2, c3, c4 = st.columns(4)
            for col, letter, name, desc, color in [
                (c1, "A", "핵심관리", "지원↑·등록↑ 관계 유지", "🟢"),
                (c2, "B", "전환율개선", "지원↑·등록↓ 원인 조사", "🟠"),
                (c3, "C", "잠재력발굴", "지원↓·등록↑ 홍보 확대", "🔵"),
                (c4, "D", "신규개척", "지원↓·등록↓ 장기 구축", "⚪"),
            ]:
                n = len(detail[detail["분류"].str.startswith(letter)])
                with col:
                    st.markdown(f"**{color} {letter}. {name}** ({n}개)")
                    st.caption(desc)

            # 산점도
            rr = detail.reset_index()
            fig = px.scatter(
                rr, x="총지원", y="지원대비등록(%)", color="분류",
                hover_name="고등학교명",
                hover_data=["총합격", "총등록", "고교소재지", "설립구분"],
                title=f"4분류 매트릭스 (지원 {min_apply}명↑)",
                height=580,
                color_discrete_map={
                    "A: 지원강·등록강 (핵심관리)": "#54A24B",
                    "B: 지원강·등록약 (전환율개선)": "#F58518",
                    "C: 지원약·등록강 (잠재력발굴)": "#4C78A8",
                    "D: 지원약·등록약 (신규개척)": "#999999",
                })
            fig.add_vline(x=apply_med, line_dash="dash", line_color="gray",
                          annotation_text=f"지원 중앙값 {apply_med:.0f}")
            fig.add_hline(y=reg_med, line_dash="dash", line_color="gray",
                          annotation_text=f"등록률 중앙값 {reg_med:.1f}%")
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("### 분류별 요약")
            show_result(summary, height=200)

            # 분류별 Top 20 탭
            tabs = st.tabs([
                "A — 핵심관리", "B — 전환율개선",
                "C — 잠재력발굴", "D — 신규개척"])
            for tab, letter in zip(tabs, ["A", "B", "C", "D"]):
                with tab:
                    sub_df = detail[detail["분류"].str.startswith(letter)].head(20)
                    show_result(sub_df, height=420)

            if st.button("💾 매트릭스 저장 (요약 + 상세)", key="s4_1_save"):
                save_to_store("4-1_4분류_요약", summary, "4분류 매트릭스 요약")
                save_to_store("4-1_4분류_상세", detail, "4분류 매트릭스 상세")
                st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("4-2"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 10, 50, 20, step=5, key="s4_2_n")
        with c2:
            min_apply = st.slider("최소 지원자", 10, 100, 30, step=5,
                                   key="s4_2_m")
        r = a_gap_high_apply_low_reg(df, top_n, min_apply)
        rr = r.reset_index()
        fig = px.bar(rr, x="지원대비등록(%)", y="고등학교명", orientation="h",
                     color="총지원", color_continuous_scale="Oranges",
                     hover_data=["총합격", "총등록", "고교소재지", "설립구분"],
                     title=f"전환율 개선 대상 (지원 {min_apply}↑, Top {top_n})",
                     height=max(400, top_n * 25))
        fig.update_layout(yaxis={'categoryorder': 'total descending'})
        st.plotly_chart(fig, use_container_width=True)
        st.warning("⚠ **해석**: 많이 지원하는데 등록이 잘 안 되는 학교들. "
                   "왜 이탈하는지 방문 조사·원인 파악이 필요합니다.")
        show_result(r)
        if st.button("💾 저장소에 담기", key="s4_2_save"):
            save_to_store(f"4-2_전환율개선_Top{top_n}", r,
                          f"전환율 개선 대상 (지원 {min_apply}↑)")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("4-3"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 10, 50, 20, step=5, key="s4_3_n")
        with c2:
            min_reg = st.slider("최소 등록자", 1, 20, 3, step=1, key="s4_3_m")
        r = a_gap_low_apply_high_reg(df, top_n, min_reg)
        rr = r.reset_index()
        fig = px.scatter(rr, x="총지원", y="총등록",
                         size="지원대비등록(%)", hover_name="고등학교명",
                         color="고교소재지",
                         hover_data=["총합격", "지원대비등록(%)", "설립구분"],
                         title=f"잠재력 발굴 (등록 {min_reg}↑, Top {top_n})",
                         height=520)
        st.plotly_chart(fig, use_container_width=True)
        st.success("✨ **해석**: 지원은 적지만 등록률이 높은 학교들. "
                   "우리 대학에 관심 있는 학생들이 많다는 뜻. "
                   "홍보를 강화하면 즉시 성과가 납니다.")
        show_result(r)
        if st.button("💾 저장소에 담기", key="s4_3_save"):
            save_to_store(f"4-3_잠재력_Top{top_n}", r,
                          f"잠재력 발굴 (등록 {min_reg}↑)")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("4-4"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 10, 50, 20, step=5, key="s4_4_n")
        with c2:
            min_pass = st.slider("최소 합격자", 5, 50, 10, step=1,
                                  key="s4_4_m")
        r = a_conversion_high(df, top_n, min_pass)
        rr = r.reset_index()
        fig = px.bar(rr, x="등록률(%)", y="고등학교명", orientation="h",
                     color="총등록", color_continuous_scale="Greens",
                     hover_data=["총지원", "총합격", "고교소재지"],
                     title=f"전환율 Top {top_n} (합격 {min_pass}↑)",
                     height=max(400, top_n * 25))
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        st.success("💚 **해석**: 합격하면 반드시 오는 학교들. "
                   "우리 대학에 대한 충성도가 높음. 관계 유지·심화.")
        show_result(r)
        if st.button("💾 저장소에 담기", key="s4_4_save"):
            save_to_store(f"4-4_전환율Top{top_n}", r,
                          f"합격→등록 전환율 Top {top_n}")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("4-5"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 10, 50, 20, step=5, key="s4_5_n")
        with c2:
            min_pass = st.slider("최소 합격자", 5, 50, 10, step=1,
                                  key="s4_5_m")
        r = a_conversion_low(df, top_n, min_pass)
        rr = r.reset_index()
        fig = px.bar(rr, x="등록률(%)", y="고등학교명", orientation="h",
                     color="총합격", color_continuous_scale="Reds",
                     hover_data=["총지원", "총등록", "고교소재지"],
                     title=f"전환율 Bottom {top_n} (합격 {min_pass}↑)",
                     height=max(400, top_n * 25))
        fig.update_layout(yaxis={'categoryorder': 'total descending'})
        st.plotly_chart(fig, use_container_width=True)
        st.error("🚨 **해석**: 합격시켜도 이탈하는 학교들. "
                 "타 대학과 중복 합격 후 선택받지 못하는 원인 파악 필요.")
        show_result(r)
        if st.button("💾 저장소에 담기", key="s4_5_save"):
            save_to_store(f"4-5_전환율Bottom{top_n}", r,
                          f"합격→등록 전환율 Bottom {top_n}")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("4-6"):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 10, 50, 20, step=5, key="s4_6_n")
        with c2:
            min_size = st.slider("최소 고3학년수", 20, 200, 50, step=10,
                                  key="s4_6_m")
        r = a_size_apply_ratio(df, top_n, min_size)
        rr = r.reset_index()
        fig = px.bar(rr, x="규모대비지원율(%)", y="고등학교명",
                     orientation="h", color="규모대비등록률(%)",
                     color_continuous_scale="Viridis",
                     hover_data=["총지원", "총등록", "고3학년수", "고교소재지"],
                     title=f"규모 대비 지원율 (고3 {min_size}↑, Top {top_n})",
                     height=max(400, top_n * 25))
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s4_6_save"):
            save_to_store(f"4-6_규모대비지원율_Top{top_n}", r,
                          f"규모 대비 지원율 Top {top_n}")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    else:   # 4-7
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("상위 N", 10, 50, 20, step=5, key="s4_7_n")
        with c2:
            min_size = st.slider("최소 고3학년수", 20, 200, 50, step=10,
                                  key="s4_7_m")
        r = a_size_reg_ratio(df, top_n, min_size)
        rr = r.reset_index()
        fig = px.bar(rr, x="규모대비등록률(%)", y="고등학교명",
                     orientation="h", color="규모대비지원율(%)",
                     color_continuous_scale="Viridis",
                     hover_data=["총지원", "총등록", "고3학년수", "고교소재지"],
                     title=f"규모 대비 등록률 (고3 {min_size}↑, Top {top_n})",
                     height=max(400, top_n * 25))
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s4_7_save"):
            save_to_store(f"4-7_규모대비등록률_Top{top_n}", r,
                          f"규모 대비 등록률 Top {top_n}")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")


# ─────────────────────────────────────────────────────────────
#  🔽 Funnel
# ─────────────────────────────────────────────────────────────
elif group.startswith("🔽"):
    st.markdown("## 🔽 Funnel 분석")
    st.caption("지원 → 합격 → 등록 단계에서 어디서 얼마나 빠지는가")

    sub = st.radio("세부 분석",
        ["5-1. 전체 Funnel",
         "5-2. 연도별 Funnel",
         "5-3. 지역별 Funnel",
         "5-4. 전형구분별 Funnel",
         "5-5. 충원 차수 분석"],
        horizontal=True, label_visibility="collapsed")

    if sub.startswith("5-1"):
        r = a_funnel_overall(df)
        fig = go.Figure(go.Funnel(
            y=r["단계"], x=r["인원수"],
            textposition="inside",
            textinfo="value+percent initial",
            marker={"color": ["#4C78A8", "#F58518", "#54A24B"]},
        ))
        fig.update_layout(title="전체 입시 Funnel", height=480)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s5_1"):
            save_to_store("5-1_전체Funnel", r, "전체 지원→합격→등록")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("5-2"):
        r = a_funnel_by_year(df)
        chart = st.selectbox("차트 유형",
            ["연도별 Funnel", "묶음 막대", "선 (비율)"])

        if chart == "연도별 Funnel":
            fig = go.Figure()
            for yr in r.index:
                fig.add_trace(go.Funnel(
                    name=str(yr),
                    y=["지원자", "발표합격", "최종등록"],
                    x=[r.loc[yr, "지원자"], r.loc[yr, "발표합격"],
                       r.loc[yr, "최종등록"]],
                    textinfo="value+percent initial"))
            fig.update_layout(title="연도별 Funnel", height=520)
        elif chart == "묶음 막대":
            rr = r.reset_index().melt(
                id_vars="입시년도",
                value_vars=["지원자", "발표합격", "최종등록"],
                var_name="단계", value_name="인원")
            fig = px.bar(rr, x="입시년도", y="인원", color="단계",
                         barmode="group", text="인원",
                         title="연도별 Funnel 인원", height=460)
        else:
            rr = r.reset_index().melt(
                id_vars="입시년도",
                value_vars=["합격률(%)", "등록률(%)", "지원대비등록(%)"],
                var_name="지표", value_name="값")
            fig = px.line(rr, x="입시년도", y="값", color="지표",
                          markers=True, title="연도별 비율 추이",
                          height=460)
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s5_2"):
            save_to_store("5-2_연도별Funnel", r, "연도별 Funnel")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("5-3"):
        r = a_funnel_by_region(df)
        top_n = st.slider("표시 지역 수", 5, 17, 10, key="s5_3_n")
        r_top = r.head(top_n)
        rr = r_top.reset_index().melt(
            id_vars="고교소재지",
            value_vars=["지원자", "발표합격", "최종등록"],
            var_name="단계", value_name="인원")
        fig = px.bar(rr, x="인원", y="고교소재지", color="단계",
                     orientation="h", barmode="group",
                     title=f"지역별 Funnel (Top {top_n})", height=500)
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s5_3"):
            save_to_store("5-3_지역Funnel", r, "지역별 Funnel")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("5-4"):
        top_n = st.slider("상위 전형 수", 5, 30, 20, key="s5_4_n")
        r = a_funnel_by_admtype(df, top_n)
        rr = r.reset_index().melt(
            id_vars="전형구분",
            value_vars=["지원자", "발표합격", "최종등록"],
            var_name="단계", value_name="인원")
        fig = px.bar(rr, x="인원", y="전형구분", color="단계",
                     orientation="h", barmode="group",
                     title=f"전형구분별 Funnel (Top {top_n})",
                     height=max(500, top_n * 28))
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        show_result(r)
        if st.button("💾 저장소에 담기", key="s5_4"):
            save_to_store(f"5-4_전형Funnel_Top{top_n}", r,
                          f"전형구분별 Funnel (Top {top_n})")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")

    else:   # 5-5
        r = a_fill_round(df)
        rr = r.reset_index().rename(columns={"index": "차수"})
        fig = px.bar(rr, x="차수", y="합격자수",
                     text="합격자수",
                     title="충원 차수별 합격자 분포", height=440,
                     color="차수")
        st.plotly_chart(fig, use_container_width=True)
        st.info("💡 **해석**: 최초합격 비중이 높을수록 우리 대학 충성도가 높고, "
                "충원 차수가 길어질수록 이탈이 많다는 뜻.")
        show_result(r)
        if st.button("💾 저장소에 담기", key="s5_5"):
            save_to_store("5-5_충원차수", r, "충원 차수별 분포")
            st.success(f"저장됨 ({len(st.session_state.results)}개)")


# ─────────────────────────────────────────────────────────────
#  🔍 심층 분석
# ─────────────────────────────────────────────────────────────
elif group.startswith("🔍"):
    st.markdown("## 🔍 심층 분석")
    st.caption("특정 학과·고교·지역을 자세히 들여다보기")

    sub = st.radio("세부 분석",
        ["6-1. 학과 선택",
         "6-2. 고교 검색",
         "6-3. 지역 선택"],
        horizontal=True, label_visibility="collapsed")

    if sub.startswith("6-1"):
        units = sorted(df["모집단위명"].dropna().unique())
        unit = st.selectbox("모집단위 선택", units, key="s6_1_unit")
        res = a_deep_unit(df, unit)
        if res is None:
            st.warning("데이터 없음")
        else:
            st.markdown(f"### 🎓 {unit}")
            ry = res["by_year"].reset_index().melt(
                id_vars="입시년도",
                value_vars=["지원", "발표합격", "최종등록"],
                var_name="구분", value_name="인원")
            fig = px.bar(ry, x="입시년도", y="인원", color="구분",
                         barmode="group", text="인원", height=380)
            st.plotly_chart(fig, use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown("#### 연도별")
                show_result(res["by_year"], height=200)
                st.markdown("#### 전형별")
                show_result(res["by_admtype"], height=260)
            with c2:
                st.markdown("#### 지역별")
                show_result(res["by_region"], height=260)
                st.markdown("#### 주요 지원 고교 (Top 20)")
                show_result(res["by_school"], height=260)

            if st.button("💾 이 학과 저장", key="s6_1_save"):
                save_to_store(f"6-1_{unit}_연도", res["by_year"],
                              f"{unit} 심층")
                save_to_store(f"6-1_{unit}_고교", res["by_school"],
                              f"{unit} 심층 - 고교")
                st.success(f"저장됨 ({len(st.session_state.results)}개)")

    elif sub.startswith("6-2"):
        keyword = st.text_input("고교 이름 검색", key="s6_2_kw",
                                 placeholder="예: 강릉문성, 판곡, 광문")
        all_schools = sorted([s for s in df["고등학교명"].dropna().unique()
                              if "검정고시" not in str(s)])
        matched = [s for s in all_schools if keyword in str(s)] \
                  if keyword else all_schools
        if len(matched) == 0:
            st.warning("일치 학교 없음")
        else:
            school = st.selectbox(
                f"결과 ({len(matched)}개)",
                matched[:200] if len(matched) > 200 else matched,
                key="s6_2_school")
            res = a_deep_school(df, school)
            if res:
                st.markdown(f"### 🏫 {school}")
                ry = res["by_year"].reset_index().melt(
                    id_vars="입시년도",
                    value_vars=["지원", "발표합격", "최종등록"],
                    var_name="구분", value_name="인원")
                fig = px.bar(ry, x="입시년도", y="인원", color="구분",
                             barmode="group", text="인원", height=380)
                st.plotly_chart(fig, use_container_width=True)

                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("#### 연도별"); show_result(res["by_year"], height=200)
                with c2:
                    st.markdown("#### 전형별"); show_result(res["by_admtype"], height=260)
                st.markdown("#### 모집단위별 지원")
                show_result(res["by_unit"], height=400)
                if st.button("💾 이 학교 저장", key="s6_2_save"):
                    save_to_store(f"6-2_{school}_연도", res["by_year"],
                                  f"{school} 심층")
                    save_to_store(f"6-2_{school}_학과", res["by_unit"],
                                  f"{school} 심층 - 학과")
                    st.success(f"저장됨 ({len(st.session_state.results)}개)")

    else:   # 6-3
        regions = sorted(df["고교소재지"].dropna().unique())
        region = st.selectbox("지역 선택", regions, key="s6_3_r")
        res = a_deep_region(df, region)
        if res is None:
            st.warning("데이터 없음")
        else:
            st.markdown(f"### 🗺 {region}")
            ry = res["by_year"].reset_index().melt(
                id_vars="입시년도",
                value_vars=["지원", "발표합격", "최종등록"],
                var_name="구분", value_name="인원")
            fig = px.bar(ry, x="입시년도", y="인원", color="구분",
                         barmode="group", text="인원", height=380)
            st.plotly_chart(fig, use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown("#### 연도별"); show_result(res["by_year"], height=200)
                st.markdown("#### 주요 고교 (Top 30)")
                show_result(res["by_school"], height=380)
            with c2:
                st.markdown("#### 선호 학과 (Top 20)")
                show_result(res["by_unit"], height=600)
            if st.button("💾 이 지역 저장", key="s6_3_save"):
                save_to_store(f"6-3_{region}_연도", res["by_year"],
                              f"{region} 심층")
                save_to_store(f"6-3_{region}_고교", res["by_school"],
                              f"{region} 심층 - 고교")
                st.success(f"저장됨 ({len(st.session_state.results)}개)")


# ─────────────────────────────────────────────────────────────
#  📝 공유·리포트
# ─────────────────────────────────────────────────────────────
else:
    st.markdown("## 📝 공유·리포트")
    st.caption("발견한 인사이트를 정리하고 공유")

    sub = st.radio("세부 분석",
        ["7-1. 자동 인사이트 리포트",
         "7-2. 저장소 현황"],
        horizontal=True, label_visibility="collapsed")

    if sub.startswith("7-1"):
        res = a_insight_report(df)
        if res is None:
            st.warning("2개년 이상 데이터 필요")
        else:
            years = sorted(df["입시년도"].unique())
            prev, last = years[-2], years[-1]
            st.info(f"📊 **핵심 요약**: {res['summary_text']}")

            st.markdown("#### [1] 전체 지원자 증감")
            show_result(res["overview"], height=170)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f"#### [2] 지원 증가 Top5 ({prev}→{last})")
                show_result(res["unit_up"], height=230)
                st.markdown("#### [3] 등록 이탈 학과 Top5")
                show_result(res["unit_weak"], height=230)
                st.markdown("#### [5] 시도별 변화")
                show_result(res["reg_change"], height=380)
            with c2:
                st.markdown(f"#### [2'] 지원 감소 Top5 ({prev}→{last})")
                show_result(res["unit_down"], height=230)
                st.markdown("#### [3'] 등록 견고 학과 Top5")
                show_result(res["unit_strong"], height=230)
                st.markdown("#### [4] 핵심 피더스쿨 (최근 2년 등록)")
                show_result(res["feeder"], height=380)

            if st.button("💾 전체 리포트 저장", use_container_width=True,
                         type="primary"):
                for name, key, desc in [
                    ("overview",    "7_리포트_전체증감",     "전체 지원자 증감"),
                    ("unit_up",     "7_리포트_증가Top5",     "증가 학과 Top5"),
                    ("unit_down",   "7_리포트_감소Top5",     "감소 학과 Top5"),
                    ("unit_weak",   "7_리포트_이탈Top5",     "등록 이탈 Top5"),
                    ("unit_strong", "7_리포트_견고Top5",     "등록 견고 Top5"),
                    ("reg_change",  "7_리포트_시도변화",     "시도별 변화"),
                    ("feeder",      "7_리포트_피더스쿨",     "핵심 피더스쿨"),
                ]:
                    save_to_store(key, res[name], desc)
                st.success(f"7개 항목 저장됨 (저장소 {len(st.session_state.results)}개) "
                           "— 사이드바에서 엑셀로 다운로드하세요")

    else:   # 7-2
        n = len(st.session_state.results)
        if n == 0:
            st.info("📭 저장소가 비어 있습니다. 각 분석에서 💾 버튼으로 담아주세요.")
        else:
            st.success(f"✅ **{n}개 분석**이 저장소에 담겨 있습니다. "
                       "사이드바의 📥 버튼으로 엑셀 다운로드 가능합니다.")
            for k, v in st.session_state.results.items():
                with st.expander(f"📄 {k}  ({v['saved_at']})"):
                    st.caption(v["description"])
                    st.dataframe(v["df"], use_container_width=True, height=300)


# Footer
st.markdown("---")
st.caption(
    f"🎓 입시 홍보 분석 대시보드  ·  "
    f"저장소 {len(st.session_state.results)}개  ·  "
    f"💡 문의·개선 제안이 있으시면 담당자에게 알려주세요"
)
