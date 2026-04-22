"""
analysis_engine.py — 입시 지원자 데이터 분석 엔진

순수 함수 모듈. Streamlit/Colab 어디서든 재사용 가능.
각 함수는 DataFrame 입력 → 분석 결과 DataFrame 반환 (UI 로직 없음).
"""
from __future__ import annotations

import io
import os
from pathlib import Path

import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════════════════
#  상수
# ═══════════════════════════════════════════════════════════════════════════
YEARS_EXCLUDE_FROM_TREND = {"작업치료학과", "디지털헬스케어전공"}
CAPITAL_REGIONS = {"서울", "경기", "인천"}


# ═══════════════════════════════════════════════════════════════════════════
#  데이터 로드
# ═══════════════════════════════════════════════════════════════════════════
def load_and_clean(source) -> pd.DataFrame:
    """엑셀 → 정제된 DataFrame. xls/xlsx, 경로/바이트/파일객체 모두 지원.

    source : str 경로 | bytes | 파일 객체 (Streamlit UploadedFile 등)
    """
    # 엔진 결정 + BytesIO 변환
    if isinstance(source, (bytes, bytearray)):
        # 바이트는 파일명을 모르므로 openpyxl 기본, 실패 시 xlrd
        try:
            df = pd.read_excel(io.BytesIO(source),
                               sheet_name="3개년데이터", engine="openpyxl")
        except Exception:
            df = pd.read_excel(io.BytesIO(source),
                               sheet_name="3개년데이터", engine="xlrd")
    elif isinstance(source, (str, Path)):
        ext = os.path.splitext(str(source))[1].lower()
        engine = "xlrd" if ext == ".xls" else "openpyxl"
        df = pd.read_excel(source, sheet_name="3개년데이터", engine=engine)
    else:
        # 파일 객체
        name = getattr(source, "name", "")
        engine = "xlrd" if name.lower().endswith(".xls") else "openpyxl"
        df = pd.read_excel(source, sheet_name="3개년데이터", engine=engine)

    # 컬럼명 안전장치
    df = df.rename(columns={"Unnamed: 11": "등록상태"})
    if set(df["합격상태"].dropna().unique()) <= {"등록", "미등록"}:
        df = df.rename(columns={"합격상태": "등록상태", "등록상태": "합격상태"})

    df["입시년도"] = df["입시년도"].astype(int)
    df["합격구분"] = pd.to_numeric(df["합격구분"], errors="coerce").astype("Int64")

    hs = df["고등학교명"].astype(str)
    df["검정고시여부"] = (hs.str.contains("검정고시", na=False) |
                       df["고등학교명"].isna())
    df["최종등록여부"] = (df["합격상태"] == "합격")
    df["발표합격여부"] = (df["합격구분"].fillna(-999).astype(int) >= 0)
    df["예비후보여부"] = (df["합격상태"] == "예비후보")
    df["고3학년수_num"] = pd.to_numeric(
        df["고등학교3학년수(2025기준)"], errors="coerce")

    def region_group(x):
        if pd.isna(x):                return "미분류/검정"
        if x in CAPITAL_REGIONS:      return "수도권"
        if x == "강원":               return "강원"
        return "기타지방"
    df["지역그룹"] = df["고교소재지"].apply(region_group)

    return df


# ═══════════════════════════════════════════════════════════════════════════
#  공용 헬퍼
# ═══════════════════════════════════════════════════════════════════════════
def _school_base_table(df):
    """고교별 공통 집계 (검정고시 제외). 여러 분석에서 공통 사용."""
    d = df[~df["검정고시여부"] & df["고등학교명"].notna()].copy()
    total_apply = d.groupby("고등학교명").size()
    total_pass  = d[d["발표합격여부"]].groupby("고등학교명").size()
    total_reg   = d[d["최종등록여부"]].groupby("고등학교명").size()

    base = pd.DataFrame({"총지원": total_apply})
    base["총합격"] = total_pass.reindex(base.index).fillna(0).astype(int)
    base["총등록"] = total_reg.reindex(base.index).fillna(0).astype(int)
    base["합격률(%)"]       = (base["총합격"] / base["총지원"] * 100).round(1)
    base["등록률(%)"]       = (base["총등록"] /
                            base["총합격"].replace(0, np.nan) * 100).round(1)
    base["지원대비등록(%)"] = (base["총등록"] / base["총지원"] * 100).round(1)

    info = d.drop_duplicates("고등학교명").set_index("고등학교명")[
        ["고교소재지", "설립구분", "고교특성", "고3학년수_num"]]
    info = info.rename(columns={"고3학년수_num": "고3학년수"})
    base = base.join(info, how="left")
    return base


def _school_year_pivot(df):
    """고교 × 연도 지원자 수 피벗"""
    d = df[~df["검정고시여부"] & df["고등학교명"].notna()]
    return d.pivot_table(index="고등학교명", columns="입시년도",
                         values="성명", aggfunc="count", fill_value=0)


# ═══════════════════════════════════════════════════════════════════════════
#  그룹 1. 현황 진단
# ═══════════════════════════════════════════════════════════════════════════
def a_overview_by_year(df):
    """연도별 전체 지원·합격·등록 + 주요 비율"""
    g = df.groupby("입시년도").agg(
        지원=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    g["합격률(%)"] = (g["발표합격"] / g["지원"] * 100).round(1)
    g["등록률(%)"] = (g["최종등록"] / g["발표합격"] * 100).round(1)
    g["지원대비등록(%)"] = (g["최종등록"] / g["지원"] * 100).round(1)
    return g


def a_overview_year_admtype(df):
    """연도 × 모집구분 (수시/정시/추가)"""
    g = df.groupby(["입시년도", "모집구분"]).agg(
        지원=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    g["등록률(%)"] = (g["최종등록"] /
                    g["발표합격"].replace(0, np.nan) * 100).round(1)
    return g


def a_region_by_year(df):
    """지역별 × 연도별 (검정고시 제외)"""
    d = df[~df["검정고시여부"]]
    g = d.groupby(["고교소재지", "입시년도"]).agg(
        지원=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    total = g.groupby(level=0)["지원"].sum().sort_values(ascending=False)
    return g.loc[total.index]


def a_school_type_by_year(df):
    """고교특성(일반/자율/특목/특성화) × 연도별"""
    d = df[~df["검정고시여부"] & df["고교특성"].notna()]
    g = d.groupby(["고교특성", "입시년도"]).agg(
        지원=("성명", "count"),
        최종등록=("최종등록여부", "sum"))
    g["등록률(%)"] = (g["최종등록"] / g["지원"] * 100).round(1)
    return g


def a_school_establishment_by_year(df):
    """설립구분(공/사/국립) × 연도별"""
    d = df[~df["검정고시여부"] & df["설립구분"].notna()]
    g = d.groupby(["설립구분", "입시년도"]).agg(
        지원=("성명", "count"),
        최종등록=("최종등록여부", "sum"))
    g["등록률(%)"] = (g["최종등록"] / g["지원"] * 100).round(1)
    return g


def a_admission_type_by_year(df):
    """전형구분 × 연도별 추이"""
    g = df.groupby(["전형구분", "입시년도"]).agg(
        지원=("성명", "count"),
        최종등록=("최종등록여부", "sum"))
    total = g.groupby(level=0)["지원"].sum().sort_values(ascending=False)
    return g.loc[total.index]


# ═══════════════════════════════════════════════════════════════════════════
#  그룹 2. 학과 진단
# ═══════════════════════════════════════════════════════════════════════════
def a_unit_3year_total(df):
    """학과별 3년 누적 요약"""
    g = df.groupby("모집단위명").agg(
        지원=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    g["합격률(%)"] = (g["발표합격"] / g["지원"] * 100).round(1)
    g["등록률(%)"] = (g["최종등록"] /
                    g["발표합격"].replace(0, np.nan) * 100).round(1)
    g["지원대비등록(%)"] = (g["최종등록"] / g["지원"] * 100).round(1)
    return g.sort_values("지원", ascending=False)


def a_unit_by_year(df):
    """학과 × 연도별 상세 (연도별 합격률·등록률)"""
    g = df.groupby(["모집단위명", "입시년도"]).agg(
        지원=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    g["합격률(%)"] = (g["발표합격"] / g["지원"] * 100).round(1)
    g["등록률(%)"] = (g["최종등록"] /
                    g["발표합격"].replace(0, np.nan) * 100).round(1)
    g["지원대비등록(%)"] = (g["최종등록"] / g["지원"] * 100).round(1)
    return g


def a_unit_low_pass_rate(df, top_n=15, min_apply=50):
    """합격률이 낮은 학과 (경쟁 치열)"""
    g = a_unit_3year_total(df)
    g = g[g["지원"] >= min_apply]
    return g.sort_values("합격률(%)", ascending=True).head(top_n)


def a_unit_high_fill(df, top_n=20, min_pass=50):
    """추가합격 많은 학과 (이탈 많음).
    추가합격률(%) = 충원합격 / 총합격 * 100  [*대학알리미 '신입생 충원율'과 다른 지표*]"""
    d = df[df["발표합격여부"]].copy()
    g = d.groupby("모집단위명").agg(
        총합격=("성명", "count"),
        최초합격=("합격구분", lambda s: (s == 0).sum()))
    g["충원합격"] = g["총합격"] - g["최초합격"]
    g["추가합격률(%)"] = (g["충원합격"] / g["총합격"] * 100).round(1)
    g = g[g["총합격"] >= min_pass]
    return g.sort_values("추가합격률(%)", ascending=False).head(top_n)


def a_unit_low_fill(df, top_n=20, min_pass=50):
    """추가합격 적은 학과 (안정 마감).
    추가합격률(%) = 충원합격 / 총합격 * 100  [*대학알리미 '신입생 충원율'과 다른 지표*]"""
    d = df[df["발표합격여부"]].copy()
    g = d.groupby("모집단위명").agg(
        총합격=("성명", "count"),
        최초합격=("합격구분", lambda s: (s == 0).sum()))
    g["충원합격"] = g["총합격"] - g["최초합격"]
    g["추가합격률(%)"] = (g["충원합격"] / g["총합격"] * 100).round(1)
    g = g[g["총합격"] >= min_pass]
    return g.sort_values("추가합격률(%)", ascending=True).head(top_n)


# ═══════════════════════════════════════════════════════════════════════════
#  그룹 3. 피더 스쿨
# ═══════════════════════════════════════════════════════════════════════════
def a_feeder_apply_top(df, top_n=20):
    """3년 누적 지원자 Top N"""
    base = _school_base_table(df)
    return base.sort_values("총지원", ascending=False).head(top_n)


def a_feeder_registered_top(df, top_n=20):
    """3년 누적 등록자 Top N"""
    base = _school_base_table(df)
    return base[base["총등록"] > 0].sort_values(
        "총등록", ascending=False).head(top_n)


def _school_trend(df, direction, top_n=None):
    """3년 연속 증가/감소 공통 로직"""
    pv = _school_year_pivot(df)
    years = sorted(c for c in pv.columns if isinstance(c, (int, np.integer)))
    if len(years) < 3:
        return pd.DataFrame()
    y1, y2, y3 = years[-3:]
    cont = pv[(pv[y1] > 0) & (pv[y2] > 0) & (pv[y3] > 0)].copy()
    if direction == "up":
        cont = cont[(cont[y2] > cont[y1]) & (cont[y3] > cont[y2])]
    else:
        cont = cont[(cont[y2] < cont[y1]) & (cont[y3] < cont[y2])]
    cont["3년증감"] = cont[y3] - cont[y1]
    cont["3년증감률(%)"] = (cont["3년증감"] / cont[y1] * 100).round(1)
    info = _school_base_table(df)[
        ["고교소재지", "설립구분", "고교특성", "고3학년수"]]
    cont = cont.join(info, how="left")
    cont = cont.sort_values("3년증감",
                             ascending=(direction == "down"))
    return cont.head(top_n) if top_n else cont


def a_school_3yr_increase(df, top_n=None):
    """3년 연속 지원 증가 고교"""
    return _school_trend(df, "up", top_n)


def a_school_3yr_decrease(df, top_n=None):
    """3년 연속 지원 감소 고교"""
    return _school_trend(df, "down", top_n)


def _school_delta(df, threshold, direction, top_n=None):
    """전년대비 급증/급감 공통 로직"""
    pv = _school_year_pivot(df)
    years = sorted(c for c in pv.columns if isinstance(c, (int, np.integer)))
    if len(years) < 2:
        return pd.DataFrame()
    prev, last = years[-2], years[-1]
    delta = pv[last] - pv[prev]

    if direction == "surge":
        mask = delta >= threshold
    else:
        mask = delta <= -threshold
    result = pv[mask].copy()
    result["증감"] = delta[mask]
    result["증감률(%)"] = (result["증감"] /
                        pv.loc[result.index, prev].replace(0, np.nan)
                        * 100).round(1)
    info = _school_base_table(df)[
        ["고교소재지", "설립구분", "고교특성", "고3학년수"]]
    result = result.join(info, how="left")
    result = result.sort_values("증감",
                                 ascending=(direction == "drop"))
    return result.head(top_n) if top_n else result


def a_school_surge(df, threshold=15, top_n=None):
    """전년대비 N명 이상 급증한 고교"""
    return _school_delta(df, threshold, "surge", top_n)


def a_school_drop(df, threshold=15, top_n=None):
    """전년대비 N명 이상 급감한 고교"""
    return _school_delta(df, threshold, "drop", top_n)


# ═══════════════════════════════════════════════════════════════════════════
#  그룹 4. 전략 발굴 (핵심)
# ═══════════════════════════════════════════════════════════════════════════
def a_gap_high_apply_low_reg(df, top_n=20, min_apply=30):
    """지원 많지만 등록 약한 고교 (전환율 개선 대상)"""
    base = _school_base_table(df)
    base = base[base["총지원"] >= min_apply]
    return base.sort_values("지원대비등록(%)", ascending=True).head(top_n)


def a_gap_low_apply_high_reg(df, top_n=20, min_reg=3):
    """지원 적어도 등록 강한 고교 (잠재력 발굴)"""
    base = _school_base_table(df)
    base = base[base["총등록"] >= min_reg]
    return base.sort_values("지원대비등록(%)", ascending=False).head(top_n)


def a_conversion_high(df, top_n=20, min_pass=10):
    """합격→등록 전환율 Top (충성도)"""
    base = _school_base_table(df)
    base = base[base["총합격"] >= min_pass]
    return base.sort_values(["등록률(%)", "총등록"],
                            ascending=[False, False]).head(top_n)


def a_conversion_low(df, top_n=20, min_pass=10):
    """합격→등록 전환율 Bottom (이탈)"""
    base = _school_base_table(df)
    base = base[base["총합격"] >= min_pass]
    return base.sort_values("등록률(%)", ascending=True).head(top_n)


def a_school_matrix(df, min_apply=30):
    """학교 4분류 매트릭스 (A/B/C/D)"""
    base = _school_base_table(df)
    mat = base[base["총지원"] >= min_apply].copy()
    if len(mat) == 0:
        return pd.DataFrame(), pd.DataFrame()
    apply_med = mat["총지원"].median()
    reg_med = mat["지원대비등록(%)"].median()

    def classify(row):
        hi_a = row["총지원"] >= apply_med
        hi_r = row["지원대비등록(%)"] >= reg_med
        if hi_a and hi_r:     return "A: 지원강·등록강 (핵심관리)"
        if hi_a and not hi_r: return "B: 지원강·등록약 (전환율개선)"
        if not hi_a and hi_r: return "C: 지원약·등록강 (잠재력발굴)"
        return "D: 지원약·등록약 (신규개척)"

    mat["분류"] = mat.apply(classify, axis=1)
    mat = mat.sort_values(["분류", "총지원"], ascending=[True, False])

    summary = mat.groupby("분류").agg(
        학교수=("총지원", "count"),
        평균지원=("총지원", "mean"),
        평균등록=("총등록", "mean"),
        평균등록률=("지원대비등록(%)", "mean"),
    ).round(1)
    summary.attrs["apply_median"] = apply_med
    summary.attrs["reg_median"] = reg_med
    return summary, mat


def a_size_apply_ratio(df, top_n=20, min_size=50):
    """규모(고3학년수) 대비 지원율 Top"""
    base = _school_base_table(df)
    r = base[base["고3학년수"] >= min_size].dropna(subset=["고3학년수"]).copy()
    r["규모대비지원율(%)"] = (r["총지원"] / r["고3학년수"] * 100).round(1)
    r["규모대비등록률(%)"] = (r["총등록"] / r["고3학년수"] * 100).round(1)
    return r.sort_values("규모대비지원율(%)", ascending=False).head(top_n)


def a_size_reg_ratio(df, top_n=20, min_size=50):
    """규모(고3학년수) 대비 등록률 Top"""
    base = _school_base_table(df)
    r = base[base["고3학년수"] >= min_size].dropna(subset=["고3학년수"]).copy()
    r["규모대비지원율(%)"] = (r["총지원"] / r["고3학년수"] * 100).round(1)
    r["규모대비등록률(%)"] = (r["총등록"] / r["고3학년수"] * 100).round(1)
    return r.sort_values("규모대비등록률(%)", ascending=False).head(top_n)


# ═══════════════════════════════════════════════════════════════════════════
#  그룹 5. Funnel
# ═══════════════════════════════════════════════════════════════════════════
def a_funnel_overall(df):
    """전체 Funnel (지원→합격→등록)"""
    n_apply = len(df)
    n_pass = int(df["발표합격여부"].sum())
    n_reg = int(df["최종등록여부"].sum())
    f = pd.DataFrame([
        ("지원자", n_apply), ("발표합격", n_pass), ("최종등록", n_reg),
    ], columns=["단계", "인원수"])
    f["전단계대비(%)"] = (f["인원수"] / f["인원수"].shift(1) * 100).round(1)
    f["최초대비(%)"] = (f["인원수"] / f["인원수"].iloc[0] * 100).round(1)
    return f


def a_funnel_by_year(df):
    """연도별 Funnel 추이"""
    years = sorted(df["입시년도"].unique())
    rows = []
    for y in years:
        sub = df[df["입시년도"] == y]
        rows.append({
            "입시년도": y, "지원자": len(sub),
            "발표합격": int(sub["발표합격여부"].sum()),
            "최종등록": int(sub["최종등록여부"].sum()),
        })
    f = pd.DataFrame(rows).set_index("입시년도")
    f["합격률(%)"] = (f["발표합격"] / f["지원자"] * 100).round(1)
    f["등록률(%)"] = (f["최종등록"] / f["발표합격"] * 100).round(1)
    f["지원대비등록(%)"] = (f["최종등록"] / f["지원자"] * 100).round(1)
    return f


def a_funnel_by_region(df):
    """지역별 Funnel"""
    d = df[~df["검정고시여부"]]
    g = d.groupby("고교소재지").agg(
        지원자=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    g["합격률(%)"] = (g["발표합격"] / g["지원자"] * 100).round(1)
    g["등록률(%)"] = (g["최종등록"] /
                    g["발표합격"].replace(0, np.nan) * 100).round(1)
    g["지원대비등록(%)"] = (g["최종등록"] / g["지원자"] * 100).round(1)
    return g.sort_values("지원자", ascending=False)


def a_funnel_by_admtype(df, top_n=20):
    """전형구분별 Funnel"""
    g = df.groupby("전형구분").agg(
        지원자=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    g["합격률(%)"] = (g["발표합격"] / g["지원자"] * 100).round(1)
    g["등록률(%)"] = (g["최종등록"] /
                    g["발표합격"].replace(0, np.nan) * 100).round(1)
    g["지원대비등록(%)"] = (g["최종등록"] / g["지원자"] * 100).round(1)
    return g.sort_values("지원자", ascending=False).head(top_n)


def a_fill_round(df):
    """충원 차수별 분포"""
    d = df[df["발표합격여부"]].copy()
    d["차수"] = d["합격구분"].fillna(-1).astype(int)
    g = d.groupby("차수").size().to_frame("합격자수")
    g["비율(%)"] = (g["합격자수"] / g["합격자수"].sum() * 100).round(1)
    g.index = g.index.map(lambda x: "최초합격" if x == 0 else f"{x}차 충원")
    return g


# ═══════════════════════════════════════════════════════════════════════════
#  그룹 6. 심층 분석
# ═══════════════════════════════════════════════════════════════════════════
def a_deep_unit(df, unit_name):
    """특정 학과 심층 — dict 반환 (by_year, by_admtype, by_school, by_region)"""
    d = df[df["모집단위명"] == unit_name]
    if len(d) == 0: return None
    by_year = d.groupby("입시년도").agg(
        지원=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    by_year["합격률(%)"] = (by_year["발표합격"] / by_year["지원"] * 100).round(1)
    by_year["등록률(%)"] = (by_year["최종등록"] /
                         by_year["발표합격"].replace(0, np.nan) * 100).round(1)
    by_admtype = (d.groupby("전형구분").size()
                   .sort_values(ascending=False).to_frame("지원수"))
    dd = d[~d["검정고시여부"] & d["고등학교명"].notna()]
    by_school = (dd["고등학교명"].value_counts().head(20).to_frame("지원수"))
    by_region = dd["고교소재지"].value_counts().to_frame("지원수")
    return {"by_year": by_year, "by_admtype": by_admtype,
            "by_school": by_school, "by_region": by_region}


def a_deep_school(df, school_name):
    """특정 고교 심층 — dict (by_year, by_unit, by_admtype)"""
    d = df[df["고등학교명"] == school_name]
    if len(d) == 0: return None
    by_year = d.groupby("입시년도").agg(
        지원=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    by_unit = d.pivot_table(index="모집단위명", columns="입시년도",
                            values="성명", aggfunc="count", fill_value=0)
    by_unit["합계"] = by_unit.sum(axis=1)
    by_unit = by_unit.sort_values("합계", ascending=False)
    by_admtype = d["전형구분"].value_counts().to_frame("지원수")
    return {"by_year": by_year, "by_unit": by_unit, "by_admtype": by_admtype}


def a_deep_region(df, region):
    """특정 지역 심층 — dict (by_year, by_school, by_unit)"""
    d = df[df["고교소재지"] == region]
    if len(d) == 0: return None
    by_year = d.groupby("입시년도").agg(
        지원=("성명", "count"),
        발표합격=("발표합격여부", "sum"),
        최종등록=("최종등록여부", "sum"))
    by_school = (d.groupby("고등학교명").size()
                   .sort_values(ascending=False).head(30).to_frame("지원수"))
    by_unit = (d.groupby("모집단위명").size()
                  .sort_values(ascending=False).head(20).to_frame("지원수"))
    return {"by_year": by_year, "by_school": by_school, "by_unit": by_unit}


# ═══════════════════════════════════════════════════════════════════════════
#  그룹 7. 자동 리포트
# ═══════════════════════════════════════════════════════════════════════════
def a_insight_report(df):
    """홍보 인사이트 자동 리포트 — dict 반환"""
    years = sorted(df["입시년도"].unique())
    if len(years) < 2: return None
    prev, last = years[-2], years[-1]

    overview = df.groupby("입시년도").size().to_frame("지원자수")
    overview["증감률(%)"] = (overview["지원자수"].pct_change() * 100).round(1)

    pv = df.pivot_table(index="모집단위명", columns="입시년도",
                        values="성명", aggfunc="count", fill_value=0)
    both = pv[(pv[prev] >= 20) & (~pv.index.isin(YEARS_EXCLUDE_FROM_TREND))].copy()
    both["증감률(%)"] = ((both[last] - both[prev]) / both[prev] * 100).round(1)
    unit_up = both.sort_values("증감률(%)", ascending=False).head(5)[
        [prev, last, "증감률(%)"]]
    unit_down = both.sort_values("증감률(%)").head(5)[[prev, last, "증감률(%)"]]

    g = df[df["발표합격여부"]].groupby("모집단위명").agg(
        합격=("발표합격여부", "sum"),
        등록=("최종등록여부", "sum"))
    g = g[g["합격"] >= 20]
    g["등록률(%)"] = (g["등록"] / g["합격"] * 100).round(1)
    unit_weak = g.sort_values("등록률(%)").head(5)
    unit_strong = g.sort_values("등록률(%)", ascending=False).head(5)

    d_ = df[~df["검정고시여부"]]
    reg = d_.pivot_table(index="고교소재지", columns="입시년도",
                         values="성명", aggfunc="count", fill_value=0)
    reg["증감률(%)"] = ((reg[last] - reg[prev]) /
                      reg[prev].replace(0, np.nan) * 100).round(1)
    reg_change = reg.sort_values(last, ascending=False).head(10)

    d_reg = d_[(d_["최종등록여부"]) & (d_["입시년도"].isin([prev, last]))]
    feeder = (d_reg.groupby("고등학교명").size()
                    .sort_values(ascending=False).head(15)
                    .to_frame("등록자수"))
    return {
        "overview": overview, "unit_up": unit_up, "unit_down": unit_down,
        "unit_weak": unit_weak, "unit_strong": unit_strong,
        "reg_change": reg_change, "feeder": feeder,
        "summary_text": (
            f"전체 지원자 {overview.loc[prev,'지원자수']:,} → "
            f"{overview.loc[last,'지원자수']:,}명 "
            f"({(overview.loc[last,'지원자수']-overview.loc[prev,'지원자수']) / overview.loc[prev,'지원자수']*100:+.1f}%)"),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  홈 대시보드용 헬퍼
# ═══════════════════════════════════════════════════════════════════════════
def home_kpis(df):
    """홈 KPI 지표 + 전년 대비 증감률"""
    years = sorted(df["입시년도"].unique())
    last = years[-1]
    prev = years[-2] if len(years) >= 2 else None

    def count(year, mask=None):
        sub = df[df["입시년도"] == year]
        if mask is not None:
            sub = sub[mask(sub)]
        return len(sub)

    apply_l = count(last)
    pass_l = count(last, mask=lambda d: d["발표합격여부"])
    reg_l = count(last, mask=lambda d: d["최종등록여부"])

    def delta(c, p):
        if p is None or p == 0: return None
        return (c - p) / p * 100

    out = {
        "last_year": last, "prev_year": prev,
        "apply_last": apply_l, "pass_last": pass_l, "reg_last": reg_l,
        "pass_rate_last": round(pass_l / apply_l * 100, 1) if apply_l else 0,
        "reg_rate_last": round(reg_l / pass_l * 100, 1) if pass_l else 0,
    }
    if prev is not None:
        out["apply_delta"] = delta(apply_l, count(prev))
        out["pass_delta"] = delta(pass_l, count(prev, mask=lambda d: d["발표합격여부"]))
        out["reg_delta"] = delta(reg_l, count(prev, mask=lambda d: d["최종등록여부"]))
    return out


def home_insights(df, k=3):
    """홈 화면의 자동 인사이트 텍스트 (최대 k개)"""
    years = sorted(df["입시년도"].unique())
    if len(years) < 2:
        return ["데이터가 2개 연도 이상 있어야 추세 인사이트를 생성할 수 있습니다."]
    last, prev = years[-1], years[-2]
    insights = []

    n_last = int((df["입시년도"] == last).sum())
    n_prev = int((df["입시년도"] == prev).sum())
    diff = (n_last - n_prev) / n_prev * 100 if n_prev else 0
    arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
    insights.append(
        f"📈 **{last}학년도 총 지원자 {n_last:,}명** "
        f"(전년 대비 {diff:+.1f}% {arrow})")

    pv = df.pivot_table(index="모집단위명", columns="입시년도",
                        values="성명", aggfunc="count", fill_value=0)
    if prev in pv.columns and last in pv.columns:
        both = pv[(pv[prev] >= 50) & (~pv.index.isin(YEARS_EXCLUDE_FROM_TREND))].copy()
        if len(both) > 0:
            both["change"] = (both[last] - both[prev]) / both[prev] * 100
            top = both.reindex(both["change"].abs().sort_values(
                ascending=False).index).iloc[0]
            sign = "↑" if top["change"] > 0 else "↓"
            insights.append(
                f"🎓 최대 변화 학과: **{top.name}** "
                f"{top['change']:+.1f}% {sign} "
                f"({int(top[prev])}→{int(top[last])}명)")

    dd = df[(df["입시년도"] == last) & (~df["검정고시여부"])
            & (df["고등학교명"].notna())]
    if len(dd) > 0:
        ts = dd["고등학교명"].value_counts().head(1)
        if len(ts) > 0:
            insights.append(
                f"🏫 {last}년 최다 지원 고교: **{ts.index[0]}** ({ts.iloc[0]}명)")
    return insights[:k]
