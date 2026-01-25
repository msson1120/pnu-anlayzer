# app.py
import time
import requests
import pandas as pd
import streamlit as st
from io import BytesIO

VWORLD_DATA_URL = "https://api.vworld.kr/req/data"

def pnu_to_address_vworld(pnu: str, key: str, domain: str | None = None, timeout: int = 15) -> dict:
    """
    VWorld 2D 데이터(LX맵)에서 PNU로 조회 → 주소/구성요소 반환
    """
    pnu = (pnu or "").strip()
    if len(pnu) != 19 or not pnu.isdigit():
        return {"ok": False, "error": "INVALID_PNU", "pnu": pnu}

    params = {
        "service": "data",
        "version": "2.0",
        "request": "GetFeature",
        "format": "json",
        "data": "LT_C_LANDINFOBASEMAP",  # LX맵
        "key": key,
        "geometry": "false",
        "size": 1,
        "attrFilter": f"pnu:=:{pnu}",
        "columns": "pnu,sido_nm,sgg_nm,emd_nm,ri_nm,jibun,rn_nm,bld_mnnm,bld_slno,jimok,parea",
    }
    if domain:
        params["domain"] = domain

    r = requests.get(VWORLD_DATA_URL, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    status = data.get("response", {}).get("status")
    if status != "OK":
        return {"ok": False, "error": data.get("response", {}).get("error", {}), "pnu": pnu}

    features = (
        data.get("response", {})
            .get("result", {})
            .get("featureCollection", {})
            .get("features", [])
    )
    if not features:
        return {"ok": False, "error": "NOT_FOUND", "pnu": pnu}

    props = features[0].get("properties", {}) or {}

    sido = (props.get("sido_nm") or "").strip()
    sgg  = (props.get("sgg_nm") or "").strip()
    emd  = (props.get("emd_nm") or "").strip()
    ri   = (props.get("ri_nm") or "").strip()
    jibun = (props.get("jibun") or "").strip()
    rn_nm = (props.get("rn_nm") or "").strip()
    bld_mnnm = (props.get("bld_mnnm") or "").strip()
    bld_slno = (props.get("bld_slno") or "").strip()

    # 지번주소
    parts = [p for p in [sido, sgg, emd] if p]
    if ri:
        parts.append(ri)
    if jibun:
        parts.append(jibun)
    parcel_addr = " ".join(parts) if parts else ""

    # 도로명주소 (있을 때만)
    road_addr = ""
    if rn_nm and bld_mnnm:
        # 00012 -> 12
        try:
            main_no = str(int(bld_mnnm))
        except:
            main_no = bld_mnnm

        sub_no = ""
        if bld_slno and bld_slno != "00000":
            try:
                sub_no = str(int(bld_slno))
            except:
                sub_no = bld_slno

        bno = f"{main_no}-{sub_no}" if sub_no else main_no
        road_addr = " ".join([p for p in [sido, sgg, rn_nm, bno] if p])

    return {
        "ok": True,
        "pnu": pnu,
        "parcel_addr": parcel_addr,
        "road_addr": road_addr,
        "sido_nm": sido,
        "sgg_nm": sgg,
        "emd_nm": emd,
        "ri_nm": ri,
        "jibun": jibun,
        "rn_nm": rn_nm,
        "bld_mnnm": bld_mnnm,
        "bld_slno": bld_slno,
        "jimok": props.get("jimok"),
        "parea": props.get("parea"),
    }

def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "result") -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return bio.getvalue()

# ---------------------------
# Streamlit UI
# ---------------------------
st.set_page_config(page_title="PNU → 주소 변환 시연", layout="wide")
st.title("PNU → 주소 변환 (VWorld API) 시연")

with st.expander("사용 방법", expanded=True):
    st.write(
        "- 엑셀에 `PNU` 컬럼(19자리)을 넣고 업로드\n"
        "- VWorld API 키를 입력\n"
        "- 실행하면 `지번주소`, `도로명주소`가 추가된 결과를 내려받을 수 있음\n"
    )

col1, col2, col3 = st.columns([2, 2, 2])
with col1:
    api_key = st.text_input("VWorld API Key", type="password", placeholder="발급받은 키를 붙여넣기")
with col2:
    pnu_col = st.text_input("PNU 컬럼명", value="PNU")
with col3:
    domain = st.text_input("domain (선택)", value="", help="키가 도메인 제한이면 앱 도메인을 등록하고 여기에 입력")

uploaded = st.file_uploader("엑셀 업로드(.xlsx)", type=["xlsx"])

opt1, opt2, opt3 = st.columns([1, 1, 2])
with opt1:
    throttle = st.number_input("요청 간격(초)", min_value=0.0, max_value=2.0, value=0.0, step=0.05,
                               help="쿼터/차단이 걱정되면 0.1~0.2 권장")
with opt2:
    max_rows = st.number_input("최대 처리 행수", min_value=1, max_value=50000, value=2000, step=100)
with opt3:
    st.caption("팁: PNU 중복이 많으면 자동으로 캐시해서 호출 수를 줄입니다.")

run = st.button("주소 변환 실행", type="primary", disabled=not (uploaded and api_key))

if run:
    try:
        df = pd.read_excel(uploaded, dtype={pnu_col: str})
    except Exception as e:
        st.error(f"엑셀 읽기 실패: {e}")
        st.stop()

    if pnu_col not in df.columns:
        st.error(f"'{pnu_col}' 컬럼이 엑셀에 없습니다. 현재 컬럼: {list(df.columns)}")
        st.stop()

    df = df.copy()
    df[pnu_col] = df[pnu_col].astype(str).str.strip()

    if len(df) > max_rows:
        st.warning(f"행이 {len(df)}개라서 상위 {max_rows}행만 처리합니다.")
        df = df.iloc[:max_rows].copy()

    # 캐시(중복 PNU 최소화)
    cache = {}
    results = []

    progress = st.progress(0)
    status_box = st.empty()

    pnus = df[pnu_col].tolist()
    total = len(pnus)

    for i, pnu in enumerate(pnus, start=1):
        if pnu in cache:
            res = cache[pnu]
        else:
            try:
                res = pnu_to_address_vworld(pnu, api_key, domain=domain.strip() or None)
            except requests.HTTPError as e:
                res = {"ok": False, "error": f"HTTP_ERROR: {e}", "pnu": pnu}
            except Exception as e:
                res = {"ok": False, "error": f"EXCEPTION: {e}", "pnu": pnu}
            cache[pnu] = res

            if throttle and throttle > 0:
                time.sleep(float(throttle))

        results.append(res)

        if i % 10 == 0 or i == total:
            progress.progress(i / total)
            status_box.write(f"처리중... {i}/{total} (캐시 {len(cache)}개)")

    # 결과 컬럼 생성
    df["조회성공"] = [r.get("ok", False) for r in results]
    df["에러"] = [("" if r.get("ok") else str(r.get("error"))) for r in results]
    df["지번주소"] = [r.get("parcel_addr", "") if r.get("ok") else "" for r in results]
    df["도로명주소"] = [r.get("road_addr", "") if r.get("ok") else "" for r in results]

    # 구성요소(원하면 나중에 더 늘리면 됨)
    df["시도"] = [r.get("sido_nm", "") if r.get("ok") else "" for r in results]
    df["시군구"] = [r.get("sgg_nm", "") if r.get("ok") else "" for r in results]
    df["읍면동"] = [r.get("emd_nm", "") if r.get("ok") else "" for r in results]
    df["리"] = [r.get("ri_nm", "") if r.get("ok") else "" for r in results]
    df["지번"] = [r.get("jibun", "") if r.get("ok") else "" for r in results]

    ok_cnt = int(df["조회성공"].sum())
    fail_cnt = len(df) - ok_cnt

    st.success(f"완료: 성공 {ok_cnt} / 실패 {fail_cnt} (총 {len(df)}행)")
    st.subheader("미리보기")
    st.dataframe(df, use_container_width=True, height=520)

    # 다운로드
    out_bytes = to_excel_bytes(df)
    st.download_button(
        "결과 엑셀 다운로드",
        data=out_bytes,
        file_name="pnu_address_result.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
