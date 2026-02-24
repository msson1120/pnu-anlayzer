# app.py
# ---------------------------------------------------------
# Streamlit 버전: VWorld PNU → 공부상면적(㎡) 조회기
# - 입력: 엑셀 업로드 (pnu 컬럼 필요)
# - 처리: PNU 정규화(19자리), (옵션) 중복 제거, VWorld 비동기 조회 + 재시도
# - 출력: 결과 엑셀 다운로드
#
# 실행:
#   pip install streamlit pandas openpyxl aiohttp
#   streamlit run app.py
# ---------------------------------------------------------

import asyncio
import random
import time
from io import BytesIO
from typing import Any, Optional, Tuple, List, Callable, Dict

import pandas as pd
import aiohttp
import streamlit as st


API_URL = "https://api.vworld.kr/ned/data/ladfrlList"  # VWorld API 128


# =========================
# Core parsing helpers
# =========================
def _find_first_key(obj: Any, key: str) -> Optional[Any]:
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = _find_first_key(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = _find_first_key(it, key)
            if found is not None:
                return found
    return None


def _get_first_record(data: Any) -> Optional[dict]:
    """
    VWorld ladfrlList 응답에서 실제 1건 레코드(dict)를 찾아 반환
    """
    if not isinstance(data, (dict, list)):
        return None

    # 보통 result/items/item 또는 ladfrlList/items/item 형태가 많음
    # 안전하게 dict/list를 모두 훑어서 dict 형태(레코드 후보)를 찾는다.
    if isinstance(data, dict):
        # 가장 흔한 경로들
        for path in [
            ("result", "items", "item"),
            ("ladfrlList", "items", "item"),
            ("response", "result", "items", "item"),
            ("response", "ladfrlList", "items", "item"),
        ]:
            cur = data
            ok = True
            for k in path:
                if isinstance(cur, dict) and k in cur:
                    cur = cur[k]
                else:
                    ok = False
                    break
            if ok:
                if isinstance(cur, list) and cur:
                    return cur[0] if isinstance(cur[0], dict) else None
                if isinstance(cur, dict):
                    return cur

    # fallback: 아무 dict 하나라도 레코드처럼 보이면 반환
    # (하지만 너무 깊게 가면 엉뚱한 dict가 잡힐 수 있어 "area 필드"가 있는 dict를 우선)
    def walk(x: Any) -> Optional[dict]:
        if isinstance(x, dict):
            if "lndpclAr" in x or "ldCodeNm" in x or "lnbrMnnm" in x:
                return x
            for v in x.values():
                r = walk(v)
                if r is not None:
                    return r
        elif isinstance(x, list):
            for it in x:
                r = walk(it)
                if r is not None:
                    return r
        return None

    return walk(data)


def _pick(rec: Optional[dict], *keys: str, default: Any = "") -> Any:
    if not isinstance(rec, dict):
        return default
    for k in keys:
        if k in rec and rec[k] is not None:
            return rec[k]
    return default


def _to_int_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return ""
    try:
        return str(int(float(s)))
    except Exception:
        # 숫자 변환 실패면 원문 유지
        return s


def _pnu_to_jibun(pnu: str) -> str:
    """
    PNU(19자리)에서 지번 추정:
    - 마지막 8자리 = 본번(4) + 부번(4)
    """
    pnu = (pnu or "").strip()
    if len(pnu) != 19 or not pnu.isdigit():
        return ""
    main_no = int(pnu[-8:-4])
    sub_no = int(pnu[-4:])
    if sub_no == 0:
        return str(main_no)
    return f"{main_no}-{sub_no}"


# =========================
# PNU normalize
# =========================
def normalize_pnu_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\s+", "", regex=True)
    s = s.str.replace(r"\.0$", "", regex=True)

    def fix_one(x: str) -> str:
        if x == "" or x.lower() == "nan":
            return ""
        if "e" in x.lower():
            try:
                x = str(int(float(x)))
            except Exception:
                return x
        if x.isdigit() and len(x) < 19:
            x = x.zfill(19)
        return x

    return s.map(fix_one)


# =========================
# Async fetch
# =========================
async def fetch_one(
    session: aiohttp.ClientSession,
    pnu: str,
    key: str,
    sem: asyncio.Semaphore,
    timeout_sec: int = 20,
    max_retries: int = 6,
) -> Tuple[str, Optional[float], str, str, str, str, str, str, str, str]:
    """
    반환 컬럼:
    PNU, 공부상면적(㎡), 법정동명, 지번, 대장구분명, 지목명, 소유구분명, 상태, 소유(공유)인수(표시), 데이터기준일자
    """
    pnu = (pnu or "").strip()
    if not (len(pnu) == 19 and pnu.isdigit()):
        return (pnu, None, "", "", "", "", "", "INVALID_PNU", "", "PNU must be 19-digit numeric")

    params = {"pnu": pnu, "key": key, "format": "json", "numOfRows": 1, "pageNo": 1}

    async with sem:
        backoff = 0.8
        last_err = ""
        for attempt in range(1, max_retries + 1):
            try:
                async with session.get(API_URL, params=params, timeout=timeout_sec) as resp:
                    if resp.status in (429, 500, 502, 503, 504):
                        txt = await resp.text()
                        last_err = f"{resp.status}: {txt[:200]}"
                        if attempt < max_retries:
                            await asyncio.sleep(min(backoff, 20) + random.uniform(0, 0.6))
                            backoff = min(backoff * 2, 30.0)
                            continue
                        return (pnu, None, "", "", "", "", "", "HTTP_ERROR", "", last_err)

                    if resp.status != 200:
                        txt = await resp.text()
                        return (pnu, None, "", "", "", "", "", "HTTP_ERROR", "", f"{resp.status}: {txt[:200]}")

                    data = await resp.json(content_type=None)
                    rec = _get_first_record(data)

                    # 면적(필수)
                    area = _pick(rec, "lndpclAr", default=None)
                    if area is None:
                        area = _find_first_key(data, "lndpclAr")
                    if area is None:
                        total = _find_first_key(data, "totalCount")
                        if str(total) == "0":
                            return (pnu, None, "", "", "", "", "", "NO_DATA", "", "totalCount=0")
                        return (pnu, None, "", "", "", "", "", "NO_DATA", "", "lndpclAr not found")

                    # 법정동명
                    bjdong_nm = _pick(rec, "ldCodeNm", "bjdongNm", "emdNm", "liNm", default="")

                    # 지번: 1) 레코드 조합 2) 없으면 PNU 복원
                    lnbr_mnnm = _pick(rec, "lnbrMnnm", "mnnm", default="")
                    lnbr_slno = _pick(rec, "lnbrSlno", "slno", default="")
                    mnnm = _to_int_str(lnbr_mnnm)
                    slno = _to_int_str(lnbr_slno)

                    if mnnm and slno and slno != "0":
                        jibun = f"{mnnm}-{slno}"
                    elif mnnm:
                        jibun = mnnm
                    else:
                        jibun = _pick(rec, "jibun", default="")

                    if not jibun:
                        jibun = _pnu_to_jibun(pnu)

                    # 대장구분명 / 지목명 / 소유구분명
                    regstr_se_nm = _pick(rec, "regstrSeCodeNm", "regstrSeNm", "regstrSe", default="")
                    lndcgr_code_nm = _pick(rec, "lndcgrCodeNm", "lndcgrNm", "jimok", default="")
                    posesn_se_nm = _pick(rec, "posesnSeCodeNm", "posesnSeNm", "posesnSe", default="")

                    # 소유(공유)인수
                    raw_cnt = _pick(
                        rec,
                        "cnrsPsnCo",
                        "crprsPsnCo",
                        "prtownCo",
                        "posesnPrtnCo",
                        "prtownCnt",
                        "co",
                        "count",
                        "ownCnt",
                        default=None,
                    )

                    posesn_cnt_text = ""
                    if raw_cnt is not None:
                        try:
                            v = int(float(str(raw_cnt).strip()))
                            if v == 0:
                                posesn_cnt_text = "단독소유"
                            elif v > 0:
                                posesn_cnt_text = f"공유 {v}인"
                            else:
                                posesn_cnt_text = ""
                        except Exception:
                            posesn_cnt_text = ""
                    else:
                        posesn_cnt_text = ""

                    # 데이터기준일자
                    last_updt_dt = _pick(rec, "lastUpdtDt", "dataStdde", "stdde", default="")
                    if last_updt_dt == "":
                        last_updt_dt = _find_first_key(data, "lastUpdtDt") or ""

                    return (
                        pnu,
                        float(area),
                        bjdong_nm,
                        jibun,
                        regstr_se_nm,
                        lndcgr_code_nm,
                        posesn_se_nm,
                        "OK",
                        posesn_cnt_text,
                        last_updt_dt,
                    )

            except asyncio.TimeoutError:
                last_err = "timeout"
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"

            if attempt < max_retries:
                await asyncio.sleep(min(backoff, 20) + random.uniform(0, 0.6))
                backoff = min(backoff * 2, 30.0)

        return (pnu, None, "", "", "", "", "", "HTTP_ERROR", "", last_err)


async def run_once(
    pnu_list: List[str],
    key: str,
    concurrency: int,
    progress_cb: Optional[Callable[[int, int, float], None]] = None,
) -> pd.DataFrame:
    sem = asyncio.Semaphore(concurrency)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Connection": "close",
        "Referer": "https://pnu-analyzer-bafef8mwydilfpbwvzvhng.streamlit.app/",
    }
    timeout = aiohttp.ClientTimeout(total=None)
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
        keepalive_timeout=5,
        enable_cleanup_closed=True,
    )

    results: List[Optional[Tuple]] = [None] * len(pnu_list)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        t0 = time.time()
        total = len(pnu_list)

        async def one(i: int, p: str):
            r = await fetch_one(session, p, key, sem)
            return i, r

        tasks = [asyncio.create_task(one(i, p)) for i, p in enumerate(pnu_list)]

        done_count = 0
        for fut in asyncio.as_completed(tasks):
            i, r = await fut
            results[i] = r
            done_count += 1
            if progress_cb:
                dt = max(time.time() - t0, 1e-6)
                rate = done_count / dt
                progress_cb(done_count, total, rate)

    return pd.DataFrame(
        results,
        columns=[
            "PNU",
            "공부상면적(㎡)",
            "법정동명",
            "지번",
            "대장구분명",
            "지목명",
            "소유구분명",
            "상태",
            "소유(공유)인수",
            "데이터기준일자",
        ],
    )


async def run_batch_with_retry(
    pnu_list: List[str],
    key: str,
    concurrency: int,
    progress_cb: Optional[Callable[[int, int, float], None]] = None,
    log_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    if log_cb:
        log_cb(f"[1차] {len(pnu_list)}건 조회 시작 (동시 {concurrency})")
    out1 = await run_once(pnu_list, key, concurrency, progress_cb)

    fail = out1[out1["상태"].isin(["HTTP_ERROR"])]["PNU"].tolist()
    if not fail:
        if log_cb:
            log_cb("[완료] 1차에서 모두 처리됨")
        return out1

    conc2 = max(5, concurrency // 2)
    if log_cb:
        log_cb(f"[2차] 1차 실패 {len(fail)}건 → 재시도(동시 {conc2})")

    out2 = await run_once(fail, key, conc2, None)

    out2_map: Dict[str, pd.Series] = {r["PNU"]: r for _, r in out2.iterrows()}
    merged_rows = []
    recovered = 0
    for _, r in out1.iterrows():
        if r["상태"] == "HTTP_ERROR" and r["PNU"] in out2_map and out2_map[r["PNU"]]["상태"] == "OK":
            merged_rows.append(out2_map[r["PNU"]])
            recovered += 1
        else:
            merged_rows.append(r)

    if log_cb:
        log_cb(f"[2차 결과] 회수 성공 {recovered}건")

    return pd.DataFrame(merged_rows)


RESULT_COLS = [
    "공부상면적(㎡)",
    "법정동명",
    "지번",
    "대장구분명",
    "지목명",
    "소유구분명",
    "상태",
    "소유(공유)인수",
    "데이터기준일자",
]


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="KH-Urban PNU 기반 분석기", layout="wide")
st.title("KH-Urban PNU 기반 분석기")

with st.sidebar:
    st.subheader("설정")
    api_key = st.text_input("VWorld API Key", type="password", placeholder="키를 입력하세요")
    concurrency = st.number_input("동시 요청 수", min_value=1, max_value=100, value=15, step=1)
    dedup = st.checkbox("중복 PNU 제거", value=True)
    st.caption("입력 엑셀에 'pnu' 컬럼이 있어야 합니다.")

uploaded = st.file_uploader("입력 엑셀 업로드 (.xlsx)", type=["xlsx"])

log_box = st.empty()
progress_bar = st.progress(0)
status_line = st.empty()

if "logs" not in st.session_state:
    st.session_state.logs = []


def add_log(msg: str):
    st.session_state.logs.append(msg)
    # 너무 길어지면 최근 300줄만 유지
    if len(st.session_state.logs) > 300:
        st.session_state.logs = st.session_state.logs[-300:]
    log_box.text("\n".join(st.session_state.logs))


def set_progress(done: int, total: int, rate: float):
    if total <= 0:
        progress_bar.progress(0)
        return
    pct = int(done / total * 100)
    progress_bar.progress(min(max(pct, 0), 100))
    status_line.text(f"진행: {done}/{total}  |  속도: {rate:.2f} 건/초")


run = st.button("실행", type="primary", disabled=(uploaded is None or not api_key))

if run:
    st.session_state.logs = []
    add_log("[시작] 실행 버튼 클릭")

    try:
        # 1) 엑셀 읽기
        df_in = pd.read_excel(uploaded)

        # 'pnu' 컬럼 체크(대소문자 대비)
        cols_lower = {c.lower(): c for c in df_in.columns}
        if "pnu" not in cols_lower:
            raise RuntimeError("엑셀에 'pnu' 컬럼이 없습니다. (컬럼명: pnu)")

        pnu_col = cols_lower["pnu"]

        # 원본 순서 보존용
        df_in = df_in.reset_index(drop=False).rename(columns={"index": "_rowid"})

        # PNU 컬럼 정규화
        df_in[pnu_col] = normalize_pnu_series(df_in[pnu_col])

        # 유효성 마스크
        valid_mask = df_in[pnu_col].astype(str).str.fullmatch(r"\d{19}")
        valid_in_order = df_in.loc[valid_mask, pnu_col].astype(str).tolist()
        invalid_in_order = df_in.loc[~valid_mask, pnu_col].astype(str).tolist()

        add_log(f"원본 행수: {len(df_in)} / 유효 PNU: {len(valid_in_order)} / INVALID: {len(invalid_in_order)}")

        # 조회 대상 리스트(중복 제거 옵션은 '조회'에만 적용)
        if dedup:
            before = len(valid_in_order)
            unique_valid = list(dict.fromkeys(valid_in_order))  # 등장 순서 유지 중복 제거
            add_log(f"조회 중복 제거: {before} → {len(unique_valid)}")
        else:
            unique_valid = valid_in_order

        if not unique_valid:
            raise RuntimeError("유효한 19자리 PNU가 없습니다.")

        progress_bar.progress(0)
        status_line.text("조회 준비중...")

        # 1) 유효 PNU만 조회
        out_valid = asyncio.run(
            run_batch_with_retry(
                unique_valid,
                api_key,
                int(concurrency),
                progress_cb=set_progress,
                log_cb=add_log,
            )
        )

        # 2) 결과를 PNU → 결과 row(dict) 맵으로 변환
        res_map = out_valid.set_index("PNU")[RESULT_COLS].to_dict(orient="index")

        # 3) 원본 df_in에 결과 컬럼 붙이기 (원본 순서/행수 그대로)
        def attach_result(pnu: str) -> dict:
            pnu = str(pnu).strip()
            if not (len(pnu) == 19 and pnu.isdigit()):
                return {
                    "공부상면적(㎡)": None,
                    "법정동명": "",
                    "지번": "",
                    "대장구분명": "",
                    "지목명": "",
                    "소유구분명": "",
                    "상태": "INVALID_PNU",
                    "소유(공유)인수": "",
                    "데이터기준일자": "PNU must be 19-digit numeric",
                }
            if pnu in res_map:
                return res_map[pnu]
            # 이 케이스는 거의 없지만 안전망(예: 예외로 누락)
            return {
                "공부상면적(㎡)": None,
                "법정동명": "",
                "지번": _pnu_to_jibun(pnu),
                "대장구분명": "",
                "지목명": "",
                "소유구분명": "",
                "상태": "NO_DATA",
                "소유(공유)인수": "",
                "데이터기준일자": "missing in result map",
            }

        attached = df_in[pnu_col].map(attach_result).apply(pd.Series)
        df_out = pd.concat([df_in, attached], axis=1)

        # 4) 원본 인덱스 기준으로 정렬(=원본 순서), _rowid 제거
        df_out = df_out.sort_values("_rowid").drop(columns=["_rowid"]).reset_index(drop=True)

        add_log("[완료] 원본 순서 유지 상태로 결과 결합 완료")

        # 이후 다운로드/미리보기는 out_all 대신 df_out 사용
        out_all = df_out

        add_log("[완료] 결과 데이터 생성 완료")
        progress_bar.progress(100)
        status_line.text("완료")

        # 4) 다운로드 제공
        buf = BytesIO()
        out_all.to_excel(buf, index=False)
        buf.seek(0)

        st.success("완료: 결과 엑셀을 다운로드하세요.")
        st.download_button(
            label="결과 엑셀 다운로드",
            data=buf,
            file_name="vworld_pnu_result.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # (옵션) 화면에 미리보기
        st.subheader("결과 미리보기")
        st.dataframe(out_all, use_container_width=True)

    except Exception as e:
        add_log(f"[오류] {type(e).__name__}: {e}")
        st.error(f"{type(e).__name__}: {e}")
