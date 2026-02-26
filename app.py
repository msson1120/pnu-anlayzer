import asyncio
import io
import json
import random
import socket
import threading
import time
from typing import Any, Optional, Tuple, List, Callable

import pandas as pd
import aiohttp
import streamlit as st


API_URL = "https://api.vworld.kr/ned/data/ladfrlList"  # VWorld API 128


# =========================
# Core logic (VWorld)  ✅ 원본 로직 유지
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
    (구조가 ladfrlVOList 안에 ladfrlVOList가 또 있는 형태가 많음)
    """
    try:
        a = data.get("ladfrlVOList")
        if isinstance(a, dict):
            b = a.get("ladfrlVOList")
            if isinstance(b, list) and b and isinstance(b[0], dict):
                return b[0]
        if isinstance(a, list) and a and isinstance(a[0], dict):
            return a[0]
    except Exception:
        pass

    def walk(x: Any) -> Optional[dict]:
        if isinstance(x, dict):
            for v in x.values():
                r = walk(v)
                if r is not None:
                    return r
        elif isinstance(x, list):
            for it in x:
                if isinstance(it, dict):
                    return it
                r = walk(it)
                if r is not None:
                    return r
        return None

    return walk(data)


def _pick(d: Optional[dict], *keys: str, default="") -> Any:
    """레코드 dict에서 keys 후보 중 첫 유효 값을 반환"""
    if not isinstance(d, dict):
        return default
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        if isinstance(v, str):
            # 공백 문자열은 무효로 보되, "0"은 유효값이므로 유지
            if v.strip() == "":
                continue
            return v.strip()
        return v
    return default


def _to_int_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s == "":
        return ""
    if s.isdigit():
        return str(int(s))
    return s


def _pnu_to_jibun(pnu: str) -> str:
    """
    PNU(19자리)로 지번 복원:
    - pnu[10] : 산 여부(1이면 산)
    - pnu[11:15] : 본번(4자리)
    - pnu[15:19] : 부번(4자리)
    """
    pnu = (pnu or "").strip()
    if len(pnu) != 19 or not pnu.isdigit():
        return ""

    is_mountain = (pnu[10] == "0")  # 0이면 산, 1이면 일반
    mnnm = int(pnu[11:15])
    slno = int(pnu[15:19])

    if slno == 0:
        base = f"{mnnm}"
    else:
        base = f"{mnnm}-{slno}"

    # 산 표기 포함
    if is_mountain:
        return f"산{base}"
    return base


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
                await asyncio.sleep(random.uniform(0.05, 0.25))
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

                    # 소유(공유)인수 → 표시 문자열로 변환
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
                        default=None
                    )

                    if raw_cnt is None and isinstance(rec, dict):
                        for k in rec.keys():
                            k_lower = k.lower()
                            if any(word in k_lower for word in ["co", "cnt", "count", "인수", "소유", "prtn", "psn"]):
                                try:
                                    val = rec[k]
                                    if val is not None and str(val).strip():
                                        raw_cnt = val
                                        break
                                except Exception:
                                    continue

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
            except aiohttp.ClientError as e:
                last_err = f"client_error: {e}"
            except json.JSONDecodeError:
                last_err = "json_decode_error"
            except Exception as e:
                last_err = f"unknown_error: {type(e).__name__}: {e}"

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

    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*", "Connection": "close"}
    timeout = aiohttp.ClientTimeout(total=None)
    connector = aiohttp.TCPConnector(
        family=socket.AF_INET,            # ✅ IPv4 강제 (Streamlit에서 중요)
        limit=concurrency,
        limit_per_host=1,                 # ✅ 단건이면 1로 고정해도 됨
        ttl_dns_cache=300,
        force_close=True,                 # ✅ keep-alive 끊김 이슈 회피
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        tasks = [fetch_one(session, pnu, key, sem) for pnu in pnu_list]

        total = len(tasks)
        t0 = time.time()

        results = await asyncio.gather(*tasks)

        if progress_cb:
            dt = max(time.time() - t0, 1e-6)
            rate = total / dt
            progress_cb(total, total, rate)

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

    conc2 = max(3, concurrency // 2)
    if log_cb:
        log_cb(f"[2차] 1차 실패 {len(fail)}건 → 재시도(동시 {conc2})")

    out2 = await run_once(fail, key, conc2, None)

    out2_map = {r["PNU"]: r for _, r in out2.iterrows()}
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


# =========================
# Streamlit UI  ✅ GUI만 교체
# =========================
st.set_page_config(page_title="VWorld PNU → 공부상면적(㎡) 조회기", layout="wide")
st.title("VWorld PNU → 공부상면적(㎡) 조회기 (Streamlit)")

with st.sidebar:
    st.header("설정")
    api_key = st.text_input("VWorld API Key", type="password")
    concurrency = st.number_input("동시요청(Concurrency)", min_value=1, max_value=60, value=8, step=1)
    dedup = st.checkbox("중복 PNU 제거(dedup)", value=True)

uploaded = st.file_uploader("입력 엑셀(.xlsx) 업로드 (pnu 컬럼 필요)", type=["xlsx"])

if "logs" not in st.session_state:
    st.session_state["logs"] = []

if "running" not in st.session_state:
    st.session_state["running"] = False

if "result_df" not in st.session_state:
    st.session_state["result_df"] = None

log_placeholder = st.empty()
prog = st.progress(0)
status = st.empty()

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    st.session_state["logs"].append(f"[{ts}] {msg}")
    # 너무 길어지면 최근 300줄만 표시
    log_placeholder.text("\n".join(st.session_state["logs"][-300:]))

def progress_cb(done: int, total: int, rate: float):
    total = max(total, 1)
    frac = min(max(done / total, 0.0), 1.0)
    prog.progress(frac)
    status.info(f"{done}/{total} | {rate:.2f} req/s")

run_btn = st.button("실행", type="primary", disabled=(uploaded is None or not api_key))

if run_btn and not st.session_state["running"]:
    try:
        st.session_state["logs"] = []
        log("실행 시작")

        df = pd.read_excel(uploaded)
        if "pnu" not in df.columns:
            raise RuntimeError("엑셀에 'pnu' 컬럼이 없습니다.")

        df["pnu"] = normalize_pnu_series(df["pnu"])

        valid_mask = df["pnu"].astype(str).str.fullmatch(r"\d{19}")
        valid = df.loc[valid_mask, "pnu"].astype(str).tolist()
        invalid = df.loc[~valid_mask, "pnu"].astype(str).tolist()

        if dedup:
            before = len(valid)
            valid = list(dict.fromkeys(valid))
            log(f"중복 제거: {before} → {len(valid)}")

        log(f"유효 PNU: {len(valid)}건 / INVALID: {len(invalid)}건")
        if not valid:
            raise RuntimeError("유효한 19자리 PNU가 없습니다.")

        st.session_state["running"] = True

        def run_job(valid, invalid, api_key, concurrency):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                out_valid = loop.run_until_complete(
                    run_batch_with_retry(
                        valid,
                        api_key,
                        concurrency,
                        progress_cb=progress_cb,
                        log_cb=log,
                    )
                )

                if invalid:
                    out_invalid = pd.DataFrame(
                        [(p, None, "", "", "", "", "", "INVALID_PNU", "", "PNU must be 19-digit numeric") for p in invalid],
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
                    out_all = pd.concat([out_valid, out_invalid], ignore_index=True)
                else:
                    out_all = out_valid

                st.session_state["result_df"] = out_all

            finally:
                loop.close()
                st.session_state["running"] = False

        thread = threading.Thread(
            target=run_job,
            args=(valid, invalid, api_key, int(concurrency)),
            daemon=True,
        )
        thread.start()

    except Exception as e:
        st.error(f"{type(e).__name__}: {e}")
        log(f"오류: {type(e).__name__}: {e}")

if st.session_state["result_df"] is not None:
    st.success("완료")
    st.dataframe(st.session_state["result_df"], use_container_width=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        st.session_state["result_df"].to_excel(writer, index=False)
    buf.seek(0)

    st.download_button(
        "결과 엑셀 다운로드",
        data=buf,
        file_name="vworld_pnu_result.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
