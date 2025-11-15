# /Users/lina/Desktop/데이터/collect_online_buzz_timeseries.py

import os
import json
import time
import math
import datetime as dt
import pandas as pd
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from collections import defaultdict

# =======================
# 1) .env 로드 (NAVER 키)
# =======================
load_dotenv()
CID  = os.getenv("NAVER_CLIENT_ID")
CSEC = os.getenv("NAVER_CLIENT_SECRET")
if not CID or not CSEC:
    raise RuntimeError("❌ NAVER_CLIENT_ID / NAVER_CLIENT_SECRET이 .env에 없습니다.")

# =======================
# 2) 경로/입력 파일
# =======================
DATA_DIR = "/Users/lina/Desktop/데이터"
OUTPUT_PATH = os.path.join(DATA_DIR, "Online_Buzz.csv")

BOX_FILES = ["koreanfilms.csv"]  # ✅ 새 CSV 하나만 사용

# 네이버 데이터랩 사양
MAX_GROUPS_PER_CALL = 5
DATA_LBOUND = dt.date(2016, 1, 1)
TODAY = dt.date.today()

# =======================
# 3) CSV 로드 (조인 없음)
# =======================
def load_films() -> pd.DataFrame:
    path = os.path.join(DATA_DIR, BOX_FILES[0])
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    df = pd.read_csv(path)

    # openDt: "YYYYMMDD" 형식 → datetime
    # (이미 2023 이후 필터된 파일이지만, 한 번 더 안전하게 처리)
    df["open_dt"] = pd.to_datetime(df["openDt"].astype(str), format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["open_dt"])
    df = df[df["open_dt"] >= pd.Timestamp("2023-01-01")].copy()

    # movie_nm = title
    df["movie_nm"] = df["title"].astype(str)

    # 최소 컬럼만 사용
    df = df[["movieCd", "movie_nm", "open_dt"]].reset_index(drop=True)

    print(f"🎬 대상 영화 수(개봉일 존재): {len(df)}")
    print("🔎 샘플 5행:")
    print(df.head())

    return df

def clean_title(t: str) -> str:
    # 검색어로 쓸 제목 정리 (특수문자 조금만 정리)
    return str(t).replace("/", " ").replace(":", " ").strip()

# =======================
# 4) 네이버 데이터랩 API - 배치 호출 + 백오프
# =======================
def datalab_search_batch(keyword_groups, start, end, max_retries=5):
    """
    keyword_groups: [{"groupName": "영화명", "keywords": ["영화명"]}, ...]  (len <= 5)
    start, end: "YYYY-MM-DD"
    """
    url = "https://openapi.naver.com/v1/datalab/search"
    headers = {
        "X-Naver-Client-Id": CID,
        "X-Naver-Client-Secret": CSEC,
        "Content-Type": "application/json; charset=UTF-8",
    }
    payload = {
        "startDate": start,
        "endDate": end,
        "timeUnit": "date",
        "keywordGroups": keyword_groups,
        "device": "", "gender": "", "ages": [],
    }

    backoff = 1.0
    for attempt in range(max_retries + 1):
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            wait_sec = float(ra) if ra else backoff
            print(f"⏳ 429: {wait_sec:.1f}s 대기 후 재시도 (시도 {attempt+1}/{max_retries}) | 기간={start}~{end}")
            time.sleep(wait_sec)
            backoff = min(backoff * 2, 30)
            continue

        # 그 외 에러 → 재시도 or 실패
        try:
            resp.raise_for_status()
        except Exception as e:
            if attempt >= max_retries:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    resp.raise_for_status()

def extract_series_list(js):
    """
    반환: [ {period: ratio, ...}, ... ]
    keywordGroups 순서와 동일
    """
    results = js.get("results", [])
    out = []
    for res in results:
        data = res.get("data", [])
        out.append({row["period"]: int(round(float(row["ratio"]))) for row in data})
    return out

# =======================
# 5) 메인 로직
# =======================
def main():
    films = load_films()

    # ✅ Resume 기능: 이미 수집된 buzz_id는 스킵
    existing_ids = set()
    if os.path.exists(OUTPUT_PATH):
        try:
            prev = pd.read_csv(OUTPUT_PATH)
            if {"buzz_id", "movieCd", "movie_nm", "buzz_date", "search_buzz_vol"}.issubset(prev.columns):
                existing_ids = set(prev["buzz_id"].astype(str).tolist())
                print(f"♻️ 기존 수집 건수: {len(existing_ids)} (buzz_id)")
        except Exception as e:
            print(f"기존 결과 읽기 실패(무시하고 새로 작성): {e}")

    # 영화별 기간 계산 & (start,end) 버킷으로 묶기
    buckets = defaultdict(list)  # {(start,end): [(movieCd, movie_nm), ...]}

    for r in films.itertuples(index=False):
        odt = r.open_dt.date() if isinstance(r.open_dt, pd.Timestamp) else r.open_dt

        # 개봉일 기준 D0 ~ min(D+365, 오늘)
        start_date = max(odt, DATA_LBOUND)
        end_date   = min(odt + dt.timedelta(days=365), TODAY)
        if end_date < start_date:
            # 아직 개봉 전이거나 이상한 데이터
            # print(f"⏭️ 아직 개봉 전: {r.movie_nm} ({r.movieCd}) start={start_date}, end={end_date}")
            continue

        start = start_date.strftime("%Y-%m-%d")
        end   = end_date.strftime("%Y-%m-%d")
        buckets[(start, end)].append((str(r.movieCd), clean_title(r.movie_nm)))

    # 전체 배치 수 계산 (progress bar용)
    total_batches = sum(math.ceil(len(v) / MAX_GROUPS_PER_CALL) for v in buckets.values())
    pbar = tqdm(total=total_batches, desc="Batch Calls")

    rows = []

    # 버킷별 & 5개씩 잘라서 배치 호출
    for (start, end), items in buckets.items():
        for i in range(0, len(items), MAX_GROUPS_PER_CALL):
            chunk = items[i:i + MAX_GROUPS_PER_CALL]
            groups = [{"groupName": t, "keywords": [t]} for (_, t) in chunk]

            try:
                js = datalab_search_batch(groups, start, end, max_retries=6)
                series_list = extract_series_list(js)

                for (movieCd, title), series in zip(chunk, series_list):
                    for d, val in series.items():
                        buzz_id = f"{movieCd}_{d}"
                        if buzz_id in existing_ids:
                            continue
                        rows.append({
                            "buzz_id": buzz_id,          # PK
                            "movieCd": movieCd,          # 코드
                            "movie_nm": title,           # 제목 (= title)
                            "buzz_date": d,              # 날짜 (있으면 분석 편함)
                            "search_buzz_vol": val       # 0~100
                        })
                time.sleep(0.8)  # 배치라 콜 수 적으니 여유만 줌

            except requests.HTTPError as e:
                print(f"⚠️ 배치 오류: {e} | 기간={start}~{end} | 샘플={chunk[0][1]}")
                time.sleep(2.0)
            except Exception as e:
                print(f"⚠️ 예외: {e} | 기간={start}~{end}")
                time.sleep(1.0)
            finally:
                pbar.update(1)

    pbar.close()

    # =======================
    # 6) 저장
    # =======================
    if rows:
        new_df = pd.DataFrame(rows)
        new_df = new_df.drop_duplicates(subset=["buzz_id"])

        # 최종 컬럼 순서: buzz_id, movieCd, movie_nm, search_buzz_vol (+ buzz_date 보너스)
        cols = ["buzz_id", "movieCd", "movie_nm", "buzz_date", "search_buzz_vol"]
        new_df = new_df[cols]

        # 기존 파일 있으면 합치기
        if os.path.exists(OUTPUT_PATH):
            try:
                prev = pd.read_csv(OUTPUT_PATH)
                df = pd.concat([prev, new_df], ignore_index=True)
                df = df.drop_duplicates(subset=["buzz_id"])
            except Exception:
                df = new_df.copy()
        else:
            df = new_df.copy()

        df = df.sort_values(["movieCd", "buzz_date"])
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print(f"✅ 저장 완료: {OUTPUT_PATH}")
        print(df.head())
        print(df.tail())
    else:
        print("❌ 새로 저장할 데이터가 없습니다.")

if __name__ == "__main__":
    main()
