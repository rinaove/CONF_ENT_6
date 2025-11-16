# 📘 Daily Movie Performance Dataset

한국 영화 개봉일 기준 **일일 관객수 / 매출 / 스크린 수를 생성·보정·스무딩한 데이터셋 모음**입니다.

본 저장소는 다음 세 가지 CSV 파일로 구성됩니다:

- **`Daily_Performance.csv`**
- **`Daily_Performance_adjusted.csv`**
- **`Daily_Performance_smoothed.csv`**

각 파일은 서로 **순차적으로 전처리된 결과물**입니다.

---

# 📁 1. `Daily_Performance.csv`

영화의 개봉일을 기준으로 **최대 365일간의 일일 performance 데이터를 생성한 원본 패널 데이터**입니다.

## 📌 생성 과정 요약

### ① `dataset - koreanfilms_w_data_with_actors.csv`

- 영화 메타데이터 파일
- 주요 컬럼: `movieCd`, `title`, `openDt`, `genre`, `actors`, …

### ② `kobis.csv`

- 박스오피스 기준 일일 성과 데이터
- 주요 컬럼:
    - `movieCd`, `targetDt`
    - `audiCnt` (관객수)
    - `salesAmt` (매출)
    - `scrnCnt` (스크린 수)
    - `showCnt` (상영 수)

### ③ 처리 규칙

- 두 파일을 **`movieCd` 기준 inner join**
- 두 파일에서 **모두 존재하는 영화만 선택**
- 각 영화에 대해:
    - `openDt`부터 시작
    - `min(openDt + 364일, kobis에서 관측되는 마지막 targetDt)`까지 날짜 생성
    - 해당 날짜에 kobis 값이 있으면 그대로 사용
    - 값이 없으면 **0으로 채움**

---

## 📑 컬럼 설명

| Column | Description |
| --- | --- |
| `performance_id` | 전체 데이터 기준 unique ID |
| `movie_id` | 영화 식별자 (`movieCd`) |
| `performance_date` | 관측 날짜 |
| `daily_audi_cnt` | 해당 날짜 관객수 (kobis) / 없으면 0 |
| `daily_sales_amt` | 해당 날짜 매출액 (kobis) / 없으면 0 |
| `screen_cnt` | 스크린 수 (kobis) / 없으면 0 |
| `show_cnt` | 상영 수 (`scrnCnt` 기반) / 없으면 0 |

---

# 📁 2. `Daily_Performance_adjusted.csv`

`Daily_Performance.csv`를 기반으로,

**영화 상영 기간(active 구간) 중간의 결측(0)** 값들을 **선형보간(Linear Interpolation)** 으로 채운 버전입니다.

## 📌 Active 구간 정의

```
active = (daily_audi_cnt > 0) OR
         (daily_sales_amt > 0) OR
         (screen_cnt > 0) OR
         (show_cnt > 0)

```

- **첫 양수 등장일 ~ 마지막 양수 등장일** 사이를 active 구간으로 정의
- active 구간 안에서 **네 값이 모두 0인 날**은 *데이터 누락으로 판단*
    
    → `NaN`으로 변환 후 **interpolate()** 적용
    
- active 구간 밖(상영 전/후)은 **0 그대로 유지함**

---

## 📑 컬럼

구조는 `Daily_Performance.csv`와 동일하며,

차이는 **active 구간 내 값만 보간되어 부드러운 시계열**이라는 점입니다.

---

# 📁 3. `Daily_Performance_smoothed.csv`

보간 버전(`Daily_Performance_adjusted.csv`)을 기반으로,

**상영 종료 이후 꼬리(tail) 구간을 지수감소 함수로 스무딩한 최종 버전**입니다.

> ✔ 이 파일은 시계열 분석, 수요예측, 시각화에 가장 적합합니다.
> 

---

## 📌 스무딩 로직 상세

### **1) Active 구간 내 결측값 → 선형 보간**

- `Daily_Performance_adjusted.csv`에서 이미 적용됨
- 상영 기간 중간에 끊어지는 숫자를 부드럽게 연결하는 과정

---

### **2) Tail 구간(마지막 양수 이후) → Exponential Decay 적용**

상영 기간이 끝난 이후에

`Daily_Performance.csv`에서는 모든 값이 *0*으로 이어졌으나,

이는 실제 흥행 패턴과 어긋나므로 아래 형태로 스무딩함.

마지막 양수 값을 `v0`라고 할 때:

```
v(t) = v0 * (fraction ** (t / T))

```

- **T** = tail 길이 (마지막 양수 이후 날짜 수)
- **fraction** = 0.01
    
    → tail 마지막 날에서 원래 값의 **1%**까지 자연스럽게 감소
    
- 모든 값은 **반올림 후 정수**로 변환

---

## 📌 적용되는 컬럼

- `daily_audi_cnt`
- `daily_sales_amt`
- `screen_cnt`
- `show_cnt`

---

## 🎨 결과

- 상영 종료 이후 0만 이어지던 비자연스러운 패턴이
- 실제 흥행 곡선처럼 **점진적으로 감소하는 부드러운 tail**로 변환됩니다.
