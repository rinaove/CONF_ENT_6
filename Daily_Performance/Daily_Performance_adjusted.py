import pandas as pd
import os

DATA_DIR = "/Users/lina/Desktop/데이터"

input_path = os.path.join(DATA_DIR, "Daily_Performance.csv")
output_path = os.path.join(DATA_DIR, "Daily_Performance_adjusted.csv")

df = pd.read_csv(input_path, parse_dates=["performance_date"])

# 보정할 컬럼들
value_cols = ["daily_audi_cnt", "daily_sales_amt", "screen_cnt", "show_cnt"]

def adjust_series(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("performance_date").copy()

    # 이 영화에서 한 번이라도 양수가 나온 날짜들
    positive_mask = (g[value_cols] > 0).any(axis=1)

    if not positive_mask.any():
        # 전부 0이면 손댈 게 없음
        return g

    # active 구간: 첫 양수 ~ 마지막 양수
    first_idx = positive_mask.idxmax()               # 첫 True의 index
    last_idx = positive_mask[::-1].idxmax()          # 뒤에서부터 첫 True의 index

    # active 구간 mask
    active_mask = (g.index >= first_idx) & (g.index <= last_idx)

    # active 구간에서 "전부 0인 날" → NaN으로 바꿔서 보간 대상으로 만든다
    zero_inside_active = active_mask & (g[value_cols].sum(axis=1) == 0)
    g.loc[zero_inside_active, value_cols] = float("nan")

    # 보간 (선형). 날짜 기준으로 연속이라 그냥 interpolate()면 충분
    for col in value_cols:
        g[col] = g[col].interpolate(method="linear")

    # 여전히 NaN인 곳(머리/꼬리 포함)은 0으로 채워서 마무리
    for col in value_cols:
        g[col] = g[col].fillna(0)

        # 관객수, 매출, 스크린/상영 수는 정수로
        g[col] = g[col].round().astype(int)

    return g

# movie_id 단위로 보정 함수 적용
df_adj = df.groupby("movie_id", group_keys=False).apply(adjust_series)

# performance_id 다시 매겨도 되고, 기존 것 유지해도 됨
df_adj = df_adj.sort_values(["movie_id", "performance_date"]).reset_index(drop=True)
df_adj["performance_id"] = range(1, len(df_adj) + 1)

df_adj.to_csv(output_path, index=False, encoding="utf-8-sig")

print("✅ 보정 완료, 저장 위치:", output_path)
print("총 row 수:", len(df_adj))
print("고유 movie_id 수:", df_adj["movie_id"].nunique())
