import pandas as pd
import re

# CSV 파일 로드
df = pd.read_csv(fr"filepath\snowflake-medallion-platform\python-utils\append_data\sample_data\hybrid_manufacturing_categorical.csv")

# 복제하고 싶은 데이터 개수
num_new_rows = 1600

# PK 컬럼명
pk_col = "Job_ID" 

# ----------------------------------------------------
# 문자열 PK(J1000 형태)의 숫자 부분 +1 증가 함수
# ----------------------------------------------------
def increment_pk(pk):
    # 문자(prefix) + 숫자 부분 분리
    prefix = re.match(r"[A-Za-z]+", pk).group()
    number = re.match(r"[A-Za-z]+(\d+)", pk).group(1)

    # 숫자 증가
    new_number = str(int(number) + 1).zfill(len(number))

    return prefix + new_number

# ----------------------------------------------------
# 현재 CSV에서 가장 큰 PK 찾기
# ----------------------------------------------------
# 숫자만 추출해서 정렬
df["_num"] = df[pk_col].str.extract(r'(\d+)').astype(int)

# 숫자 기준 최댓값 row
max_row = df.loc[df["_num"].idxmax()]
current_max_pk = max_row[pk_col]

# 임시 컬럼 제거
df = df.drop(columns=["_num"]) 

print("현재 최대 PK:", current_max_pk)

# ----------------------------------------------------
# 새로운 랜덤 행 생성 + PK 증가
# ----------------------------------------------------
new_rows = []
next_pk = current_max_pk

for i in range(num_new_rows):
    # 기존 데이터에서 랜덤 한 행 선택
    row = df.sample(n=1).iloc[0].copy()

    # PK 증가
    next_pk = increment_pk(next_pk)
    row[pk_col] = next_pk

    new_rows.append(row)

# ----------------------------------------------------
# 기존 + 신규 데이터 합치기
# ----------------------------------------------------
df_new = pd.DataFrame(new_rows)
df_final = pd.concat([df, df_new], ignore_index=True)

# CSV 저장
df_final.to_csv(fr"filepath\snowflake-medallion-platform\python-utils\append_data\result\hybrid_manufacturing_categorical.csv", index=False)

print("완료! hybrid_manufacturing_categorical.csv 파일이 생성되었습니다.")