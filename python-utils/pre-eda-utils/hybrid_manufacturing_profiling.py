import pandas as pd
from openai import OpenAI
from ydata_profiling import ProfileReport
from bs4 import BeautifulSoup 
from markdown import markdown

# ======================
# 사용자 설정
# ======================
FILE_PATH = fr"filepath\snowflake-medallion-platform\python-utils\pre-eda-utils\sample_data\hybrid_manufacturing_categorical.csv"
OUTPUT_HTML = fr"filepath\snowflake-medallion-platform\python-utils\pre-eda-utils\result\hybrid_manufacturing_categorical.html"

# OpenAI API 키
USE_OPENAI_TYPE_RECOMMENDER = True
client = OpenAI(
api_key="my api key"
)

# ======================
# 기본 데이터 로드
# ======================
df = pd.read_csv(FILE_PATH)

# ======================
# 기본 프로파일링 (결측치, 이상값 등)
# ======================
profile = ProfileReport(df, title="hybrid_manufacturing_categorical Report", explorative=True)
profile_html = profile.to_html()
# print(f"1차 프로파일링 리포트 생성 완료: {OUTPUT_HTML}")

# ======================
# 컬럼별 고유값 미리보기
# ======================
unique_summary = {}
for col in df.columns:
    # 상위 20개만
    unique_summary[col] = df[col].dropna().unique().tolist()[:20]

# ======================
# 데이터 타입 추천 (OpenAI API 사용)
# ======================
# 데이터 출처 정보를 추가로 입력받음
DATA_SOURCE_INFO = """
This dataset captures production planning and optimization data from Hybrid Manufacturing Systems (HMS), which integrate additive and subtractive manufacturing processes. It includes detailed records of machine operations, material usage, processing time, energy consumption, and scheduling data to facilitate efficient resource allocation and decision-making.

The dataset also introduces a categorical target variable (Optimization_Category), which classifies production efficiency into four levels:

Low Efficiency (0-40) → High delays, failures.
Moderate Efficiency (41-70) → Acceptable but suboptimal.
High Efficiency (71-90) → Well-optimized but with minor inefficiencies.
Optimal Efficiency (91-100) → Best-case scenario, minimal delays.

About this file
This file contains structured data from Hybrid Manufacturing Systems (HMS), focusing on job scheduling, resource allocation, and efficiency optimization. Each record represents a manufacturing job, including details such as:

Machine Operations (Milling, Drilling, Additive, etc.)
Material Usage & Processing Time
Energy Consumption & Machine Availability
Scheduled vs. Actual Job Timings
Job Completion Status (Completed, Delayed, Failed)
Categorical Efficiency Score (Low, Moderate, High, Optimal)
"""

def generate_data_structure_summary(df):
    """CSV의 컬럼과 샘플 데이터를 기반으로 데이터 구조 요약(Markdown 표 형태)을 자동 생성"""
    info_summary = df.dtypes.to_dict()
    # 대표 샘플 5개 추출
    sample_df = df.sample(min(5, len(df)))

    prompt = f"""
    당신은 데이터 분석 전문가입니다.
    아래는 CSV 데이터의 컬럼 정보와 일부 샘플입니다.
    각 컬럼의 의미를 추론하여 '데이터 구조 요약'을 작성하세요.

    - 출력 형식 예시:
    Job_ID : 작업 식별자  
    Machine_ID : 해당 작업이 수행된 기계  
    Operation_Type : 공정 유형  

    모든 설명은 자연스러운 한글로 작성하세요.

    ### 컬럼 정보:
    {info_summary}

    ### 샘플 데이터:
    {sample_df.to_markdown()}
    """

    try:
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=[
                {"role": "system", "content": "당신은 데이터 구조를 해석하는 데이터 분석 전문가입니다."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )
        return completion.choices[0].message.content.strip()

    except Exception as e:
        return f"Error: {e}"

# 실행
data_structure_md = generate_data_structure_summary(df)
print(" data_structure_md 생성 완료\n")

def recommend_dtype(column_name, sample_values, source_info, data_structure_md):
    prompt = f"""
    ### 데이터셋 문맥
    {source_info}

    ### 데이터 구조 요약
    {data_structure_md}

    ### 분석 대상 컬럼
    컬럼명: {column_name}
    샘플 값: {sample_values[:10]}

    ### 가이드라인
    - 위의 데이터 구조 요약과 문맥을 고려하여, 이 컬럼에 가장 어울리는 데이터 타입을 제시하세요.
    - 반드시 SQL 또는 Snowflake에서 사용 가능한 실제 데이터 타입으로만 제시하세요.
    - 답변은 **설명 없이 데이터 타입 한 줄만 출력**해야 합니다.
    - 예시: VARCHAR(100), NUMBER(38,0), FLOAT, TIMESTAMP_NTZ, BOOLEAN
    - 날짜/시간 관련 값 → TIMESTAMP_NTZ
    - 범주형 텍스트 → VARCHAR(길이)
    - 실수/소수 포함 숫자 → FLOAT
    - 정수형 숫자 → NUMBER(38,0)
    """
    try:
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=[
                    {"role": "system", "content": f"당신은 숙련된 스노우플레이크(Snowflake) 데이터 아키텍트입니다. context: {source_info}"},
                    {"role": "user", "content": prompt}
                ],
            temperature=0
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {e}"

dtype_recommendations = {}
if USE_OPENAI_TYPE_RECOMMENDER:
    for col in df.columns:
        sample_values = df[col].dropna().astype(str).tolist()[:10]
        dtype_recommendations[col] = recommend_dtype(col, sample_values, DATA_SOURCE_INFO, data_structure_md)
else:
    dtype_recommendations = {col: str(df[col].dtype) for col in df.columns}

# ======================
# 5HTML 통합 리포트 생성
# ======================
soup = BeautifulSoup(profile_html, "html.parser")

# 유니크값 섹션 HTML 변환
unique_html = "<h2>2. 컬럼별 유니크값 미리보기</h2><table border='1' cellspacing='0' cellpadding='4'><tr><th>컬럼명</th><th>유니크값</th></tr>"
for col, vals in unique_summary.items():
    unique_html += f"<tr><td>{col}</td><td>{vals}</td></tr>"
unique_html += "</table><hr>"

# 타입 추천 테이블 HTML 변환
dtype_html = "<h2>3. 컬럼별 추천 데이터 타입</h2><table border='1' cellspacing='0' cellpadding='4'><tr><th>컬럼명</th><th>추천 데이터 타입</th></tr>"
for col, dtype in dtype_recommendations.items():
    dtype_html += f"<tr><td>{col}</td><td>{dtype}</td></tr>"
dtype_html += "</table><hr>"

# AI 요약 및 병합
custom_html = f"""
<h1>1. 데이터 구조 요약</h1>
{markdown(data_structure_md)}
{unique_html}
{dtype_html}
"""

soup.body.insert(0, BeautifulSoup(custom_html, "html.parser"))

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(str(soup))

print(f"통합 리포트 생성 완료: {OUTPUT_HTML}")