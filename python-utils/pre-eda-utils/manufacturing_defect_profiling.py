import pandas as pd
from openai import OpenAI
from ydata_profiling import ProfileReport
from bs4 import BeautifulSoup 
from markdown import markdown

# ======================
# 사용자 설정
# ======================
FILE_PATH = fr"filepath\snowflake-medallion-platform\python-utils\pre-eda-utils\sample_data\manufacturing_defect_dataset.csv"
OUTPUT_HTML = fr"filepath\snowflake-medallion-platform\python-utils\pre-eda-utils\result\manufacturing_defect_dataset.html"

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
profile = ProfileReport(df, title="manufacturing_defect_dataset Report", explorative=True)
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
Introduction
This dataset provides insights into factors influencing defect rates in a manufacturing environment. Each record represents various metrics crucial for predicting high or low defect occurrences in production processes.

Variables Description
Production Metrics
ProductionVolume: Number of units produced per day.

Data Type: Integer.
Range: 100 to 1000 units/day.
ProductionCost: Cost incurred for production per day.
Data Type: Float.
Range: $5000 to $20000.
Supply Chain and Logistics
SupplierQuality: Quality ratings of suppliers.

Data Type: Float (%).
Range: 80% to 100%.
DeliveryDelay: Average delay in delivery.
Data Type: Integer (days).
Range: 0 to 5 days.
Quality Control and Defect Rates
DefectRate: Defects per thousand units produced.

Data Type: Float.
Range: 0.5 to 5.0 defects.
QualityScore: Overall quality assessment.
Data Type: Float (%).
Range: 60% to 100%.
Maintenance and Downtime
MaintenanceHours: Hours spent on maintenance per week.

Data Type: Integer.
Range: 0 to 24 hours.
DowntimePercentage: Percentage of production downtime.
Data Type: Float (%).
Range: 0% to 5%.
Inventory Management
InventoryTurnover: Ratio of inventory turnover.

Data Type: Float.
Range: 2 to 10.
StockoutRate: Rate of inventory stockouts.
Data Type: Float (%).
Range: 0% to 10%.
Workforce Productivity and Safety
WorkerProductivity: Productivity level of the workforce.

Data Type: Float (%).
Range: 80% to 100%.
SafetyIncidents: Number of safety incidents per month.
Data Type: Integer.
Range: 0 to 10 incidents.
Energy Consumption and Efficiency
EnergyConsumption: Energy consumed in kWh.

Data Type: Float.
Range: 1000 to 5000 kWh.
EnergyEfficiency: Efficiency factor of energy usage.
Data Type: Float.
Range: 0.1 to 0.5.
Additive Manufacturing
AdditiveProcessTime: Time taken for additive manufacturing.

Data Type: Float (hours).
Range: 1 to 10 hours.
AdditiveMaterialCost: Cost of additive materials per unit.
Data Type: Float ($).
Range: $100 to $500.
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
    ProductionVolume : 생산량 
    ProductionCost : 생산 비용 
    SupplierQuality : 공급 업체 품질

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