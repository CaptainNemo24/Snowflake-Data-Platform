-- =====================================================
-- Silver Layer (Medallion Architecture)
--
-- 정의:
--  - Bronze Layer에 적재된 RAW 데이터를 기반으로
--    분석과 활용이 가능하도록 구조를 정제하는 단계
--
-- 특징:
--  - 컬럼명 표준화 및 데이터 타입 정합성 확보
--  - NOT NULL, 범위 조건 등 품질 규칙 적용
--  - JSON 데이터의 구조화 (FLATTEN 등 변환 처리)
--
-- 설계 원칙:
--  - 기존 Bronze 데이터뿐만 아니라
--    추후 유입될 데이터까지 고려한 유연한 구조
--  - 일관된 컬럼 규칙을 통해 확장성과 유지보수성 확보
--
-- 본 SQL의 역할:
--  - Bronze → Silver 데이터 정제 및 변환
--  - 품질 검증 규칙 적용 후 CLEANED / TRANSFORMED 테이블 생성
-- =====================================================

-- SILVER DATABASE 생성 및 사용
CREATE DATABASE SILVER_DB;
USE DATABASE SILVER_DB;

-- SILVER SCHEMA 생성 및 사용
CREATE SCHEMA CLEANED_TRANSFORMED;
USE SCHEMA CLEANED_TRANSFORMED;


-- HYBRID_MANUFACTURING_CLEAN
-- 적층 및 절삭 가공 공정을 통합한 하이브리드 제조 시스템(HMS)의 생산 계획 및 최적화 데이터로 구성된 테이블
-- =====================================================
-- HYBRID_MANUFACTURING_CLEAN 테이블 생성 기준 및 컬럼 설계 규칙
--
-- [식별자 제약]
--  - Job_ID, Machine_ID는 레코드 식별자 역할을 하므로 NOT NULL 제약 적용
--
-- [컬럼명 표준화]
--  - 시간 컬럼은
--    TIMESTAMP 타입 기준으로 명확한 의미를 갖도록 `_TS` 접미사 부여
--    (예: Scheduled_Start → Scheduled_Start_TS)
--
-- [컬럼 유지 정책]
--  - 시간 컬럼을 제외한 나머지 컬럼은 원본 문서에 단위/범위 정의가 불명확하므로
--    Bronze 테이블의 컬럼명 및 데이터 타입을 그대로 유지
--
-- [테이블 생성 방식]
--  - Snowflake의 CTAS(CREATE TABLE AS SELECT)는
--    SELECT 결과의 스키마만 복사하며
--    NOT NULL 등 명시적 제약 조건은 자동으로 계승하지 않음
--  - 따라서 본 테이블은 제약 조건을 명시적으로 정의하여 생성
-- =====================================================
CREATE OR REPLACE TABLE SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN (
    Job_ID                  VARCHAR(10)       NOT NULL,     -- 작업 식별자
    Machine_ID              VARCHAR(10)       NOT NULL,     -- 해당 작업이 수행된 기계의 식별자
    Operation_Type          VARCHAR(100),                   -- 공정 유형
    Material_Used           NUMBER(10,2),                   -- 사용된 재료의 양
    Processing_Time         NUMBER(10,0),                   -- 작업 처리 시간
    Energy_Consumption      NUMBER(10,2),                   -- 에너지 소비량
    Machine_Availability    NUMBER(10,0),                   -- 기계 가용성
    Scheduled_Start_TS      TIMESTAMP_NTZ,                  -- 예정 시작 시간
    Scheduled_End_TS        TIMESTAMP_NTZ,                  -- 예정 종료 시간
    Actual_Start_TS         TIMESTAMP_NTZ,                  -- 실제 시작 시간
    Actual_End_TS           TIMESTAMP_NTZ,                  -- 실제 종료 시간
    Job_Status              VARCHAR(100),                   -- 작업 상태
    Optimization_Category   VARCHAR(100)                    -- 최적화 범주
)
AS
WITH base AS (
    SELECT
        Job_ID,
        Machine_ID,
        Operation_Type,
        Material_Used,
        Processing_Time,
        Energy_Consumption,
        Machine_Availability,
        Scheduled_Start,
        Scheduled_End,
        Actual_Start,
        Actual_End,
        Job_Status,
        Optimization_Category
    FROM BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL
    WHERE Job_ID IS NOT NULL
      AND Machine_ID IS NOT NULL
)
SELECT
    Job_ID,
    Machine_ID,
    Operation_Type,
    Material_Used,
    Processing_Time,
    Energy_Consumption,
    Machine_Availability,
    Scheduled_Start   AS Scheduled_Start_TS,
    Scheduled_End     AS Scheduled_End_TS,
    Actual_Start      AS Actual_Start_TS,
    Actual_End        AS Actual_End_TS,
    Job_Status,
    Optimization_Category
FROM base;

-- MANUFACTURING_DEFECT_CLEAN
-- 제조 환경에서 불량률에 영향을 미치는 요인에 대한 예측 데이터로 구성된 테이블
-- =====================================================
-- MANUFACTURING_DEFECT_CLEAN 테이블 생성 기준 및 컬럼 설계 규칙
--
-- [식별자 제약]
--  - PRODUCTION_ID는 레코드 식별자 역할을 하므로 NOT NULL 제약 적용
--
-- 컬럼명 표준화 기준 (단위 기반 네이밍)
--
-- [설계 배경]
--  - 데이터 출처(Kaggle)에 각 컬럼의 데이터 범위 및 단위가
--    명확하게 정의되어 있으므로, 해당 정보를 컬럼명에 반영
--
-- [컬럼명 원칙]
--  - 숫자형 컬럼은 의미가 드러나도록 단위를 접미사로 명시
--  - 컬럼명만 보고도 값의 범위 및 단위를 유추할 수 있도록 표준화
--
-- [컬럼명 표준화 예시]
--  - PRODUCTION_COST        → PRODUCTION_COST_USD
--    (Kaggle 명시 범위: $5,000 ~ $20,000)
--
--  - SUPPLIER_QUALITY       → SUPPLIER_QUALITY_PERCENT
--    (Kaggle 명시 범위: 80% ~ 100%)
--
--  - DELIVERY_DELAY         → DELIVERY_DELAY_DAYS
--    (Kaggle 명시 범위: 0 ~ 5 days)
--
-- 파생 컬럼 설계 (생산량 등급화)
--  - 생산량(PRODUCTION_VOLUME)을 등급화하여
--    Gold Layer 집계 및 분석 시 활용성을 높이기 위함
--
-- [테이블 생성 방식]
--  - Bronze 테이블은 Kaggle에 명시된 데이터 타입을 기준으로
--    Snowflake 데이터 타입에 맞게 정의
--  - Silver 테이블 역시 Bronze 테이블과 동일한 데이터 타입을 유지하여 생성
-- ===================================================== 
CREATE OR REPLACE TABLE SILVER_DB.CLEANED_TRANSFORMED.MANUFACTURING_DEFECT_CLEAN (
    PRODUCTION_ID                       STRING NOT NULL,    -- 생산 식별자
    OUTPUT_LEVEL                        STRING,             -- 생산량 등급
    PRODUCTION_VOLUME                   NUMBER(10,0),       -- 생산량
    PRODUCTION_COST_USD                 FLOAT,              -- 생산 비용
    SUPPLIER_QUALITY_PERCENT            FLOAT,              -- 공급 업체 품질
    DELIVERY_DELAY_DAYS                 NUMBER(3,0),        -- 납품 지연
    DEFECT_RATE_PERCENT                 FLOAT,              -- 불량률
    QUALITY_SCORE_PERCENT               FLOAT,              -- 품질 점수
    MAINTENANCE_HOURS                   NUMBER(3,0),        -- 유지보수 시간
    DOWNTIME_PERCENT                    FLOAT,              -- 다운타임 비율
    INVENTORY_TURNOVER_RATIO            FLOAT,              -- 재고 회전율
    STOCKOUT_RATE_PERCENT               FLOAT,              -- 품절률
    WORKER_PRODUCTIVITY_PERCENT         FLOAT,              -- 노동자 생산성
    SAFETY_INCIDENT_COUNT               NUMBER(3,0),        -- 안전 사고
    ENERGY_CONSUMPTION_KWH              FLOAT,              -- 에너지 소비량
    ENERGY_EFFICIENCY_RATIO             FLOAT,              -- 에너지 효율성
    ADDITIVE_PROCESS_HOURS              FLOAT,              -- 첨가 공정 시간
    ADDITIVE_MATERIAL_COST_USD          FLOAT,              -- 첨가 재료 비용
    DEFECT_STATUS                       STRING              -- 불량 상태
)
AS
WITH base AS (
    SELECT
        PRODUCTION_ID,
        PRODUCTION_VOLUME,
        PRODUCTION_COST,
        SUPPLIER_QUALITY,
        DELIVERY_DELAY,
        DEFECT_RATE,
        QUALITY_SCORE,
        MAINTENANCE_HOURS,
        DOWNTIME_PERCENTAGE,
        INVENTORY_TURNOVER,
        STOCKOUT_RATE,
        WORKER_PRODUCTIVITY,
        SAFETY_INCIDENTS,
        ENERGY_CONSUMPTION,
        ENERGY_EFFICIENCY,
        ADDITIVE_PROCESS_TIME,
        ADDITIVE_MATERIAL_COST,
        DEFECT_STATUS
    FROM BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET
    WHERE PRODUCTION_ID IS NOT NULL
)
SELECT
    PRODUCTION_ID,
    -- 생산량을 등급으로 나누어 골드 레이어에서 집계하기 편하게 컬럼 추가(파생 컬럼)
    CASE
        WHEN PRODUCTION_VOLUME BETWEEN 100 AND 324 THEN 'LOW'
        WHEN PRODUCTION_VOLUME BETWEEN 325 AND 549 THEN 'MID_LOW'
        WHEN PRODUCTION_VOLUME BETWEEN 550 AND 774 THEN 'MID_HIGH'
        WHEN PRODUCTION_VOLUME BETWEEN 775 AND 999 THEN 'HIGH'
        ELSE 'UNKNOWN'
    END AS OUTPUT_LEVEL,
    PRODUCTION_VOLUME,           
    PRODUCTION_COST,             
    SUPPLIER_QUALITY,            
    DELIVERY_DELAY,              
    DEFECT_RATE,                 
    QUALITY_SCORE,               
    MAINTENANCE_HOURS,           
    DOWNTIME_PERCENTAGE,         
    INVENTORY_TURNOVER,          
    STOCKOUT_RATE,               
    WORKER_PRODUCTIVITY,         
    SAFETY_INCIDENTS,            
    ENERGY_CONSUMPTION,          
    ENERGY_EFFICIENCY,           
    ADDITIVE_PROCESS_TIME,       
    ADDITIVE_MATERIAL_COST,     
    CASE
        WHEN DEFECT_STATUS IN (FALSE, 0) THEN '낮음'
        WHEN DEFECT_STATUS IN (TRUE, 1)  THEN '높음'
        ELSE 'Undefined'
    END AS DEFECT_STATUS
FROM base;

-- INTENTS
-- 챗봇 기반 데이터로 구성된 테이블
-- JSON 데이터 처리: Bronze 테이블 생성 / Silver Flatten
-- =====================================================
-- JSON 배열 데이터 조회 (FLATTEN 기반 구조화)
--
-- [조회 방식]
--  - JSON 형태로 적재된 반정형 테이블은
--    조회 시 FLATTEN 함수를 사용하여 배열 구조를 펼쳐 확인
--
-- [처리 대상]
--  - JSON 파일 내 tag, patterns, responses 필드를
--    FLATTEN하여 컬럼 형태로 조회
--  - 배열 내부 값을 행 단위로 분리하여 데이터 구조 확인
--
-- [프로젝트 범위]
--  - 본 프로젝트에서는 JSON 샘플 데이터 처리 범위를
--    Bronze → Silver 단계까지로 한정
--  - Silver 단계에서 JSON 구조를 확인·정제하는 데 목적을 둠
--
-- [향후 확장 계획]
--  - 이후 단계에서는 CSV 기반 정형 데이터셋과 연계하여
--    사용자의 자연어 질의 입력에 따라
--    CSV 데이터를 조회·응답하는 챗봇 기반 질의 기능 구현 예정
-- =====================================================
SELECT 
  VALUE:tag::STRING       AS TAG,
  VALUE:patterns::ARRAY    AS PATTERNS,
  VALUE:responses::ARRAY   AS RESPONSES
FROM intents,
LATERAL FLATTEN(INPUT => DATA:intents);

