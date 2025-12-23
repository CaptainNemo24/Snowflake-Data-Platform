-- =====================================================
-- Bronze Layer (Medallion Architecture)
--
-- 정의:
--  - 원천 시스템(S3, DB, API 등)에서 수집한 데이터를
--    가공 없이 Snowflake에 적재하는 단계
--
-- 특징:
--  - 데이터 정합성 보장 최소화 (Raw 보존 목적)
--  - 컬럼 타입 최소 변경
--  - 재처리(Backfill)를 고려한 구조
--
-- 본 SQL의 역할:
--  - AWS S3 / AZURE Blob → Snowflake RAW 영역 적재
--  - External Stage 및 COPY INTO 구성
-- =====================================================

-- BRONZE DATABASE 생성 및 사용
CREATE DATABASE BRONZE_DB;
USE DATABASE BRONZE_DB;

-- BRONZE SCHEMA 생성 및 사용
-- 파일 포맷(CSV / JSON) 기준 스키마 분리
-- CSV
CREATE SCHEMA RAW_CSV;
USE SCHEMA RAW_CSV;
-- JSON
CREATE SCHEMA RAW_JSON;
USE SCHEMA RAW_JSON;

-- HYBRID_MANUFACTURING_CATEGORICAL
-- 적층 및 절삭 가공 공정을 통합한 하이브리드 제조 시스템(HMS)의 생산 계획 및 최적화 데이터로 구성된 테이블
CREATE
OR REPLACE TABLE BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL (
    Job_ID VARCHAR(10),                 -- 작업 식별자
    Machine_ID VARCHAR(10),             -- 해당 작업이 수행된 기계의 식별자
    Operation_Type VARCHAR(100),        -- 공정 유형
    Material_Used NUMBER(10, 2),        -- 사용된 재료의 양
    Processing_Time NUMBER(10, 0),      -- 작업 처리 시간
    Energy_Consumption NUMBER(10, 2),   -- 에너지 소비량
    Machine_Availability NUMBER(10, 0), -- 기계 가용성
    Scheduled_Start TIMESTAMP_NTZ,      -- 예정 시작 시간
    Scheduled_End TIMESTAMP_NTZ,        -- 예정 종료 시간
    Actual_Start TIMESTAMP_NTZ,         -- 실제 시작 시간
    Actual_End TIMESTAMP_NTZ,           -- 실제 종료 시간
    Job_Status VARCHAR(100),            -- 작업 상태
    Optimization_Category VARCHAR(100)  -- 최적화 범주
);

-- MANUFACTURING_DEFECT_DATASET
-- 제조 환경에서 불량률에 영향을 미치는 요인에 대한 예측 데이터로 구성된 테이블
CREATE
OR REPLACE TABLE BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET (
    PRODUCTION_ID STRING,               -- 생산 식별자
    PRODUCTION_VOLUME NUMBER(10,0),     -- 생산량
    PRODUCTION_COST FLOAT,              -- 생산 비용
    SUPPLIER_QUALITY FLOAT,             -- 공급 업체 품질
    DELIVERY_DELAY NUMBER(3,0),         -- 납품 지연
    DEFECT_RATE FLOAT,                  -- 불량률
    QUALITY_SCORE FLOAT,                -- 품질 점수
    MAINTENANCE_HOURS NUMBER(3,0),      -- 유지보수 시간
    DOWNTIME_PERCENTAGE FLOAT,          -- 다운타임 비율
    INVENTORY_TURNOVER FLOAT,           -- 재고 회전율
    STOCKOUT_RATE FLOAT,                -- 품절률
    WORKER_PRODUCTIVITY FLOAT,          -- 노동자 생산성
    SAFETY_INCIDENTS NUMBER(3,0),       -- 안전 사고
    ENERGY_CONSUMPTION FLOAT,           -- 에너지 소비량
    ENERGY_EFFICIENCY FLOAT,            -- 에너지 효율성
    ADDITIVE_PROCESS_TIME FLOAT,        -- 첨가 공정 시간
    ADDITIVE_MATERIAL_COST FLOAT,       -- 첨가 재료 비용
    DEFECT_STATUS BOOLEAN               -- 불량 상태
);

-- INTENTS
-- 챗봇 기반 데이터로 구성된 테이블
-- JSON 데이터 처리: Bronze 테이블 생성 / Silver Flatten
CREATE OR REPLACE TABLE BRONZE_DB.RAW_JSON.INTENTS (DATA VARIANT);

-- ===================================================
-- S3 CSV → HYBRID_MANUFACTURING_CATEGORICAL 테이블 적재
-- ===================================================
COPY INTO BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL
FROM 
@ky_s3_stage/csv/hybrid_manufacturing_categorical.csv 
    FILE_FORMAT = (
        TYPE = CSV,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        SKIP_HEADER = 1
    );

-- =============================================================
-- HYBRID_MANUFACTURING_CATEGORICAL 적재 결과 확인 (RAW 데이터 조회)
-- =============================================================
SELECT * FROM BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL LIMIT 30;

-- ===============================================
-- AZURE Blob → MANUFACTURING_DEFECT_DATASET 테이블 적재
-- ===============================================
COPY INTO BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET
FROM 
@ky_azure_stage/csv/manufacturing_defect_dataset.csv 
    FILE_FORMAT = (
        TYPE = CSV,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        SKIP_HEADER = 1
    );

-- ==========================================================
-- MANUFACTURING_DEFECT_DATASET 적재 결과 확인 (RAW 데이터 조회)
-- ==========================================================
SELECT * FROM BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET LIMIT 30;