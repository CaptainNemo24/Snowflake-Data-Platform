-- =====================================================
-- Dynamic Table (Gold Layer) 설계 개요
--
-- Dynamic Table 정의:
--  - SQL로 정의한 "결과 테이블"을
--    Snowflake가 자동으로 최신 상태로 유지해주는 기능
--  - 사용자가 작성한 변환 / 정제 / 집계 SQL을 기준으로
--    증분 기반 자동 업데이트 수행
--
-- 동작 방식:
--  - 지정된 주기(TARGET_LAG)에 따라 SQL을 자동 실행
--  - 변경된 데이터만 반영하여 결과 테이블을 갱신
--  - 별도의 Stream + Task 구성 없이도 자동 파이프라인 구현 가능
--
-- 적용 단계:
--  - 본 프로젝트에서는 Gold Layer에서 Dynamic Table을 활용
--  - Silver Layer에서 정제된 데이터를 기반으로
--    주제별 요약 및 집계 결과 생성
--
-- 설계 목적:
--  - 항상 최신 상태의 분석/시각화용 테이블 제공
-- =====================================================

-- Dynamic Table 생성 대상이 되는 GOLD_DB / Schema 설정
USE GOLD_DB.HYBRID_MANUFACTURING_KPI;
USE GOLD_DB.MANUFACTURING_DEFECT_KPI;

-- =====================================================
-- FCT_MACHINE_SUMMARY_DT
-- 기계(Machine) 단위로 공정 유형(Operation_Type)별
-- 주요 운영 지표를 요약·집계한 Fact 테이블
-- =====================================================
USE GOLD_DB.HYBRID_MANUFACTURING_KPI;

-- FCT_MACHINE_SUMMARY Dynamic Table 생성
CREATE OR REPLACE DYNAMIC TABLE GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_MACHINE_SUMMARY_DT
-- TARGET_LAG: Dynamic Table 결과가 얼마나 최신 상태여야 하는지를 정의하는 파라미터
-- - 지정한 시간 이내(예: 10분)로 결과를 유지하도록 Snowflake가 자동 갱신
-- - 실행 시점은 Snowflake가 내부적으로 판단
TARGET_LAG = '10 MINUTE'
WAREHOUSE = COMPUTE_WH
AS
-- 기존에 사용했던 FCT_MACHINE_SUMMARY 테이블의 요약·집계 그대로 사용
WITH operation_ratio AS (
    -- 각 Machine_ID × Operation_Type별 비율(%) 계산
    SELECT
        Machine_ID,
        Operation_Type,
        COUNT(*) AS operation_count,
        ROUND(
            COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY Machine_ID), 0),
            2
        ) AS operation_ratio_percent
    FROM SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN
    GROUP BY Machine_ID, Operation_Type
)
-- 기계별 주요 KPI + 공정 비율 결합
SELECT
    h.Machine_ID,
    o.Operation_Type,
    o.operation_ratio_percent,
    ROUND(AVG(h.Machine_Availability), 2) AS Machine_Availability_avg,
    ROUND(AVG(h.Energy_Consumption), 2) AS Energy_Consumption_avg,
    ROUND(AVG(h.Material_Used), 2) AS Material_Used_avg,
FROM SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN h
LEFT JOIN operation_ratio o
    ON h.Machine_ID = o.Machine_ID
GROUP BY
    h.Machine_ID,
    o.Operation_Type,
    o.operation_ratio_percent
ORDER BY
    h.Machine_ID ASC,
    o.operation_ratio_percent DESC;
    
-- =========================================
-- Gold 집계 결과 비교
--  - 기존 Gold 테이블: 백업용
--  - Dynamic Table: 최신 집계 결과
-- =========================================
SELECT * FROM GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_MACHINE_SUMMARY;
SELECT * FROM GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_MACHINE_SUMMARY_DT;

-- =====================================================
-- FCT_JOB_STATUS_SUMMARY_DT
-- 작업 상태(Job_Status: 실패 / 지연 / 완료) 기준으로
-- 주요 운영 지표를 요약·집계한 Fact 테이블
-- =====================================================
USE GOLD_DB.HYBRID_MANUFACTURING_KPI;

-- FCT_JOB_STATUS_SUMMARY Dynamic Table 생성
CREATE OR REPLACE DYNAMIC TABLE GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_JOB_STATUS_SUMMARY_DT
-- TARGET_LAG: Dynamic Table 결과가 얼마나 최신 상태여야 하는지를 정의하는 파라미터
-- - 지정한 시간 이내(예: 10분)로 결과를 유지하도록 Snowflake가 자동 갱신
-- - 실행 시점은 Snowflake가 내부적으로 판단
TARGET_LAG = '10 MINUTE'
WAREHOUSE = COMPUTE_WH
AS
-- 기존에 사용했던 FCT_JOB_STATUS_SUMMARY 테이블의 요약·집계 그대로 사용
WITH
-- Optimization_Category 비율 계산
opt_ratio AS (
    SELECT
        Job_Status,
        Optimization_Category,
        COUNT(*) AS cnt,
        ROUND(
            COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY Job_Status), 0),
            2
        ) AS category_ratio_percent
    FROM SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN
    GROUP BY Job_Status, Optimization_Category
),
-- Job_Status별 전체 건수 계산
status_total AS (
    SELECT
        Job_Status,
        COUNT(*) AS total_count
    FROM SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN
    GROUP BY Job_Status
)
-- 최종 테이블 생성
SELECT
    h.Job_Status,
    s.total_count,
    o.Optimization_Category,
    o.category_ratio_percent,               
    ROUND(AVG(DATEDIFF(minute, h.Scheduled_End_TS, h.Actual_End_TS)), 2) AS avg_delay_min,
    ROUND(AVG(h.Processing_Time), 2) AS processing_time_avg,
    ROUND(AVG(h.Energy_Consumption), 2) AS energy_consumption_avg,
    ROUND(AVG(h.Machine_Availability), 2) AS machine_availability_avg
FROM SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN h
LEFT JOIN opt_ratio o
    ON h.Job_Status = o.Job_Status
    AND h.Optimization_Category = o.Optimization_Category
LEFT JOIN status_total s
    ON h.Job_Status = s.Job_Status
GROUP BY
    h.Job_Status,
    s.total_count,
    o.Optimization_Category,
    o.category_ratio_percent
ORDER BY
    h.Job_Status,
    o.category_ratio_percent DESC;
    
-- =========================================
-- Gold 집계 결과 비교
--  - 기존 Gold 테이블: 백업용
--  - Dynamic Table: 최신 집계 결과
-- =========================================
SELECT * FROM GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_JOB_STATUS_SUMMARY;
SELECT * FROM GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_JOB_STATUS_SUMMARY_DT;

-- Dynamic table(GOLD 진행)
-- =====================================================
-- FCT_OPERATION_SUMMARY_DT
-- 생산량 등급(Production Volume Grade) 기준으로
-- 주요 생산·운영 지표를 요약·집계한 Fact 테이블
-- =====================================================
USE GOLD_DB.MANUFACTURING_DEFECT_KPI;

-- FCT_OPERATION_SUMMARY Dynamic Table 생성
CREATE OR REPLACE DYNAMIC TABLE GOLD_DB.MANUFACTURING_DEFECT_KPI.FCT_OPERATION_SUMMARY_DT
-- TARGET_LAG: Dynamic Table 결과가 얼마나 최신 상태여야 하는지를 정의하는 파라미터
-- - 지정한 시간 이내(예: 10분)로 결과를 유지하도록 Snowflake가 자동 갱신
-- - 실행 시점은 Snowflake가 내부적으로 판단
TARGET_LAG = '10 MINUTE'
WAREHOUSE = COMPUTE_WH
AS
-- 기존에 사용했던 FCT_OPERATION_SUMMARY 테이블의 요약·집계 그대로 사용
SELECT
    OUTPUT_LEVEL,

    -- 생산량 등급별 평균 KPI
    ROUND(AVG(PRODUCTION_COST_USD), 2)            AS PRODUCTION_COST_USD_AVG,
    ROUND(AVG(MAINTENANCE_HOURS), 2)              AS MAINTENANCE_HOURS_AVG,
    ROUND(AVG(ENERGY_CONSUMPTION_KWH), 2)         AS ENERGY_CONSUMPTION_KWH_AVG,
    ROUND(AVG(ADDITIVE_PROCESS_HOURS), 2)         AS ADDITIVE_PROCESS_HOURS_AVG,
    ROUND(AVG(INVENTORY_TURNOVER_RATIO), 2)       AS INVENTORY_TURNOVER_RATIO_AVG,
    ROUND(AVG(STOCKOUT_RATE_PERCENT), 2)          AS STOCKOUT_RATE_PERCENT_AVG,
    ROUND(AVG(ADDITIVE_MATERIAL_COST_USD), 2)     AS ADDITIVE_MATERIAL_COST_USD_AVG,

    -- 불량 상태 비율 (범주형 -> 비율로 변환)
    ROUND(
        100 * SUM(CASE WHEN DEFECT_STATUS = '높음' THEN 1 ELSE 0 END) 
        / NULLIF(COUNT(*), 0),
        2
    ) AS DEFECT_HIGH_PERCENT,

    ROUND(
        100 * SUM(CASE WHEN DEFECT_STATUS = '낮음' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    ) AS DEFECT_LOW_PERCENT

FROM SILVER_DB.CLEANED_TRANSFORMED.MANUFACTURING_DEFECT_CLEAN
GROUP BY OUTPUT_LEVEL
ORDER BY 
    CASE 
        WHEN OUTPUT_LEVEL = 'LOW' THEN 1
        WHEN OUTPUT_LEVEL = 'MID_LOW' THEN 2
        WHEN OUTPUT_LEVEL = 'MID_HIGH' THEN 3
        WHEN OUTPUT_LEVEL = 'HIGH' THEN 4
        ELSE NULL
    END;

-- =========================================
-- Gold 집계 결과 비교
--  - 기존 Gold 테이블: 백업용
--  - Dynamic Table: 최신 집계 결과
-- =========================================
SELECT * FROM GOLD_DB.MANUFACTURING_DEFECT_KPI.FCT_OPERATION_SUMMARY;
SELECT * FROM GOLD_DB.MANUFACTURING_DEFECT_KPI.FCT_OPERATION_SUMMARY_DT;
