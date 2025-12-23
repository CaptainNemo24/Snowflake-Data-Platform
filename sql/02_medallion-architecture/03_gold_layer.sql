-- =====================================================
-- Gold Layer (Medallion Architecture)
--
-- 정의:
--  - Silver Layer에서 정제된 데이터를 기반으로
--    데이터 특성에 맞춰 주제별 요약 및 집계를 수행하는 단계
--
-- 특징:
--  - 분석 목적 중심의 집계 테이블 구성
--  - 지표(KPI) 계산 및 등급/구간 기반 요약
--  - 시각화 및 리포팅에 최적화된 구조
--
-- 분석 방식:
--  - 데이터 도메인(생산량, 품질, 비용 등)별로
--    주제 중심의 요약 및 집계 수행
--  - 불필요한 상세 컬럼 제거, 분석에 필요한 지표만 유지
--
-- 설계 원칙:
--  - 반복 계산이 필요한 로직은 Gold Layer에서 일관되게 관리
--  - 대시보드 및 리포트에서 바로 사용 가능한 형태로 설계
--
-- 본 SQL의 역할:
--  - Silver → Gold 데이터 집계 및 요약
--  - 주제별 분석 테이블(FACT / SUMMARY) 생성
-- =====================================================

-- GOLD DATABASE 생성 및 사용
CREATE DATABASE GOLD_DB;
USE DATABASE GOLD_DB;

-- GOLD SCHEMA 생성 및 사용
CREATE SCHEMA HYBRID_MANUFACTURING_KPI;
CREATE SCHEMA MANUFACTURING_DEFECT_KPI;

-- HYBRID_MANUFACTURING_KPI 스키마 사용
USE GOLD_DB.HYBRID_MANUFACTURING_KPI;
-- MANUFACTURING_DEFECT_KPI 스키마 사용
USE GOLD_DB.MANUFACTURING_DEFECT_KPI;

-- HYBRID_MANUFACTURING_KPI 스키마에 테이블 생성
-- =====================================================
-- FCT_JOB_STATUS_SUMMARY
--
-- 정의:
--  - 작업 상태(Job_Status: 실패 / 지연 / 완료) 기준으로
--    주요 운영 지표를 요약·집계한 Fact 테이블
--
-- 집계 기준:
--  - Job_Status(실패·지연·완료)별 그룹화
--  - 각 상태에 따른 운영 성과 및 효율 지표 계산
--
-- 주요 지표:
--  - 최적화 범주(Optimization_Category) 수 및 비율
--  - 평균 지연 시간 (delay_min_avg)
--  - 평균 처리 시간 (processing_time_avg)
--  - 평균 에너지 소비량 (energy_consumption_avg)
--  - 평균 기계 가용성 (machine_availability_avg)
--  - 실제 테이블에는 추가 집계 지표가 포함됨
--
-- 테이블 정보:
--  - 스키마: GOLD_DB.HYBRID_MANUFACTURING_KPI
--  - 테이블: FCT_JOB_STATUS_SUMMARY
--  - 컬럼 수: 8
--
-- 활용 목적:
--  - 작업 상태별 운영 성과 비교
--  - 품질 및 생산 효율 모니터링
-- =====================================================
CREATE OR REPLACE TABLE GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_JOB_STATUS_SUMMARY AS
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
    ROUND(AVG(DATEDIFF(minute, h.Scheduled_End_TS, h.Actual_End_TS)), 2) AS delay_min_avg,
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

-- FCT_JOB_STATUS_SUMMARY 테이블 조회
SELECT * FROM GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_JOB_STATUS_SUMMARY;

-- HYBRID_MANUFACTURING_KPI 스키마에 테이블 생성
-- =====================================================
-- FCT_MACHINE_SUMMARY
--
-- 정의:
--  - 기계(Machine) 단위로 공정 유형(Operation_Type)별
--    주요 운영 지표를 요약·집계한 Fact 테이블
--
-- 집계 기준:
--  - Machine 기준 그룹화
--  - 각 기계의 공정 유형(Operation_Type) 분포 및 성과 지표 집계
--
-- 주요 지표:
--  - 공정 유형(Operation_Type) 비율
--  - 평균 기계 가용성 (Machine_Availability_avg)
--  - 평균 에너지 소비량 (Energy_Consumption_avg)
--  - 평균 사용 재료량 (Material_Used_avg)
--  - 실제 테이블에는 추가 집계 지표가 포함됨
--
-- 테이블 정보:
--  - 스키마: GOLD_DB.HYBRID_MANUFACTURING_KPI
--  - 테이블: FCT_MACHINE_SUMMARY
--  - 컬럼 수: 6
--
-- 활용 목적:
--  - 기계별 공정 유형 특성 분석
--  - 장비 운영 효율 비교 및 병목 구간 식별
-- =====================================================
CREATE OR REPLACE TABLE GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_MACHINE_SUMMARY AS
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

-- FCT_MACHINE_SUMMARY 테이블 조회
SELECT * FROM GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_MACHINE_SUMMARY;

-- MANUFACTURING_DEFECT_KPI 스키마에 테이블 생성
-- =====================================================
-- FCT_OPERATION_SUMMARY (Gold Layer)
--
-- 정의:
--  - 생산량 등급(Production Volume Grade) 기준으로
--    주요 생산·운영 지표를 요약·집계한 Fact 테이블
--
-- 집계 기준:
--  - 생산량 등급별 그룹화
--  - 등급별 비용/설비/에너지/품질 지표를 비교 가능하도록 집계
--
-- 주요 지표:
--  - 생산 비용 (PRODUCTION_COST_USD_AVG)
--  - 유지보수 시간 (MAINTENANCE_HOURS_AVG)
--  - 에너지 소비량 (ENERGY_CONSUMPTION_KWH_AVG)
--  - 불량 상태 비율 (DEFECT_LOW_PERCENT)
--  - 실제 테이블에는 추가 집계 지표가 포함됨
-- 테이블 정보:
--  - 스키마: GOLD_DB.MANUFACTURING_DEFECT_KPI
--  - 테이블: FCT_OPERATION_SUMMARY
--  - 컬럼 수: 10
--
-- 활용 목적:
--  - 생산량 규모별 운영 효율/품질 비교 분석
-- =====================================================
CREATE OR REPLACE TABLE GOLD_DB.MANUFACTURING_DEFECT_KPI.FCT_OPERATION_SUMMARY AS
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
        ELSE 'UNKNOWN'
    END;

SELECT
OUTPUT_LEVEL,
PRODUCTION_COST_USD_AVG,
MAINTENANCE_HOURS_AVG,
ENERGY_CONSUMPTION_KWH_AVG,
ADDITIVE_PROCESS_HOURS_AVG,
INVENTORY_TURNOVER_RATIO_AVG,
STOCKOUT_RATE_PERCENT_AVG,
STOCKOUT_RATE_PERCENT_AVG,
ADDITIVE_MATERIAL_COST_USD_AVG
FROM GOLD_DB.MANUFACTURING_DEFECT_KPI.FCT_OPERATION_SUMMARY;

-- FCT_OPERATION_SUMMARY 테이블 조회
SELECT * FROM GOLD_DB.MANUFACTURING_DEFECT_KPI.FCT_OPERATION_SUMMARY;