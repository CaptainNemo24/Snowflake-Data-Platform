-- =====================================================
-- Row Access Policy 개요 (Snowflake Data Governance)
--
-- Row Access Policy 정의:
--  - 사용자 또는 ROLE에 따라
--    조회 가능한 행(Row)을 제한하는 보안 정책
--
-- Masking Policy와의 차이:
--  - Masking Policy: 컬럼(Column) 단위 접근 제어
--    → 특정 컬럼 값을 ROLE에 따라 마스킹
--  - Row Access Policy: 행(Row) 단위 접근 제어
--    → 특정 행 자체의 조회 가능 여부를 제어
--
-- 적용 조건:
--  - Row 단위 접근 제어를 위해
--    테이블 내에 부서/조직을 식별할 수 있는 컬럼 필요
--  - 본 프로젝트에서는 DEPT_TAG 컬럼을 부서 식별자로 사용
--
-- 본 실습 설계 방식:
--  - 일반적으로 Row Access Policy는
--    하나의 테이블에 여러 부서 데이터가 혼재된 경우 사용
--  - 본 실습에서는 부서별 테이블이 분리되어 있으므로,
--    Row Access Policy 동작을 검증하기 위해
--    부서 식별 컬럼(DEPT_TAG)을 추가하여 정책 적용
-- =====================================================

-- Row Access Policy 정책 정의를 위한 각 부서별 테이블에 DEPT_TAG 컬럼 추가
-- =====================================================
-- 생산 관련 부서는 DEPT_TAG의 컬럼값 PRODUCTION 지정
-- =====================================================
ALTER TABLE BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL ADD COLUMN DEPT_TAG STRING DEFAULT 'PRODUCTION';
ALTER TABLE SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN ADD COLUMN DEPT_TAG STRING DEFAULT 'PRODUCTION';
-- =====================================================
-- 품질 관련 부서는 DEPT_TAG의 컬럼값 QUALITY 지정
-- =====================================================
ALTER TABLE BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET ADD COLUMN DEPT_TAG STRING DEFAULT 'QUALITY';
ALTER TABLE SILVER_DB.CLEANED_TRANSFORMED.MANUFACTURING_DEFECT_CLEAN ADD COLUMN DEPT_TAG STRING DEFAULT 'QUALITY';
-- =====================================================
-- 데이터 분석팀은 DEPT_TAG의 컬럼값 ANALYTICS 지정
-- =====================================================
ALTER TABLE GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_JOB_STATUS_SUMMARY ADD COLUMN DEPT_TAG STRING DEFAULT 'ANALYTICS';
ALTER TABLE GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_MACHINE_SUMMARY ADD COLUMN DEPT_TAG STRING DEFAULT 'ANALYTICS';
ALTER TABLE GOLD_DB.MANUFACTURING_DEFECT_KPI.FCT_OPERATION_SUMMARY ADD COLUMN DEPT_TAG STRING DEFAULT 'ANALYTICS';

-- Row Access Policy 정의
CREATE OR REPLACE ROW ACCESS POLICY POLICY_TEAM_ACCESS
  AS (DEPT_TAG STRING)
  RETURNS BOOLEAN ->
  CASE
    -- ACCOUNTADMIN은 전체 권한을 가진 관리 역할로, 모든 부서의 행(Row) 조회 가능
    WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN TRUE
    WHEN CURRENT_ROLE() IN ('PRODUCTION_ENGINEERING_TEAM', 'PRODUCTION_MANAGEMENT_TEAM')
         AND DEPT_TAG IN ('PRODUCTION') THEN TRUE
    WHEN CURRENT_ROLE() IN ('QUALITY_ENGINEERING_TEAM', 'QUALITY_MANAGEMENT_TEAM')
         AND DEPT_TAG IN ('QUALITY') THEN TRUE
    WHEN CURRENT_ROLE() IN ('PRODUCTION_ENGINEERING_TEAM', 'PRODUCTION_MANAGEMENT_TEAM','QUALITY_ENGINEERING_TEAM', 'QUALITY_MANAGEMENT_TEAM')
     AND DEPT_TAG IN ('ANALYTICS') THEN TRUE
    WHEN CURRENT_ROLE() = 'DATA_ANALYTICS_TEAM' THEN TRUE
    WHEN CURRENT_ROLE() = 'STRATEGIC_PLANNING_TEAM' THEN TRUE
    ELSE FALSE
  END;

-- Row Access Policy 정책 적용
-- =====================================================
-- 생산 기술/관리팀 정책 적용
-- =====================================================
ALTER TABLE BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL ADD ROW ACCESS POLICY POLICY_TEAM_ACCESS ON (DEPT_TAG);
ALTER TABLE SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN ADD ROW ACCESS POLICY POLICY_TEAM_ACCESS ON (DEPT_TAG);
-- =====================================================
-- 품질 기술/관리팀 정책 적용
-- =====================================================
ALTER TABLE BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET ADD ROW ACCESS POLICY POLICY_TEAM_ACCESS ON (DEPT_TAG);
ALTER TABLE SILVER_DB.CLEANED_TRANSFORMED.MANUFACTURING_DEFECT_CLEAN ADD ROW ACCESS POLICY POLICY_TEAM_ACCESS ON (DEPT_TAG);
-- =====================================================
-- 데이터 분석팀
-- =====================================================
ALTER TABLE GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_JOB_STATUS_SUMMARY ADD ROW ACCESS POLICY POLICY_TEAM_ACCESS ON (DEPT_TAG);
ALTER TABLE GOLD_DB.HYBRID_MANUFACTURING_KPI.FCT_MACHINE_SUMMARY ADD ROW ACCESS POLICY POLICY_TEAM_ACCESS ON (DEPT_TAG);
ALTER TABLE GOLD_DB.MANUFACTURING_DEFECT_KPI.FCT_OPERATION_SUMMARY ADD ROW ACCESS POLICY POLICY_TEAM_ACCESS ON (DEPT_TAG);

-- Row Access Policy 적용 확인하기
-- =================================================================================
-- 각 부서 ROLE LIST
-- 생산/품질 기술팀(BRONZE): PRODUCTION_ENGINEERING_TEAM, QUALITY_ENGINEERING_TEAM
-- 생산/품질 관리팀(SILVER): PRODUCTION_MANAGEMENT_TEAM, QUALITY_MANAGEMENT_TEAM
-- 데이터 분석팀(GOLD): DATA_ANALYTICS_TEAM
-- 전략 기획팀: STRATEGIC_PLANNING_TEAM
-- 일반 사용자(TEST): TEST

-- 각 부서 TABLE LIST(Row Access Policy 대상 부서 한정)
-- 생산 기술팀(BRONZE): BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL
-- 품질 기술팀(BRONZE): BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET
-- 생산 관리팀(SILVER): SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN
-- 품질 관리팀(SILVER): SILVER_DB.CLEANED_TRANSFORMED.MANUFACTURING_DEFECT_CLEAN
-- =================================================================================
USE ROLE DATA_ANALYTICS_TEAM;
SELECT * FROM DB.SCHEMA.TABLE;