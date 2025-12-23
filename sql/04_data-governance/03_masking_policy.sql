-- =====================================================
-- Masking Policy 개요 (Snowflake Data Governance)
--
-- Masking Policy 정의:
--  - 컬럼(Column) 단위로 데이터 노출을 제어하는 보안 정책
--  - 사용자 ROLE 또는 조건에 따라
--    컬럼 값이 다르게 보이도록 마스킹 처리 가능
--
-- 동작 방식:
--  - CASE WHEN 표현식을 사용하여
--    현재 활성 ROLE 또는 조건에 따라
--    실제 값 / 마스킹 값 반환
--
-- 설계 원칙:
--  - 마스킹 대상 컬럼의 데이터 타입별로
--    Masking Policy를 개별 정의
--    (예: STRING, NUMBER, DATE 등)
--
-- 적용 및 확장:
--  - 테이블 컬럼에 직접 Masking Policy 적용 가능
--  - Object Tag와 연계하여
--    TAG 기반 자동 Masking 정책 확장 가능
--
-- 활용 목적:
--  - 민감 정보 보호
--  - Role 기반 접근 제어를 통한 보안 강화
-- =====================================================

-- Masking Policy 정의
-- 문자/숫자형(NUMBER/FLOAT)/날짜형 타입의 컬럼 별도 마스킹 처리
-- 마스킹 처리 하고 싶은 컬럼들을 타입별로 마스킹 정책 따로 정의해야 함

-- ACCOUNTADMIN은 전체 권한을 가진 관리 역할로,
-- 모든 부서의 컬럼 원본 값을 조회 가능
--
-- 그 외 ROLE의 경우:
--  - CURRENT_ROLE()이 허용된 ROLE이 아니면
--    원본 값 대신 마스킹된 값 또는 제한된 형태의 값/변환된 값 반환
--  - 예: 날짜만 제공하고 시간 정보는 마스킹

CREATE OR REPLACE MASKING POLICY MASK_POLICY_SENSITIVITY_STRING
  AS (val STRING)
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','PRODUCTION_ENGINEERING_TEAM', 'PRODUCTION_MANAGEMENT_TEAM',
                            'QUALITY_ENGINEERING_TEAM', 'QUALITY_MANAGEMENT_TEAM','DATA_ANALYTICS_TEAM')
      THEN val
    WHEN CURRENT_ROLE() = 'STRATEGIC_PLANNING_TEAM'
      THEN '**No permission**'
    ELSE NULL
  END;

CREATE OR REPLACE MASKING POLICY MASK_POLICY_SENSITIVITY_NUMBER
  AS (val NUMBER)
  RETURNS NUMBER ->
  CASE
    -- 원본 값을 숨기고 변환된 값 제공
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','PRODUCTION_ENGINEERING_TEAM', 'PRODUCTION_MANAGEMENT_TEAM',
                            'QUALITY_ENGINEERING_TEAM', 'QUALITY_MANAGEMENT_TEAM','DATA_ANALYTICS_TEAM')
      THEN val
    WHEN CURRENT_ROLE() = 'STRATEGIC_PLANNING_TEAM'
      THEN ROUND(val * 0.1, 2)
    ELSE NULL
  END;

CREATE OR REPLACE MASKING POLICY MASK_POLICY_SENSITIVITY_FLOAT
  AS (val FLOAT)
  RETURNS FLOAT ->
  CASE
    -- 원본 값을 숨기고 변환된 값 제공
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','PRODUCTION_ENGINEERING_TEAM', 'PRODUCTION_MANAGEMENT_TEAM',
                            'QUALITY_ENGINEERING_TEAM', 'QUALITY_MANAGEMENT_TEAM','DATA_ANALYTICS_TEAM')
      THEN val
    WHEN CURRENT_ROLE() = 'STRATEGIC_PLANNING_TEAM'
      THEN ROUND(val * 0.1, 2)
    ELSE NULL
  END;
  
CREATE OR REPLACE MASKING POLICY MASK_POLICY_SENSITIVITY_DATE
  AS (val TIMESTAMP_NTZ)
  RETURNS TIMESTAMP_NTZ ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','PRODUCTION_ENGINEERING_TEAM', 'PRODUCTION_MANAGEMENT_TEAM',
                        'QUALITY_ENGINEERING_TEAM', 'QUALITY_MANAGEMENT_TEAM','DATA_ANALYTICS_TEAM')
      THEN val
    WHEN CURRENT_ROLE() = 'STRATEGIC_PLANNING_TEAM'
      -- 날짜만 보여주고 시간 부분은 숨김
      THEN DATE_TRUNC('DAY', val)
    ELSE NULL
  END;

-- =====================================================
-- TAG 기반 자동 Masking 정책 적용
--
-- 정의한 Masking Policy를 사전에 생성한 TAG에 연결하여
-- 해당 TAG가 부여된 컬럼에 자동으로 마스킹 정책 적용
-- =====================================================
ALTER TAG TAG_SENSITIVITY SET MASKING POLICY MASK_POLICY_SENSITIVITY_STRING;
ALTER TAG TAG_SENSITIVITY SET MASKING POLICY MASK_POLICY_SENSITIVITY_NUMBER;
ALTER TAG TAG_SENSITIVITY SET MASKING POLICY MASK_POLICY_SENSITIVITY_FLOAT;
ALTER TAG TAG_SENSITIVITY SET MASKING POLICY MASK_POLICY_SENSITIVITY_DATE;

-- Masking Policy 적용 확인
-- ===================================================================================
-- 각 부서 ROLE LIST
-- 생산/품질 기술팀(BRONZE): PRODUCTION_ENGINEERING_TEAM, QUALITY_ENGINEERING_TEAM
-- 생산/품질 관리팀(SILVER): PRODUCTION_MANAGEMENT_TEAM, QUALITY_MANAGEMENT_TEAM
-- 데이터 분석팀(GOLD): DATA_ANALYTICS_TEAM
-- 전략 기획팀: STRATEGIC_PLANNING_TEAM
-- 일반 사용자(TEST): TEST_USER

-- 각 부서 TABLE LIST(마스킹 처리 대상 부서 한정)
-- 생산 기술팀(BRONZE): BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL
-- 품질 기술팀(BRONZE): BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET
-- 생산 관리팀(SILVER): SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN
-- 품질 관리팀(SILVER): SILVER_DB.CLEANED_TRANSFORMED.MANUFACTURING_DEFECT_CLEAN
-- ======================================================================================

-- 부서 ROLE 전환 후 마스킹 정책 검증
USE ROLE DATA_ANALYTICS_TEAM;
-- 부서별 ROLE 전환 후 데이터 표시 결과 확인
SELECT * FROM DB.SCHEMA.TABLE;