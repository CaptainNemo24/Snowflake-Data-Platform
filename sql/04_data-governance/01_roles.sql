-- ===================================================================
-- Role 기반 권한 관리 개요 (Snowflake Data Governance)
--
-- ROLE:
--  - 사용자 접근 권한을 제어하는 역할 단위
--
-- 실행 권한:
--  - ROLE 생성 및 GRANT 작업은 높은 권한(예: ACCOUNTADMIN)으로 수행
--  - 운영 환경에서는 최소 권한 원칙에 따라 별도 관리 계정/역할로 제한하는 것을 권장
--
-- 본 프로젝트 적용 방식(데모/검증 목적):
--  - 권한/정책 동작 검증을 단순화하기 위해
--    부서별 ROLE을 단일 사용자에게 할당하여 테스트
--  - 단, 운영 환경에서는 사용자/부서별로 ROLE을 분리하여 부여하는 것이 일반적
--
-- Role 기반 권한 관리 유의사항:
--  - 한 사용자에게 여러 ROLE이 부여되어도 모든 권한이 동시에 적용되지는 않음
--  - Snowflake는 "현재 활성화된 ROLE" 기준으로
--    조회 가능 데이터와 적용 정책(Masking/Row Access)이 결정됨
-- ===================================================================

-- 새로운 부서(Role) 만들기
-- 생산/품질 기술팀(BRONZE)
CREATE ROLE PRODUCTION_ENGINEERING_TEAM;
CREATE ROLE QUALITY_ENGINEERING_TEAM;
-- 생산/품질 관리팀(SILVER)
CREATE ROLE PRODUCTION_MANAGEMENT_TEAM;
CREATE ROLE QUALITY_MANAGEMENT_TEAM;
-- 데이터 분석팀(GOLD)
CREATE ROLE DATA_ANALYTICS_TEAM;
-- 전략 기획팀
CREATE ROLE STRATEGIC_PLANNING_TEAM;
-- 일반 사용자(TEST_USER)
CREATE ROLE TEST_USER;

-- 생성한 각 부서(Role) 부여하기
-- 생산(HYBRID_MANUFACTURING_CATEGORICAL)/품질(MANUFACTURING_DEFECT_DATASET)기술팀 테이블 접근 권한 부여
GRANT SELECT ON TABLE BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL TO ROLE PRODUCTION_ENGINEERING_TEAM;
GRANT SELECT ON TABLE BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET TO ROLE QUALITY_ENGINEERING_TEAM;

-- 생산(HYBRID_MANUFACTURING_CLEAN)/품질(MANUFACTURING_DEFECT_CLEAN)관리팀 테이블 접근 권한 부여
GRANT SELECT ON TABLE SILVER_DB.CLEANED_TRANSFORMED.HYBRID_MANUFACTURING_CLEAN TO ROLE PRODUCTION_MANAGEMENT_TEAM;
GRANT SELECT ON TABLE SILVER_DB.CLEANED_TRANSFORMED.MANUFACTURING_DEFECT_CLEAN TO ROLE QUALITY_MANAGEMENT_TEAM;

-- GOLD_DB는 데이터 분석팀 테이블 접근 권한 부여
GRANT USAGE ON DATABASE GOLD_DB TO ROLE DATA_ANALYTICS_TEAM;

-- =============================================================================
-- 각 부서 ROLE LIST
-- 생산/품질 기술팀(BRONZE): PRODUCTION_ENGINEERING_TEAM, QUALITY_ENGINEERING_TEAM
-- 생산/품질 관리팀(SILVER): PRODUCTION_MANAGEMENT_TEAM, QUALITY_MANAGEMENT_TEAM
-- 데이터 분석팀(GOLD): DATA_ANALYTICS_TEAM
-- 전략 기획팀: STRATEGIC_PLANNING_TEAM
-- 일반 사용자(TEST): TEST_USER
-- ==============================================================================

-- 사용자 이름 확인하기
SHOW USERS;
GRANT ROLE 부서 TO USER 사용자 이름;
-- 권한 회수
-- REVOKE ROLE STRATEGIC_PLANNING_TEAM FROM USER CKY;