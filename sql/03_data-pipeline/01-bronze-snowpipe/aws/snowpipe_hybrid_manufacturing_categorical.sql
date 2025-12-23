-- =====================================================
-- Bronze Layer Snowpipe 설계 개요
--
-- 처리 흐름:
--  - 외부 스토리지(AWS S3 / Azure Blob)에 파일 업로드
--  - Snowpipe가 신규 파일을 감지하여 자동으로 적재
--  - 적재 대상은 Bronze Layer RAW 테이블
--
-- 스토리지 구성:
--  - 본 파이프라인은 **AWS S3** 버킷을 소스로 사용
--  - External Stage를 통해 **S3**와 Snowflake 연동
--
-- 설계 원칙:
--  - Snowpipe 자동 적재 대상이 명확하도록
--    Bronze 테이블과 External Stage를 1:1로 매핑
--  - 테이블별 전용 Stage 생성 방식 권장
-- =====================================================
-- BRONZE DATABASE, SCHEMA 사용
USE BRONZE_DB.RAW_CSV;

-- AWS
-- S3_STAGE_HYBRID_MANUFACTURING STAGE 생성
CREATE OR REPLACE STAGE S3_STAGE_HYBRID_MANUFACTURING
URL = 's3://my-s3-bucket/HYBRID_MANUFACTURING/'
CREDENTIALS = (
  AWS_KEY_ID = '<AWS_KEY_ID>'
  AWS_SECRET_KEY = '<AWS_SECRET_KEY>'
);

-- HYBRID_MANUFACTURING_CATEGORICAL Snowpipe 생성
CREATE OR REPLACE PIPE S3_PIPE_HYBRID_MANUFACTURING_CATEGORICAL
    -- Snowpipe를 자동으로 실행하려면 **AUTO_INGEST = TRUE** 옵션을 반드시 활성화
    AUTO_INGEST = TRUE
    AS
-- CSV 컬럼 개수 ≠ 테이블 컬럼 개수 라면 무조건 컬럼 리스트 명시
COPY INTO BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL (
  JOB_ID,
  MACHINE_ID,
  OPERATION_TYPE,
  MATERIAL_USED,
  PROCESSING_TIME,
  ENERGY_CONSUMPTION,
  MACHINE_AVAILABILITY,
  SCHEDULED_START,
  SCHEDULED_END,
  ACTUAL_START,
  ACTUAL_END,
  JOB_STATUS,
  OPTIMIZATION_CATEGORY
)
FROM @S3_STAGE_HYBRID_MANUFACTURING
FILE_FORMAT = (
  TYPE = CSV,
  FIELD_OPTIONALLY_ENCLOSED_BY = '"',
  SKIP_HEADER = 1
)
-- 파일명, 확장자 대소문자 무시(원래 구분 함)
PATTERN='(?i)^.*\\.csv$';

-- =========================================
-- Snowpipe 적재 결과 검증 (행 수 확인)
-- =========================================
SELECT COUNT(*) FROM BRONZE_DB.RAW_CSV.HYBRID_MANUFACTURING_CATEGORICAL;