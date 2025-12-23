-- =====================================================
-- Bronze Layer Snowpipe 설계 개요
--
-- 처리 흐름:
--  - 외부 스토리지(AWS S3 / Azure Blob)에 파일 업로드
--  - Snowpipe가 신규 파일을 감지하여 자동으로 적재
--  - 적재 대상은 Bronze Layer RAW 테이블
--
-- 스토리지 구성:
--  - 본 파이프라인은 **Azure Blob**을 소스로 사용
--  - External Stage를 통해 **Blob**과 Snowflake 연동
--
-- 설계 원칙:
--  - Snowpipe 자동 적재 대상이 명확하도록
--    Bronze 테이블과 External Stage를 1:1로 매핑
--  - 테이블별 전용 Stage 생성 방식 권장
-- =====================================================
-- BRONZE DATABASE, SCHEMA 사용
USE BRONZE_DB.RAW_CSV;

-- AZURE
-- AZURE에서 EventNotification을 사용하려면 먼저 STORAGE INTERGRATION 생성
CREATE OR REPLACE STORAGE INTEGRATION AZURE_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'AZURE'
ENABLED = TRUE
AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
STORAGE_ALLOWED_LOCATIONS = (
  'azure://my.blob.core.windows.net/mycontainer/' 
);

-- STAGE 생성 및 INTERGRATION 연결
CREATE OR REPLACE STAGE KY_AZURE_STAGE
URL='azure://my.blob.core.windows.net/mycontainer//'
STORAGE_INTEGRATION = AZURE_INT;

-- Notification Integration 만들기
CREATE OR REPLACE NOTIFICATION INTEGRATION AZURE_NOTIFICATION
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://myqueue.queue.core.windows.net/queuename-queue'
  AZURE_TENANT_ID = '<AZURE_TENANT_ID>';
  
-- MANUFACTURING_DEFECT_DATASET Snowpipe 생성
-- AZURE Snowpipe
CREATE OR REPLACE PIPE AZURE_PIPE_CSV
    -- Snowpipe를 자동으로 실행하려면 **AUTO_INGEST = TRUE** 옵션을 반드시 활성화
    AUTO_INGEST = TRUE
    INTEGRATION = 'AZURE_NOTIFICATION'
    AS
COPY INTO BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET 
FROM @KY_AZURE_STAGE
PATTERN='^snowpipe/.*\.csv$'
FILE_FORMAT = (
  TYPE = CSV,
  FIELD_OPTIONALLY_ENCLOSED_BY = '"',
  SKIP_HEADER = 1
);

-- =========================================
-- Snowpipe 적재 결과 검증 (행 수 확인)
-- =========================================
SELECT COUNT(*) FROM BRONZE_DB.RAW_CSV.MANUFACTURING_DEFECT_DATASET;