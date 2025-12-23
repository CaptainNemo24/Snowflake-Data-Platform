-- ====================================================
-- AWS S3에 적재된 RAW CSV 데이터를 Snowflake로 적재하기 위한
-- External Stage 정의
-- =====================================================

-- AWS S3 External Stage 생성 (RAW 영역)
-- S3 버킷과 Snowflake 간 연결 담당
CREATE
OR REPLACE STAGE ky_s3_stage 
URL = 's3://my-s3-bucket/' 
CREDENTIALS = (
    AWS_KEY_ID = '<AWS_KEY_ID>'
    AWS_SECRET_KEY = '<AWS_SECRET_KEY>'
)
FILE_FORMAT = (
    TYPE = CSV, 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);
-- Stage 연결 및 파일 존재 여부 확인
-- Snowflake에서 S3 파일을 정상적으로 인식하는지 검증
LIST @ky_s3_stage;