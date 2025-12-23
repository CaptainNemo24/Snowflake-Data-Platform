-- ====================================================
-- AZURE Blob에 적재된 RAW CSV 데이터를 Snowflake로 적재하기 위한
-- External Stage 정의
-- =====================================================
    CREATE
    OR REPLACE STAGE ky_azure_stage 
    URL = 'azure://my.blob.core.windows.net/mycontainer/' 
    CREDENTIALS =(
        AZURE_SAS_TOKEN = '<AZURE_SAS_TOKEN>'
    );
    
-- Stage 연결 및 파일 존재 여부 확인
-- Snowflake에서 S3 파일을 정상적으로 인식하는지 검증
LIST @ky_azure_stage;