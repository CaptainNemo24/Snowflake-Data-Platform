# SQL

본 디렉터리는 **Snowflake Data Platform 전반적인 구축을 담당하는 SQL 쿼리**로 구성
외부 스테이지 연동부터 Medallion Architecture 설계, 자동화 파이프라인, 데이터 거버넌스까지  
Snowflake 환경에서 수행되는 핵심 SQL 로직을 관리

---

## 디렉터리 구성

```plaintext
sql/
├─ 01_external-stage/
├─ 02_medallion-architecture/
├─ 03_data-pipeline/
└─ 04_data-governance/
```

각 디렉터리는 Snowflake Data Platform 구축 단계별 역할에 따라 분리

---

## 01_external-stage

### 목적
Snowflake와 외부 스토리지(AWS S3 / Azure Blob Storage)를 연동하기 위한  
**External Stage 구성 SQL**을 관리하는 디렉터리

### 주요 내용
- AWS S3 / Azure Blob 외부 스토리지 연결
- External Stage 생성
- Storage Integration 설정
- 외부 스테이지에 적재된 파일 기반 **Bronze 테이블 적재 준비**

외부 스테이지의 데이터 파일을 기반으로  
Bronze Layer 테이블 적재를 수행하기 위한 **초기 연결 단계**

---

## 02_medallion-architecture

### 목적
Snowflake Data Platform의 핵심 구조인  
**Medallion Architecture (Bronze · Silver · Gold)** 전반을 구성하는 디렉터리

### 주요 내용
- Bronze / Silver / Gold 테이블 생성 SQL
- 데이터 정제 및 변환 로직
- 집계 및 요약 테이블 구조 정의
- Data Lake · Data Warehouse(DW) · Data Mart(DM) 구성

본 디렉터리는 Snowflake 기반 Data Platform 구축의 **핵심 SQL 레이어**

---

## 03_data-pipeline

### 목적
Snowflake에서 제공하는 기능을 활용하여  
**자동화 데이터 파이프라인을 구축**하는 SQL을 관리

### 적용 기술
- Snowpipe
- Stream + Task
- Dynamic Table

### 자동화 흐름
1. **Bronze (Snowpipe)**  
   - AWS S3 / Azure Blob에 업로드된 파일 자동 적재

2. **Silver (Stream + Task)**  
   - Bronze 테이블의 데이터 변경(INSERT / UPDATE)을 자동 감지
   - 정제된 Silver 테이블에 반영

3. **Gold (Dynamic Table)**  
   - Silver 테이블 기반 집계 및 요약
   - Gold 테이블을 항상 최신 상태로 유지

실제 운영 환경을 가정한 **End-to-End 자동화 파이프라인 검증 목적**

---

## 04_data-governance

### 목적
Snowflake의 보안 및 접근 제어 기능을 활용한  
**데이터 거버넌스(Data Governance) 구축**을 담당하는 디렉터리

### 주요 구성 요소
- ROLE (부서별 역할 정의)
- OBJECT TAGGING
- MASKING POLICY
- ROW ACCESS POLICY

### 적용 내용
- 부서별 데이터 접근 권한 제어
- 특정 컬럼/행에 대한 제한적 조회
- 부서 간 데이터 공유 규칙 정의

### 예시
- 품질 관련 부서 간 데이터(행/컬럼) 공유 가능
- 권한이 없는 부서는 민감 컬럼 마스킹 또는 접근 제한

데이터 보안과 활용을 동시에 고려한 **Snowflake 거버넌스 설계**

---

## 설계 요약

본 SQL 디렉터리는 단순 쿼리 모음이 아닌,

- Snowflake Data Platform 구축
- Medallion Architecture 설계
- 자동화 파이프라인 운영
- 데이터 거버넌스 수립

을 위한 **전체 흐름을 SQL 레이어**로 구성
