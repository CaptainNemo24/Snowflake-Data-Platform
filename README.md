# 클라우드 기반 제조 품질 데이터 분석 플랫폼

<img width="1345" height="574" alt="Snowflake Data Platform" src="https://github.com/user-attachments/assets/101ca161-e206-42dc-97f3-51ca4e349a73" />

본 프로젝트는 **Snowflake를 중심으로 한 Data Platform을 직접 설계·구축**하며,  
실무 환경에서 활용 가능한 데이터 수집 · 정제 · 집계 · 자동화 · 거버넌스 전 과정을  
**Medallion Architecture(Bronze · Silver · Gold)** 기반으로 구현한 프로젝트

---

## 프로젝트 개요

향후 실무 프로젝트에서 Snowflake를 주요 데이터 플랫폼으로 활용할 것을 대비하여,  
Snowflake의 구조와 데이터 처리 기능을 **이론이 아닌 실제 구축을 통해 체득**하는 것을 목표로  
본 Data Platform을 설계·구축

- Snowflake 아키텍처 및 데이터 처리 흐름 이해
- 클라우드 기반 데이터 적재 및 자동화 경험
- 보안·접근제어(거버넌스) 적용

---

## 프로젝트 목적

- Snowflake를 기반으로 **Medallion Architecture**를 적용하여
  데이터 수집 → 정제 → 집계까지의 전 과정을 실제 환경 수준으로 구현
- 정형 데이터(CSV) 및 반정형 데이터(JSON)를  
  외부 스토리지(AWS S3, Azure Blob)와 연동하여 Stage 기반 적재
- Snowpipe, Stream + Task, Dynamic Table을 활용한  
  **End-to-End 자동화 데이터 파이프라인 구축**
- ROLE, Object Tagging, MASKING POLICY, ROW ACCESS POLICY 등을 활용한  
  **데이터 거버넌스 및 접근 제어 체계 수립**
- **Tableau**를 활용한 Gold 테이블 시각화

---

## 아키텍처 개요

- External Storage  
  - AWS S3  
  - Azure Blob Storage  

- Snowflake  
  - External Stage  
  - Bronze / Silver / Gold Layer  
  - Automated Pipeline  
  - Data Governance

---

## 프로젝트 디렉터리 구조

```plaintext
snowflake-data-platform/
├─ sql/
├─ python-utils/
├─ eda/
└─ docs/
```

---

## 디렉터리별 설명

### sql
Snowflake Data Platform의 전반적인 구축을 담당하는  
**Snowflake SQL 쿼리**를 관리하는 디렉터리

- External Stage (AWS S3 / Azure Blob 연동)
- Medallion Architecture (Bronze · Silver · Gold)
- 자동화 파이프라인 (Snowpipe, Stream + Task, Dynamic Table)
- 데이터 거버넌스 (ROLE, TAG, MASKING, ROW ACCESS)

---

### python-utils
Snowflake Data Platform 구축 과정에서 필요한  
**자동화 Python 스크립트**를 관리하는 디렉터리

- EDA 이전 단계 Data Profiling 자동화
- 데이터 파이프라인 테스트용 샘플 데이터 생성
- 자연어 질의 → SQL 변환 챗봇 구조 준비

---

### eda (Exploratory Data Analysis)
Snowflake 기반 Data Platform 구축에 앞서  
**Medallion Architecture 설계 검증을 목적**으로 수행한 EDA 결과를 관리

- 원본 데이터 구조 파악
- 데이터 타입 및 PK 후보 검증
- 집계·요약을 통한 Gold 테이블 설계 검증

※ 본 EDA는 **모델링 목적이 아닌**,  
   데이터 레이어 설계 위해 수행

---

### docs (Documentation)
Snowflake Data Platform 구축 **전 과정**을 문서화한  
보고서 및 실습 가이드를 관리하는 디렉터리

- 전체 설계 설명용 Report
- 실제 실행 가능한 Hands-on Guide

---

## 프로젝트 핵심 요약

- Snowflake 중심 Data Platform 직접 구축
- Medallion Architecture 기반 데이터 레이어 설계
- 클라우드 외부 스토리지 연동
- 자동화 파이프라인 구현
- 데이터 거버넌스 적용
- Tableau를 활용한 Gold 테이블 시각화
  - https://public.tableau.com/app/profile/.15366561/viz/SnowflakeGoldLayerKPI/FCT_JOB_STATUS_SUMMARY#1

---

## 참고

각 하위 디렉터리에는 해당 영역의 역할과 설계 의도를 설명하는 `README.md` 포함
