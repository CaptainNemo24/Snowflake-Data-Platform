# Python Utils

본 디렉터리는 Snowflake Data Platform 구축 과정에서  
**Medallion Architecture(Bronze · Silver · Gold)**를 적용하기 위해 필요한  
각종 **자동화 Python 스크립트**를 관리

주요 목적:

- EDA 이전 단계에서의 데이터 사전 검증 자동화
- 데이터 파이프라인 테스트를 위한 샘플 데이터 생성
- 자연어 기반 SQL 질의 응답 구조 확장 준비

---

## 디렉터리 구성

```plaintext
python-utils/
├─ pre-eda-utils/
├─ chatbot/
└─ append_data/
```

각 하위 디렉터리는 Snowflake Data Platform 구축 단계별 자동화를 목적으로 분리

---

## pre-eda-utils

### 목적
EDA를 본격적으로 수행하기 이전에  
**Data Profiling 기반 사전 분석을 자동화**하기 위함

### 주요 기능
- 원본 데이터 구조 및 컬럼 정보 분석
- 컬럼별 데이터 값 분포 및 특성 파악
- AI 기반으로 데이터 값의 **단위 / 범위 / 의미 추론**
- Snowflake 적재를 위한 **데이터 타입 추천**
- 컬럼 성격 요약 (ID / 범주형 / 수치형 등)

### 활용 방식
- Profiling 결과를 기반으로 **추가 EDA 대상 컬럼 선별**
- Snowflake Silver Layer 설계 전 **데이터 타입 및 정제 기준 사전 검증**

본 유틸리티를 통해 **검증이 필요한 포인트만 선별하여 EDA를 진행**하는 것을 목표

---

## chatbot

### 목적
자연어 질의 기반 데이터 조회를 위한  
**챗봇 구조 확장을 준비하는 디렉터리**

### 주요 작업 내용
- 자연어 질의 → SQL 자동 생성 → 결과 응답 구조 설계
- JSON 기반 챗봇 데이터 구조 관리
- 기존 **영문 챗봇 구조를 한글화**하여 한국어 질의 대응 구조로 변환

### 향후 활용 계획
- Snowflake SQL 자동 생성
- KPI 및 분석 결과 자연어 응답
- JSON 기반 결과 반환 구조 연계

---

## append_data

### 목적
Snowflake Data Pipeline 자동화 테스트를 위해  
**데이터 UPDATE / INSERT 상황을 시뮬레이션**하기 위함

### 주요 기능
- 샘플 데이터 파일 기반 랜덤 데이터 생성
- 사용자가 지정한 개수만큼 신규 데이터 추가
- 데이터 누적 및 변경 시나리오 테스트

### 활용 시나리오
- Snowpipe
- Stream + Task
- CDC(Change Data Capture) 구조 검증

---

## 설계 의도 요약

본 디렉터리의 유틸리티들은  
단순 스크립트 실행 목적이 아닌,

- Snowflake 데이터 타입 설계 검증
- Medallion Architecture 단계별 구조 검증
- 자동화 데이터 파이프라인 안정성 확보

를 목표로 구성
