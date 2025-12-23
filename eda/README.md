# Exploratory Data Analysis (EDA)

본 디렉토리는 Snowflake 기반 Data Platform 구축에 앞서  
**Medallion Architecture(Bronze · Silver · Gold) 설계 검증을 목적**으로 수행한  
Exploratory Data Analysis(EDA) 결과를 정리함

본 EDA는 **모델링 목적이 아닌**,  
원본 데이터 이해 및 데이터 레이어 설계 타당성 검증을 위해 수행

---

## EDA 수행 목적

Snowflake Data Platform 구축 이전 단계에서 다음을 검증하기 위해 EDA를 수행

- Bronze 단계에 적재될 **원본 데이터 구조 사전 파악**
- Silver 단계 정제를 위한 **데이터 타입, 결측치, 이상값 처리 기준 수립**
- Gold 단계에서 생성할 **집계·요약 테이블(KPI) 대략 설계**
- 시각화 및 상관관계 분석을 통한 **지표 활용 가능성 검증**

---

## EDA 접근 방식

### 1. Data Profiling 기반 사전 분석
EDA의 효율성을 높이기 위해,  
Python 기반 **Data Profiling 스크립트**를 활용하여 다음 항목을 선제적으로 분석

- 데이터 스키마 및 컬럼 구조
- 결측치 및 이상값 분포
- 컬럼 간 상관관계
- Snowflake 데이터 타입 추천 결과

이를 통해 **검증이 필요한 포인트만 선별하여 추가 EDA 및 시각화** 진행

---

### 2. EDA의 역할 정의
본 EDA는 다음을 목적으로 함

- 데이터 구조 이해
- Snowflake 데이터 타입 결정
- PK 후보 컬럼 식별
- 정제 및 집계 전략 검증

---

## 사용 데이터셋

### ① hybrid_manufacturing_categorical.csv

#### 데이터 개요
- **데이터 출처**: Kaggle
- **데이터 설명**:  
  적층 가공(Additive Manufacturing)과  
  절삭 가공(Subtractive Manufacturing)을 통합한  
  **하이브리드 제조 시스템(Hybrid Manufacturing System, HMS)**의  
  생산 계획 및 최적화 관련 데이터

#### EDA 주요 목적
- 범주형 변수 구조 및 카디널리티 확인
- PK 후보 컬럼 식별
- Gold 단계 KPI 테이블 설계 검증

---

### ② manufacturing_defect_dataset.csv

#### 데이터 개요
- **데이터 출처**: Kaggle
- **데이터 설명**:  
  제조 환경에서 **불량률(Defect Rate)에 영향을 미치는 요인**을 분석하기 위한 데이터

#### EDA 주요 목적
- 수치형 지표 분포 및 이상값 확인
- 주요 변수 간 상관관계 분석
- Gold 단계 KPI 테이블 설계 검증

---

## 주요 EDA 수행 항목

본 EDA에서는 다음 항목을 중심으로 분석을 수행하였습니다.

### 1. 데이터 구조 파악
- 컬럼 수 및 데이터 타입 확인
- 범주형 / 수치형 컬럼 분리

---

### 2. Data Profiling 결과 검증
- Profiling 단계에서 제안된 **Snowflake 데이터 타입**
- 원본 CSV 데이터 타입 비교
- 최종 **Snowflake 컬럼 데이터 타입 결정**

---

### 3. PK 후보 컬럼 식별
- 고유값 비율 분석
- 기존 데이터에 명확한 PK가 존재하지 않는 경우, 신규 식별자 컬럼을 생성하여 PK로 활용
- 향후 Fact / Dimension 테이블 설계 반영

---

### 4. 결측치 및 고유값 분석
- 결측치 발생 원인 추정
- 결측치 처리 여부 판단 (유지 / 제거 / 대체)
- 특정 값 편중(0, 동일 값 반복) 원인 분석

---

### 5. 집계·요약 및 시각화
- 주요 지표 집계 및 요약 통계 확인
- 상관관계 분석을 통한 지표 활용 가능성 검증
- Gold Layer KPI 테이블 대략 설계

---

## 설계 반영 결과

EDA 결과는 Snowflake Medallion Architecture 설계에 다음과 같이 반영

- **Bronze Layer**
  - 원본 데이터 구조 유지
  - 타입 최소 변환

- **Silver Layer**
  - 컬럼명 표준화
  - 데이터 타입 정제
  - 결측치 / 이상값 처리 규칙 적용

- **Gold Layer**
  - KPI 후보 지표 정의
  - 집계 기준 확정
  - 시각화 대상 테이블 설계

---

## 관련 노트북

- `eda_hybrid_manufacturing_categorical.ipynb`
- `eda_manufacturing_defect_dataset.ipynb`

각 노트북에는 데이터셋별 상세 분석 과정과 시각화 결과가 포함