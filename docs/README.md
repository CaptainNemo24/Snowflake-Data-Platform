# Documentation (docs)

본 디렉터리는 **Snowflake Data Platform 구축 전 과정**을 정리한  
보고서 및 실습 가이드 문서를 관리

Snowflake 기반 Data Platform을 설계·구축하면서 수행한  
기획 → 데이터 이해 → 아키텍처 설계 → 자동화 → 거버넌스  
**전 과정을 문서화**하는 것을 목적

---

## 문서 구성

```plaintext
docs/
├─ Snowflake_Data_Platform_Report.docx
└─ Snowflake_Data_Platform_Hands_on_Guide.docx
```

---

## Snowflake_Data_Platform_Report

### 문서 성격
- **설명 중심의 보고서(Document / Report)**
- Snowflake Data Platform 구축에 대한 **전체 개요 및 설계 의도**를 전달

### 주요 내용
- Snowflake Data Platform 구축 목적
- 전체 데이터 흐름 및 의사결정 배경
- Medallion Architecture 개요
- 사용 데이터 및 분석 대상 설명
- Bronze · Silver · Gold 단계별 설계 방향
- 자동화 및 거버넌스 설계 개념

### 특징
- 개념 및 설계 의도 중심 설명
- 변수명 및 예시는 **추상화된 형태**로 기술
- 외부 Stage 및 세부 설정은 개념 위주로 설명

---

## Snowflake_Data_Platform_Hands_on_Guide

### 문서 성격
- **실습 중심의 가이드(Hands-on Guide)**
- Snowflake Data Platform 구축 전 과정을  
  **직접 실행 가능하도록 구성**

### 주요 내용
- Snowflake 환경 설정
- 외부 Stage 구성 (상세 옵션 포함)
- 실제 변수명 및 SQL 스크립트
- 데이터 적재 및 자동화 파이프라인 구축
- Medallion Architecture 단계별 구현 절차
- 테스트 및 검증 시나리오

### 특징
- Report와 동일한 전체 흐름 유지
- 모든 과정이 **실행 가능한 형태로 구체화**
- 추상화 없이 **실제 설정값과 코드 중심**

---

## 두 문서의 역할 차이

| 구분 | Report | Hands-on Guide |
|---|---|---|
| 목적 | 설계 이해 | 실제 구현 |
| 설명 방식 | 개념 중심 | 실행 중심 |
| 코드 | 예시/추상화 | 실제 실행 코드 |
| 설정 | 개념 설명 | 상세 설정 포함 |

---

## 문서 구성 의도

본 디렉터리는 다음 두 목적을 분리하여 구성

- **설계 의도와 전체 흐름을 이해하기 위한 문서**
- **실제로 실행하며 검증할 수 있는 문서**

---

## 활용 가이드

- Snowflake Data Platform 전체 흐름 이해  
  → `Snowflake_Data_Platform_Report`

- Snowflake 환경에서 직접 구축 진행  
  → `Snowflake_Data_Platform_Hands_on_Guide`
