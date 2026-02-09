airflow-project/README.md:
markdown# 📈 Airflow 주식 데이터 파이프라인

## 프로젝트 개요
Airflow로 자동화된 주식 데이터 수집 파이프라인

## 아키텍처
Yahoo Finance API
↓
Airflow Scheduler
↓ (매일 9시 자동 실행)
fetch_stock_data → save_to_db → validate_data → generate_summary
↓
PostgreSQL

## DAG 목록

### 1. stock_pipeline_dag
- 스케줄: 평일 오전 9시
- Task: 수집 → 저장 → 검증 → 요약
- 재시도: 3회

### 2. hello_world_dag
- 스케줄: 매일
- 목적: Airflow 테스트

### 3. failure_test_dag
- 스케줄: 수동
- 목적: 실패 처리 테스트

## 실행 방법

### 1. Airflow 시작
```bash
docker-compose up -d
```

### 2. UI 접속
http://localhost:8080
- ID: airflow
- PW: airflow

### 3. DAG 실행
DAG 토글 ON → Trigger DAG

## 모니터링

### Grid 뷰
- 실행 이력 확인
- 성공/실패 한눈에 파악

### Gantt 차트
- Task별 실행 시간
- 병목 지점 분석

## 배운 점
- DAG 설계 & Task 의존성
- 스케줄러 설정 (Cron)
- 실패 처리 & 재시도
- XCom으로 데이터 전달

## 다음 단계
- Kafka로 실시간 처리
- dbt로 데이터 모델링
- 알람 시스템 (Slack)