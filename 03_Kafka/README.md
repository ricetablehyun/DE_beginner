# 🚀 DE 파이프라인 통합 프로젝트

## 프로젝트 개요
3개 프로젝트를 통합한 완전한 데이터 엔지니어링 파이프라인

## 아키텍처

### 1. 배치 Layer (Python + Postgres)
- 주기적 데이터 수집
- 히스토리 관리

### 2. 자동화 Layer (Airflow)
- 스케줄링
- 실패 처리
- 모니터링

### 3. 실시간 Layer (Kafka)
- 실시간 스트리밍
- 1분 윈도우 집계
- 즉시 저장

## 기술 스택
- Python 3.10+
- PostgreSQL 15
- Apache Kafka 7.5
- Apache Airflow 2.8
- Docker

## 실행 방법

### 1. Kafka 시작
```bash
cd kafka-project
docker-compose up -d
```

### 2. Airflow 시작
```bash
cd airflow-project
docker-compose up -d
```

### 3. 실시간 스트리밍
```bash
# Producer
python stock_producer.py

# Consumer (DB 저장)
python stock_consumer_db.py
```

### 4. 통합 대시보드
```bash
python architecture_demo.py
```

## 학습 내용
- 배치 vs 실시간 처리
- Lambda Architecture
- Kafka Producer/Consumer
- Airflow DAG 설계
- 실시간 윈도우 집계

## 다음 단계
- Week 4: dbt 데이터 모델링
- Week 5: AWS Cloud 배포
- Week 7: 데이터 품질 관리
- Week 8: 포트폴리오 완성