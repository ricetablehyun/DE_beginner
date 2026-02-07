# 📈 주식 데이터 파이프라인

## 프로젝트 개요
Yahoo Finance API에서 삼성전자 주식 데이터를 30초마다 수집하여 Postgres에 저장하고 Streamlit으로 시각화하는 파이프라인

## 아키텍처
Yahoo Finance API
↓
Python Collector (yfinance)
↓
PostgreSQL (Docker)
↓
SQL 분석
↓
Streamlit 대시보드

## 기술 스택
- Python 3.10+
- PostgreSQL 15
- Docker
- yfinance, pandas, streamlit

## 설치 & 실행

### 1. Postgres 실행
```bash
docker run --name stock-db -e POSTGRES_DB=stock -e POSTGRES_USER=deuser -e POSTGRES_PASSWORD=depass123 -p 5432:5432 -d postgres:15
```

### 2. 패키지 설치
```bash
pip install -r requirements.txt
```

### 3. 데이터 수집
```bash
python stock_collector.py  # 테스트 (10회)
python auto_collector.py   # 자동화 (30초마다)
```

### 4. 대시보드
```bash
streamlit run dashboard.py
```

## 배운 점
- 데이터 파이프라인 전체 흐름 이해
- 에러 핸들링의 중요성
- 자동화의 편리함
- SQL 집계 쿼리 활용

## 다음 단계
- Airflow로 업그레이드
- Kafka 실시간 처리
- 여러 종목 동시 수집