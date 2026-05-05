DE_beginner — 배치에서 실시간 스트리밍까지

데이터 엔지니어링의 3단계 진화: 수동 수집 → 스케줄 자동화 → 실시간 스트리밍


학습 로드맵
01_stock_pipeline   →   02_airflow          →   03_Kafka
"내가 직접 돌린다"      "스케줄러가 대신 돌린다"   "데이터가 실시간으로 흐른다"
(Batch / Manual)        (Batch / Scheduled)       (Streaming / Real-time)

01. 주식 데이터 수집 파이프라인
문제: Yahoo Finance에서 삼성전자 주가를 자동으로 수집해 DB에 쌓으려면?
아키텍처
Yahoo Finance API
      ↓
Python Collector (yfinance) — 30초마다 수집
      ↓
PostgreSQL (Docker) — 실시간 가격 / 일별 요약 분리 저장
      ↓
Streamlit 대시보드
핵심 설계 결정

stock_prices (원시) + stock_summary (집계) 테이블 분리 → 대시보드 조회 속도 확보
ON CONFLICT DO NOTHING → 중복 수집 방어
.env 환경변수 분리 → 보안 원칙 준수

기술 스택: Python · PostgreSQL · Docker · yfinance · Streamlit

02. Airflow 스케줄 자동화
문제: 파이프라인을 내가 직접 실행하지 않아도 매일 자동으로 돌아가게 하려면?
아키텍처
Airflow Scheduler (Docker)
      ↓
fetch_stock_data → save_to_db → validate_data → generate_summary
      ↓  (평일 오전 9시 KST 자동 실행, 실패 시 3회 재시도)
PostgreSQL
DAG 목록
DAG스케줄목적stock_pipeline_dag평일 09:00수집 → 저장 → 검증 → 요약hello_world_dag매일Airflow 연결 테스트failure_test_dag수동실패/재시도 핸들링 확인
핵심 학습: XCom 데이터 전달 / Cron 스케줄링 / Task 의존성 설계
기술 스택: Apache Airflow · Docker Compose · PostgreSQL

03. Kafka 실시간 스트리밍
문제: 배치(batch)가 아닌 실시간으로 주가 변동을 처리하려면?
아키텍처
네이버/야후 금융 API
      ↓
Producer (Python) — 실시간 주가 메시지 발행
      ↓
Kafka Broker (Confluent 7.5.0, Docker)
      ↓ ↓
Consumer A              Consumer B
(Raw 저장)              (1분 윈도우 집계: AVG/MAX/MIN)
      ↓                       ↓
          PostgreSQL (영구 저장)
                ↓
          Kafka-UI (Port 8090) 모니터링
핵심 학습

Kafka Listeners: 내부망(29092) / 외부망(9092) 분리 이유 이해
Serialization: Python dict → bytes 직렬화/역직렬화
Lambda Architecture: 실시간(Speed Layer) + 배치(Batch Layer) 병렬 운영

기술 스택: Apache Kafka · Python (kafka-python) · Docker Compose · PostgreSQL

실행 환경
bash# Python 3.10+
pip install yfinance psycopg2-binary pandas streamlit python-dotenv kafka-python

# Docker (Airflow / Kafka)
docker-compose up -d
