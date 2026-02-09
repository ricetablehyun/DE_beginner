# 📈 Kafka 실시간 주식 데이터 파이프라인

## 🎯 프로젝트 개요
Airflow를 이용한 **배치(Batch)** 처리를 넘어, Kafka를 이용한 **실시간(Streaming)** 데이터 엔지니어링 환경을 구축했습니다.

## 🏗️ 시스템 아키텍처

1. **Producer**: 네이버/야후 금융 API를 통해 실시간 주가 수집 및 전송
2. **Kafka (Broker)**: 고속 데이터 스트리밍 및 메시지 관리
3. **Consumer**: 
   - Raw 데이터 실시간 DB 저장
   - 1분 단위 윈도우 집계(Average, Max, Min) 후 저장
4. **PostgreSQL**: 실시간 및 집계 데이터 영구 보존

## 🛠️ 핵심 기술 스택
- **Message Broker**: Apache Kafka (Confluent 7.5.0)
- **Orchestration**: Docker Compose
- **Language**: Python (kafka-python, pandas)
- **Monitoring**: Kafka-UI (Port 8090)

## 💡 배운 점
- **Docker Network**: 내부망(29092)과 외부망(9092)을 분리하여 Kafka Listeners 설정 해결
- **Serialization**: 파이썬 딕셔너리를 바이트로 변환하는 직렬화/역직렬화 과정 이해
- **Lambda Architecture**: 실시간 데이터와 배치 데이터의 차이점을 이해하고 통합 대시보드 구현