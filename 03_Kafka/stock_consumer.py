# stock_consumer.py
from kafka import KafkaConsumer
import json
from datetime import datetime

# ==========================================
# 1. 앵커석 세팅 (Consumer 생성)
# ==========================================
consumer = KafkaConsumer(
    'stock-prices',              # "방송국(Kafka), 'stock-prices' 채널 틀어주세요!"
    bootstrap_servers='localhost:9092', # 카프카 접속 문 (9092번)
    
    # [중요] 'latest' vs 'earliest'
    # latest: "지금부터 들어오는 속보만 볼래!" (생방송)
    # earliest: "못 본 옛날 뉴스부터 다 보여줘!" (다시보기)
    auto_offset_reset='latest',  
    
    enable_auto_commit=True,     # "읽은 건 읽었다고 자동 체크해줘"
    group_id='stock-monitor-group', # "우리 팀 이름은 모니터링반이야"
    
    # [중요] 포장 뜯기 (역직렬화)
    # 0과 1(Bytes) -> 글자(String) -> 딕셔너리(JSON)로 복구!
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📥 주식 데이터 실시간 모니터링 시작! (기다리는 중...)")
print("=" * 60)

# ==========================================
# 2. 기억장치 준비 (통계 계산용)
# ==========================================
prices = []   # 가격들을 기록할 공책
volumes = []  # 거래량을 기록할 공책

# ==========================================
# 3. 생방송 시작 (무한 루프)
# ==========================================
# 카프카에 새 데이터가 들어올 때까지 여기서 '일시정지'하고 기다림
for message in consumer:
    # 1) 쪽지 내용물 꺼내기
    data = message.value
    
    # 2) 공책에 적기 (통계용)
    prices.append(data['price'])
    volumes.append(data['volume'])
    
    # 3) [핵심] 기억력 관리 (최근 10개만 기억하기)
    # 데이터가 10개를 넘으면, 제일 옛날 거 지우고 최근 10개만 남김
    if len(prices) > 10:
        prices = prices[-10:]
        volumes = volumes[-10:]
    
    # 4) 화면에 속보 출력
    print(f"\n⏰ 시간: {data['timestamp']}")
    print(f"💰 종목: {data['symbol']} | 현재가: {data['price']:,.0f}원")
    print(f"📊 고가: {data['high']:,.0f} | 저가: {data['low']:,.0f}")
    print(f"📈 거래량: {data['volume']:,}주")
    
    # 5) 실시간 분석 (평균 계산)
    # 데이터가 2개 이상 모였을 때만 계산
    if len(prices) > 1:
        avg_price = sum(prices) / len(prices)     # 가격 평균
        avg_volume = sum(volumes) / len(volumes)  # 거래량 평균
        
        print(f"\n📉 [분석] 최근 {len(prices)}건 평균:")
        print(f"   - 평균 주가: {avg_price:,.0f}원")
        print(f"   - 평균 거래량: {avg_volume:,.0f}주")
    
    print("=" * 60)

# 방송 종료 (사실 Ctrl+C 누르기 전엔 안 옴)
consumer.close()