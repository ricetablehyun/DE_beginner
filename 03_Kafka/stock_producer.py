# stock_producer_fake.py
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🎲 가짜 삼성전자 데이터!")
print("=" * 60)

base_price = 71000

def fetch_and_send():
    global base_price
    
    base_price += random.randint(-200, 200)
    
    message = {
        'symbol': '005930',
        'price': base_price,
        'open': base_price - random.randint(0, 100),
        'high': base_price + random.randint(0, 300),
        'low': base_price - random.randint(0, 300),
        'volume': random.randint(100000, 500000),
        'timestamp': datetime.now().isoformat()
    }
    
    producer.send('stock-prices', value=message)
    
    print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] "
          f"삼성전자: {message['price']:,}원 (가짜)")
    
    return True

try:
    count = 0
    while True:
        fetch_and_send()
        count += 1
        print(f"📊 총 {count}개")
        time.sleep(3)
        
except KeyboardInterrupt:
    print("\n🛑 종료!")
    producer.flush()
    producer.close()