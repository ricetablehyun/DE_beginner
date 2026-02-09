from kafka import KafkaProducer
import json
import time
from datetime import datetime

# 1. 카프카 연결 설정 (주방장 출근)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', # "카프카 서버(오토바이)는 내 컴퓨터 9092번 항구에 있어!"
    # [중요] 직렬화(Serializer): 파이썬 객체(딕셔너리)를 0과 1(바이트)로 포장하는 법
    # 카프카는 0과 1밖에 모르는 바보라서, 우리가 JSON을 바이트로 바꿔줘야 함.
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📤 Producer 시작! (데이터를 보냅니다)")
print("=" * 50)

# 2. 메시지 10개 만들기 (피자 10판 굽기)
for i in range(10):
    # 보낼 데이터 (내용물)
    message = {
        'id': i,
        'message': f'안녕하세요! {i+1}번째 메시지',
        'timestamp': datetime.now().isoformat()
    }
    
    # 3. 전송 (Send)
    # 'test-topic'이라는 우편함(토픽)에 메시지를 넣음 ⭐️ 주고 받는놈들 이름이 같아야힘.
    producer.send('test-topic', value=message)
    
    print(f"✅ 전송 완료: {message}")
    time.sleep(1) # 1초 쉬고 보내기 (너무 빠르면 정신없으니까)

# 4. 마무리 (퇴근)
producer.flush() # "야, 파이프에 남은 거 있으면 싹 다 밀어넣어!" (잔반 처리)
producer.close() # 연결 끊기

print("=" * 50)
print("🎉 전송 끝!")