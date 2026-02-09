from kafka import KafkaConsumer
import json

# 1. 카프카 연결 설정 (손님 대기)
consumer = KafkaConsumer(
    'test-topic', # 🐚 보내는놈이랑 이름이 같아야 받는거임. "나는 'test-topic'이라는 우편함만 감시할 거야!" (구독)
    bootstrap_servers='localhost:9092', # 카프카 서버 위치
    auto_offset_reset='earliest',  # "내가 늦게 왔으면, 옛날 것부터 다시 보여줘!" (처음부터 읽기)
    enable_auto_commit=True,       # "읽은 건 읽었다고 자동으로 체크해줘."
    group_id='my-group',           # "우리 팀 이름은 my-group이야." (팀끼리 업무 분담할 때 씀)
    # [중요] 역직렬화(Deserializer): 0과 1(바이트)을 다시 파이썬 딕셔너리로 까는 법
    # 포장지를 뜯어야 내용물을 보니까!
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📥 Consumer 시작! (데이터를 기다립니다)")
print("=" * 50)
print("메시지 수신 중... (Ctrl+C 누를 때까지 안 꺼짐)")
print("=" * 50)

# 2. 무한 루프 (기다림의 미학)
# KafkaConsumer는 for문을 돌리면, 새 메시지가 올 때까지 여기서 '일시정지'하고 기다림.
for message in consumer:
    # 3. 데이터 꺼내기
    data = message.value
    
    # 받은 거 출력하기
    print(f"✅ 수신 완료: {data}")
    
    # (참고) 여기에는 보통 DB에 저장하거나 분석하는 코드가 들어감

consumer.close()