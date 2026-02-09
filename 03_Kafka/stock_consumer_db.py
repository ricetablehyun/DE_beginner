# stock_consumer_db.py
from kafka import KafkaConsumer
import json
import psycopg2 # 파이썬에서 DB(Postgres)를 다루는 도구
from datetime import datetime, timedelta
from collections import defaultdict

# ==========================================
# 1. 서기 채용 (Consumer 설정)
# 🐚 stock_consumer 코드는 그냥 print()찍음 DB에 저장안하고.
# ==========================================
consumer = KafkaConsumer(
    'stock-prices',              # "stock-prices" 채널을 듣겠습니다.
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  # "지금부터 들어오는 것만 처리합니다."
    enable_auto_commit=True,
    group_id='stock-db-group',   # "제 소속 팀은 DB 저장팀입니다."
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) # 포장 뜯기
)

# ==========================================
# 2. 창고(DB) 열쇠 준비
# ==========================================
def get_db_connection():
    # Airflow 프로젝트 때 띄워둔 Postgres DB에 접속합니다.
    return psycopg2.connect(
        host='localhost',    # 내 컴퓨터(로컬)
        port=5432,           # DB 문 번호
        dbname='stock',      # 창고 이름 (미리 만들어져 있어야 함, 없으면 'postgres'로 변경)
        user='deuser',       # 아이디
        password='depass123' # 비밀번호
    )

# ==========================================
# 3. 장부(Table) 만들기
# ==========================================
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # (1) 실시간 데이터 장부: 들어오는 족족 다 적는 곳
    cur.execute("""
        CREATE TABLE IF NOT EXISTS realtime_stock_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            price DECIMAL(10,2),
            open_price DECIMAL(10,2),
            high DECIMAL(10,2),
            low DECIMAL(10,2),
            volume BIGINT,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # (2) 1분 요약 장부: 1분마다 통계 내서 적는 곳 (여기가 핵심!) ⭐️
    cur.execute("""
        CREATE TABLE IF NOT EXISTS minute_aggregates (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            minute TIMESTAMP,
            avg_price DECIMAL(10,2), -- 평균가
            max_price DECIMAL(10,2), -- 최고가
            min_price DECIMAL(10,2), -- 최저가
            total_volume BIGINT,     -- 총 거래량
            count INT,               -- 데이터 개수
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, minute)   -- (중복 방지) 같은 시간에 같은 종목은 1개만!
        );
    """)
    
    conn.commit() # "저장!" 도장 쾅
    cur.close()
    conn.close()
    print("✅ DB 장부 준비 완료!")

# ==========================================
# 4. 윈도우(바구니) 준비 🧺
# ==========================================
# 데이터를 1분 동안 잠시 담아둘 바구니입니다.
window_data = defaultdict(list)
window_start = datetime.now()      # 바구니 놓은 시간
WINDOW_SIZE = timedelta(minutes=1) # "1분 뒤에 바구니 비울 거야"

def save_to_db(data):
    """(1)번 장부에 바로 적기"""
    conn = get_db_connection()
    cur = conn.cursor()
    # SQL: "insert into..." (데이터 삽입)
    cur.execute("""
        INSERT INTO realtime_stock_prices (symbol, price, open_price, high, low, volume)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, (
        data['symbol'], 
        data['price'], 
        data.get('open', 0), # 없을 경우 0으로 처리 (안전장치)
        data.get('high', 0), 
        data.get('low', 0), 
        data['volume']
    ))
    conn.commit()
    conn.close()

def save_aggregate(symbol, minute, prices, volumes):
    """(2)번 장부에 요약해서 적기"""
    conn = get_db_connection()
    cur = conn.cursor()
    # SQL: 요약된 정보 저장
    cur.execute("""
        INSERT INTO minute_aggregates (symbol, minute, avg_price, max_price, min_price, total_volume, count)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, minute) DO NOTHING;
    """, (
        symbol, minute, 
        sum(prices) / len(prices), # 평균 계산
        max(prices),               # 최대값
        min(prices),               # 최소값
        sum(volumes),              # 거래량 합계
        len(prices)                # 몇 건인지
    ))
    conn.commit()
    conn.close()

# 시작할 때 장부 한 번 펴기
init_db()

print("📥 실시간 집계 & 저장 시작!")
print("=" * 60)

# ==========================================
# 5. 무한 업무 시작 (Loop)
# ==========================================
for message in consumer:
    data = message.value
    current_time = datetime.now()
    
    # [업무 1] 일단 오는 대로 다 저장한다. (Raw Data)
    save_to_db(data)
    print(f"💾 저장: {data['symbol']} {data['price']:,.0f}원")
    
    # [업무 2] 바구니에 잠시 담아둔다. (Aggregation 준비)
    symbol = data['symbol']
    window_data[symbol].append({
        'price': data['price'],
        'volume': data['volume']
    })
    
    # [업무 3] 1분이 지났는지 확인한다.
    if current_time - window_start >= WINDOW_SIZE:
        print("\n" + "=" * 60)
        print(f"📊 1분 지났다! 요약 정리 시작! ({window_start.strftime('%H:%M:%S')})")
        
        # 바구니에 있는 걸 꺼내서 통계 내기
        for symbol, items in window_data.items():
            prices = [item['price'] for item in items]  # 가격들만 쫘르륵 뽑기
            volumes = [item['volume'] for item in items] # 거래량들만 쫘르륵 뽑기
            
            # 요약 장부에 저장
            save_aggregate(
                symbol,
                window_start.replace(second=0, microsecond=0), # 초 단위 떼고 분 단위로 기록
                prices,
                volumes
            )
            
            # 화면 출력
            print(f"   {symbol}: {len(items)}건 요약 완료")
            print(f"   평균: {sum(prices)/len(prices):,.0f}원 / 최고: {max(prices):,.0f}원")
        
        # 바구니 비우기 & 타이머 리셋
        window_data.clear()
        window_start = current_time
        
        print("=" * 60 + "\n")

consumer.close()