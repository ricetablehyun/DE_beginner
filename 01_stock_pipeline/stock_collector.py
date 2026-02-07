# stock_collector.py
import yfinance as yf
import psycopg2
import pandas as pd
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# DB 연결 함수
def get_db_connection():
    """데이터베이스 연결"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )

# 테이블 생성
def init_db():
    """주식 데이터 저장 테이블 생성"""
    print("📊 데이터베이스 초기화 중...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 실시간 주식 가격 테이블
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            price DECIMAL(10,2),
            change_rate DECIMAL(5,2),
            volume BIGINT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
    """)
    
    # 일별 요약 테이블
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_summary (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            date DATE,
            avg_price DECIMAL(10,2),
            max_price DECIMAL(10,2),
            min_price DECIMAL(10,2),
            total_volume BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ 테이블 생성 완료!")

# 데이터 수집 
def fetch_stock_data(symbol="005930.KS"):
    try:
        print(f"📡 {symbol} 데이터 수집 중...")
        
        # 1. 삼성전자(symbol)라는 낚싯대를 야후 바다에 던집니다.
        stock = yf.Ticker(symbol) 
        
        # 2. 오늘(1d) 데이터를 1분 단위(1m)로 쪼개진 '엑셀 표' 형태로 가져옵니다.
        #    이 표에는 [시간, 시가, 고가, 저가, 종가, 거래량] 컬럼들이 들어있어요.
        data = stock.history(period="1d", interval="1m")  #🐚 1분마다 수집하는게 아니라 1분으로 그룹바이 한거임.
        
        # 3. 만약 가져온 표가 텅 비어있지 않다면 (장이 열려 있다면)
        if not data.empty:
            # 4. 표의 가장 마지막 줄(iloc[-1]) 즉, '지금 이 순간'의 1분 데이터를 선택합니다.
            latest = data.iloc[-1]
            
            # 5. 복잡한 표 형태에서 내가 필요한 정보만 뽑아 '딕셔너리(Key:Value)'로 정리합니다.
            result = {
                # '005930.KS'에서 점 앞의 '005930'만 잘라냅니다.
                'symbol': symbol.split('.')[0], 
                # 그 1분 동안의 마지막 가격(Close)을 실수(float)로 저장합니다.
                'price': float(latest['Close']), 
                # (종가 - 시가) / 시가 공식으로 가격 변동률을 계산합니다.
                'change_rate': float((latest['Close'] - latest['Open']) / latest['Open'] * 100),
                # 그 1분 동안 거래된 주식 수를 정수(int)로 저장합니다.
                'volume': int(latest['Volume'])
            }
            # 6. 정리가 끝난 '포스트잇(result)'을 함수 밖으로 던져줍니다.
            print(f"✅ 수집 성공: {result['price']:,.0f}원")
            return result
        else:
            # 장이 닫혔거나 통신이 안 되면 "데이터 없음"을 알립니다.
            print("⚠️  데이터 없음 (장 마감 시간일 수 있음)")
            return None
            
    except Exception as e:
        print(f"❌ API 오류: {e}")
        return None

# 데이터 저장
# 🏗️ SQL의 %s는 왜 쓰는 걸까? (Safety First)
# VALUES ('삼성', 70000...) 처럼 직접 쓰지 않고 %s라는 구멍을 뚫어놓는 이유는 보안(SQL Injection 방지) 때문입니다.
# [Fact]: psycopg2 라이브러리는 %s 자리에 들어올 데이터를 검사하여 해킹 위험이 있는 문자를 걸러줍니다.
# [Opinion]: 직접 문자열을 더해서 SQL을 만드는 것은 매우 위험한 습관이므로, 반드시 이 방식을 고수해야 '시니어급' 데이터 엔지니어로 성장할 수 있습니다.

def save_stock_data(stock_data):
    # 1. 수집된 데이터가 None(빈 값)이면 저장하지 않고 바로 함수를 종료합니다.
    if not stock_data:
        print("⏭️  저장할 데이터 없음")
        return
    
    try:
        # 2. 미리 만들어둔 연결 함수를 통해 DB로 가는 '통로'를 엽니다.
        conn = get_db_connection()
        # 3. SQL 명령어를 전달할 '심부름꾼(Cursor)'을 생성합니다.
        cur = conn.cursor()
        
        # 4. 실제 DB에 데이터를 넣는 SQL 문을 작성합니다. execute: table에 쓰기
        cur.execute("""
            INSERT INTO stock_prices (symbol, price, change_rate, volume)
            VALUES (%s, %s, %s, %s)
            -- [핵심] 중복 방지: 같은 종목과 시간 데이터가 이미 있으면 무시합니다.
            ON CONFLICT (symbol, timestamp) DO NOTHING;
        """, (
            # 5. 오른쪽 괄호의 값들이 위 SQL문의 %s 자리에 순서대로 쏙쏙 들어갑니다.
            stock_data['symbol'],
            stock_data['price'],
            stock_data['change_rate'],
            stock_data['volume']
        ))
        
        # 6. 'Commit'을 해야 DB에 최종적으로 도장이 꽝 찍히며 저장됩니다.
        conn.commit()
        
        # 7. 사용한 심부름꾼과 통로를 닫아줍니다. (중요: 안 닫으면 DB가 힘들어해요!)
        cur.close()
        conn.close()
        
        print(f"💾 저장 완료: {stock_data['symbol']} {stock_data['price']:,.0f}원")
        
    except Exception as e:
        # 저장 과정에서 DB가 꺼져있거나 문법이 틀리면 에러를 출력합니다.
        print(f"❌ 저장 오류: {e}")

# 일별 요약 생성
# 🏗️ 왜 실시간 데이터와 요약 데이터를 따로 관리할까? (Why)
# 성능(Speed): 실시간 테이블(stock_prices)은 1분마다 데이터가 쌓여서 금방 수백만 줄이 됩니다. 대시보드를 그릴 때마다 수백만 줄을 계산하면 너무 느리겠죠?
# 효율(Efficiency): 미리 계산된 요약 테이블(stock_summary)은 하루에 딱 한 줄씩만 생깁니다. 한 달 치를 조회해도 30줄이면 끝나니 속도가 엄청나게 빠릅니다.

def generate_daily_summary():
    """오늘 수집한 데이터의 통계 생성"""
    print("📈 일별 요약 생성 중...")
    
    # 1. DB와 대화하기 위한 통로를 엽니다.
    conn = get_db_connection()
    
    # 2. [핵심] SQL로 데이터를 계산해서 가져옵니다. 
    #    단순히 가져오는 게 아니라 '평균, 최대, 최소'를 DB에서 미리 계산(Aggregate)해서 표(df)로 만듭니다.
    df = pd.read_sql("""
        SELECT 
            symbol,                          -- 종목 이름
            DATE(timestamp) as date,         -- 시간을 '날짜'만 남기고 자릅니다 (예: 2026-02-07)
            AVG(price) as avg_price,         -- 오늘 가격의 '평균'을 구합니다.
            MAX(price) as max_price,         -- 오늘 가격 중 '가장 비싼 값'을 찾습니다.
            MIN(price) as min_price,         -- 오늘 가격 중 '가장 싼 값'을 찾습니다.
            SUM(volume) as total_volume      -- 오늘 하루 동안의 '총 거래량'을 더합니다.
        FROM stock_prices                    -- 실시간 가격 테이블에서 가져오되,
        WHERE timestamp >= CURRENT_DATE      -- '오늘(00시 00분 이후)' 데이터만 골라냅니다.
        GROUP BY symbol, date                -- 종목별, 날짜별로 묶어서 계산합니다.
    """, conn)
    
    # 3. 만약 계산된 결과(통계 표)가 있다면 저장 단계로 넘어갑니다.
    if not df.empty:
        cur = conn.cursor()
        # 4. 표의 한 줄(row)씩 차례대로 읽으면서 저장합니다. (iterrows = 줄 반복하기)
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO stock_summary (symbol, date, avg_price, max_price, min_price, total_volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                -- 이미 해당 날짜의 요약본이 있다면 중복해서 넣지 않습니다.
                ON CONFLICT DO NOTHING; 
            """, (
                row['symbol'],
                row['date'],
                row['avg_price'],
                row['max_price'],
                row['min_price'],
                row['total_volume']
            ))
        # 5. DB에 최종 도장을 찍고 통로를 닫습니다.
        conn.commit()
        cur.close()
        print("✅ 요약 생성 완료!")
    else:
        # 오늘 수집된 실시간 데이터가 하나도 없으면 요약할 게 없다고 알립니다.
        print("⚠️  오늘 데이터 없음")
    
    conn.close()
    
# 메인 실행
if __name__ == "__main__":
    print("🚀 주식 데이터 파이프라인 시작!")
    print("=" * 50)
    
    # 1. 테이블 생성
    init_db()
    
    # 2. 테스트: 10회 수집 (60초 간격)
    print("\n📡 데이터 수집 시작 (10회, 60초 간격)")
    print("=" * 50)
    
    for i in range(10):
        print(f"\n[{i+1}/10회]")
        
        # 수집
        stock = fetch_stock_data()
        
        # 저장
        save_stock_data(stock)
        
        # 대기 (마지막 회차는 대기 안 함)
        if i < 9:
            print("⏳ 60초 대기 중...")
            time.sleep(60)
    
    # 3. 요약 생성
    print("\n" + "=" * 50)
    generate_daily_summary()
    
    print("\n" + "=" * 50)
    print("✅ 첫 테스트 완료!")
    print("👉 다음: DBeaver에서 데이터 확인하기")
    print("=" * 50)