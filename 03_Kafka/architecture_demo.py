# architecture_demo.py
"""
Lambda Architecture 데모:
  - 실시간 Layer (Kafka)
  - 배치 Layer (Airflow - 시뮬레이션)
  - Serving Layer (Postgres)
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta

def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        dbname='stock',
        user='deuser',
        password='depass123'
    )

print("📊 Lambda Architecture 통합 대시보드")
print("=" * 60)

conn = get_db_connection()

# 1. 실시간 Layer (최근 5분)
print("\n🔴 실시간 Layer (Kafka → Postgres)")
realtime_df = pd.read_sql("""
    SELECT 
        symbol,
        COUNT(*) as count,
        AVG(price) as avg_price,
        MAX(price) as max_price,
        MIN(price) as min_price
    FROM realtime_stock_prices
    WHERE received_at >= NOW() - INTERVAL '5 minutes'
    GROUP BY symbol
""", conn)

if not realtime_df.empty:
    print(realtime_df.to_string(index=False))
else:
    print("   데이터 없음 (Producer 실행 확인)")

# 2. 배치 Layer (전체 이력)
print("\n🔵 배치 Layer (Airflow → Postgres)")
batch_df = pd.read_sql("""
    SELECT 
        symbol,
        DATE(timestamp) as date,
        COUNT(*) as count,
        AVG(price) as avg_price
    FROM stock_prices
    WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY symbol, date
    ORDER BY date DESC
    LIMIT 7
""", conn)

if not batch_df.empty:
    print(batch_df.to_string(index=False))
else:
    print("   데이터 없음 (Airflow DAG 실행 확인)")

# 3. Serving Layer (1분 집계)
print("\n🟢 Serving Layer (집계 결과)")
serving_df = pd.read_sql("""
    SELECT 
        symbol,
        minute,
        avg_price,
        count,
        total_volume
    FROM minute_aggregates
    ORDER BY minute DESC
    LIMIT 10
""", conn)

if not serving_df.empty:
    print(serving_df.to_string(index=False))
else:
    print("   데이터 없음")

print("\n" + "=" * 60)

# 4. 통합 분석
print("\n📈 통합 분석")

# 실시간 vs 배치 비교
print("\n실시간 평균가:")
if not realtime_df.empty:
    print(f"  {realtime_df.iloc[0]['avg_price']:,.0f}원")

print("\n배치 평균가 (오늘):")
if not batch_df.empty:
    today = batch_df[batch_df['date'] == datetime.now().date()]
    if not today.empty:
        print(f"  {today.iloc[0]['avg_price']:,.0f}원")

conn.close()

print("\n" + "=" * 60)
print("✅ 통합 대시보드 완료!")