# auto_collector.py
import schedule
import time
from stock_collector import fetch_stock_data, save_stock_data, init_db

print("🤖 자동 수집 시작!")
print("30초마다 데이터 수집 (Ctrl+C로 중지)")
print("=" * 50)

# DB 초기화
init_db()

# 수집 함수
def collect_job():
    print(f"\n⏰ {time.strftime('%H:%M:%S')} - 수집 시작")
    stock = fetch_stock_data()
    save_stock_data(stock)

# 30초마다 실행
schedule.every(30).seconds.do(collect_job)

# 최초 1회 즉시 실행
collect_job()

# 무한 루프
while True:
    schedule.run_pending()
    time.sleep(1)