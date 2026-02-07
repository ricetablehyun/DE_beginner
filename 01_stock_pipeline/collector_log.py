# collector_log.py
import logging
import schedule
import time
from stock_collector import fetch_stock_data, save_stock_data, init_db

# 로그 설정
logging.basicConfig(
    filename='collector.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

init_db()

def collect_job():
    try:
        logging.info("수집 시작")
        stock = fetch_stock_data()
        save_stock_data(stock)
        logging.info(f"성공: {stock}")
    except Exception as e:
        logging.error(f"오류 발생: {e}")

schedule.every(30).seconds.do(collect_job)
collect_job()

while True:
    schedule.run_pending()
    time.sleep(1)