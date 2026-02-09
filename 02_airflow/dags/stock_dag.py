# ==========================================
# 0. 도구 상자 (Imports)
# 요리에 필요한 칼, 도마, 재료를 미리 꺼내두는 단계입니다.
# ==========================================
from datetime import datetime, timedelta  # 날짜와 시간을 계산하는 시계
from airflow import DAG  # 작업 지시서(DAG) 양식
from airflow.operators.python import PythonOperator # 파이썬 코드를 실행해주는 로봇
import yfinance as yf  # 주식 정보를 인터넷에서 가져오는 도구 (크롤러)
import psycopg2  # 파이썬과 DB(창고)를 연결해주는 통로
import pandas as pd  # 데이터를 표(엑셀)처럼 다루는 도구

# ==========================================
# 1. 작업 규칙 설정 (DAG Settings)
# 이 공장이 어떻게 돌아갈지 규칙을 정합니다.
# ==========================================
default_args = {
    'owner': 'de-student',      # 작업 반장 이름
    'depends_on_past': False,   # "어제 망했어도 오늘은 그냥 실행해!" (과거 의존 X)
    'start_date': datetime(2024, 2, 1), # "2월 1일부터 일한 걸로 쳐줘"
    'email_on_failure': False,  # 실패해도 이메일 보내지 마 (귀찮으니까)
    'retries': 3,               # [중요] 실패하면 3번까지는 다시 시도해봐 (끈기 있는 로봇)
    'retry_delay': timedelta(minutes=5), # 재시도는 5분 쉬고 나서 해
}

# 작업 지시서(DAG) 만들기
dag = DAG(
    'stock_pipeline_dag',       # 이 작업의 ID (이름표)
    default_args=default_args,  # 위에서 정한 규칙 적용
    description='주식 데이터 수집 파이프라인',
    # [설명] 크론탭 문법: "분 시 일 월 요일"
    # 0 9 * * 1-5 = 매일 0분 9시, 월(1)~금(5)요일에만 실행
    schedule_interval='0 9 * * 1-5', 
    catchup=False,              # "과거에 안 돌린 거 굳이 몰아서 하지 마" (오늘 것만 해)
)

# ==========================================
# 2. 공통 함수: DB 연결 (Connection)
# 창고(DB) 문을 여는 열쇠입니다. 모든 Task가 이 열쇠를 씁니다.
# ==========================================
def get_db_connection():
    return psycopg2.connect(
        # [중요] host.docker.internal
        # 설명: 도커(컨테이너) 안에서 밖(내 맥북)에 있는 DB를 찾으려면 이 주소를 써야 함.
        # 비유: "방 안에서 거실에 있는 냉장고를 여는 마법의 주문"
        # host='host.docker.internal',  
        host='postgres',
        port=5432,
        dbname='stock',        # 창고 이름
        user='deuser',         # 창고지기 아이디
        password='depass123'   # 창고지기 비밀번호
    )

# ==========================================
# 3. Task 1: 데이터 수집 (Fetch)
# 역할: "기자" - 인터넷에서 삼성전자 주가를 알아옵니다.
# ==========================================
def fetch_stock_data(**context):
    print("📡 주식 데이터 수집 시작...")
    
    symbol = "005930.KS"  # 삼성전자 종목 코드
    
    # 1. yfinance 도구로 인터넷에서 정보 가져오기
    stock = yf.Ticker(symbol)
    data = stock.history(period="1d", interval="1m") # 오늘 하루치, 1분 단위 데이터
    
    # 2. 데이터가 있으면 정리해서 포장하기
    if not data.empty:
        latest = data.iloc[-1]  # 가장 최근 시간의 데이터 1줄 뽑기
        
        # 딕셔너리(Dictionary) 형태로 예쁘게 포장
        result = {
            'symbol': symbol.split('.')[0], # 005930 (뒤에 .KS 떼기)
            'price': float(latest['Close']), # 종가 (끝난 가격)
            'change_rate': float((latest['Close'] - latest['Open']) / latest['Open'] * 100), # 등락률
            'volume': int(latest['Volume']) # 거래량
        }
        print(f"✅ 수집 성공: {result}")
        
        # [핵심] XCom으로 다음 사람에게 전달하기
        # 비유: 기자가 취재 수첩(result)을 사물함(XCom)에 넣어둠.
        # "야, 다음 타자(Task 2)! 내가 'stock_data'라는 이름으로 사물함에 넣어놨어!"
        context['ti'].xcom_push(key='stock_data', value=result) # 🐚 hello_dag에서는 return을 통해 창고에 넣었는데 그래도 되긴함. 근데 단점이 그럼 retrun_value가 뭔지 헷갈릴수있음. 
        # 🐚 따라서 여기서는 key를쓰기위해 즉 주는사람을 명시하고자 이런방식으로 진행한것이다.
        
        return result
    else:
        # 데이터가 없으면 에러를 내서 작업을 멈춤 (빨간불 뜸)
        raise ValueError("데이터 수집 실패! 인터넷이 끊겼거나 장이 안 열렸나?")

fetch_task = PythonOperator(
    task_id='fetch_stock_data',     # Airflow 화면에 뜨는 이름
    python_callable=fetch_stock_data, # 실행할 함수
    provide_context=True,           # "사물함(XCom) 열쇠(**context) 줄 테니까 써라"
    dag=dag,
)

# ==========================================
# 4. Task 2: 데이터 저장 (Save)
# 역할: "서기" - 기자가 가져온 정보를 장부(DB)에 기록합니다.
# ==========================================
def save_to_db(**context):
    print("💾 데이터베이스 저장 시작...")
    
    # 1. 사물함(XCom) 열어서 기자가 두고 간 쪽지 꺼내기
    ti = context['ti']
    # "fetch_stock_data 작업이 남긴 stock_data 쪽지 내놔"
    stock_data = ti.xcom_pull(task_ids='fetch_stock_data', key='stock_data')
    
    if not stock_data:
        raise ValueError("저장할 데이터 없음! 기자가 쪽지를 안 남겼어!")
    
    # 2. DB(장부) 펼치기
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 3. 테이블(페이지) 만들기 (없으면 새로 만듦)
    # IF NOT EXISTS: "이미 있으면 만들지 마" (에러 방지용 안전장치)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            price DECIMAL(10,2),
            change_rate DECIMAL(5,2),
            volume BIGINT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)  -- 중복 방지 규칙
        );
    """)
    
    # 4. 데이터 적기 (INSERT)
    # ON CONFLICT DO NOTHING: "똑같은 시간에 똑같은 데이터가 이미 있으면? 무시해!" (중복 방지)
    cur.execute("""
        INSERT INTO stock_prices (symbol, price, change_rate, volume)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
    """, (
        stock_data['symbol'],
        stock_data['price'],
        stock_data['change_rate'],
        stock_data['volume']
    ))
    
    # 5. 저장 확정(Commit) 하고 장부 덮기
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"✅ 저장 완료: {stock_data['symbol']} {stock_data['price']}원")

save_task = PythonOperator(
    task_id='save_to_db',
    python_callable=save_to_db,
    provide_context=True,
    dag=dag,
)

# ==========================================
# 5. Task 3: 데이터 검증 (Validate)
# 역할: "검사관" - 서기가 제대로 적었는지 확인합니다.
# ==========================================
def validate_data(**context):
    print("🔍 데이터 검증 시작...")
    
    conn = get_db_connection()
    
    # 1. 오늘 날짜로 저장된 데이터가 몇 개인지 세어보기
    # SQL: "stock_prices 테이블에서 오늘 날짜 이후 데이터 개수(Count) 세어봐"
    df = pd.read_sql("""
        SELECT COUNT(*) as count
        FROM stock_prices
        WHERE timestamp >= CURRENT_DATE
    """, conn)
    conn.close()
    
    count = df.iloc[0]['count']
    print(f"✅ 오늘 데이터 {count}개 확인")
    
    # 2. 만약 0개라면? 비상 사태 선포!
    if count == 0:
        raise ValueError("오늘 데이터 없음! 서기가 땡땡이쳤음!")
    
    return f"검증 완료: {count}개"

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

# ==========================================
# 6. Task 4: 일별 요약 생성 (Summary)
# 역할: "편집장" - 하루치 데이터를 모아서 요약 리포트를 만듭니다.
# ==========================================
def generate_summary(**context):
    print("📊 요약 생성 시작...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. 요약용 테이블 만들기
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_summary (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            date DATE,
            avg_price DECIMAL(10,2), -- 평균 가격
            max_price DECIMAL(10,2), -- 최고가
            min_price DECIMAL(10,2), -- 최저가
            total_volume BIGINT,     -- 총 거래량
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # 2. 통계 내기 (Pandas + SQL 콜라보)
    # AVG(평균), MAX(최대), MIN(최소), SUM(합계) 계산
    df = pd.read_sql("""
        SELECT 
            symbol,
            CURRENT_DATE as date,
            AVG(price) as avg_price,
            MAX(price) as max_price,
            MIN(price) as min_price,
            SUM(volume) as total_volume
        FROM stock_prices
        WHERE timestamp >= CURRENT_DATE
        GROUP BY symbol
    """, conn)
    
    # 3. 계산된 요약 정보를 요약 테이블에 저장
    if not df.empty:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO stock_summary (symbol, date, avg_price, max_price, min_price, total_volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                row['symbol'],
                row['date'],
                row['avg_price'],
                row['max_price'],
                row['min_price'],
                row['total_volume']
            ))
        conn.commit()
        print(f"✅ 요약 생성 완료: {len(df)}개 종목")
    
    cur.close()
    conn.close()

summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    provide_context=True,
    dag=dag,
)

# ==========================================
# 7. 순서 연결 (Dependency)
# "기자(Fetch) -> 서기(Save) -> 검사관(Validate) -> 편집장(Summary)" 순서로 일해라!
# ==========================================
fetch_task >> save_task >> validate_task >> summary_task