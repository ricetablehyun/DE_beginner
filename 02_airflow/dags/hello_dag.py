# 1. 도구 상자 열기 (Imports)
from datetime import datetime, timedelta  # 날짜와 시간을 다루는 도구
from airflow import DAG  # 작업 지시서(DAG) 그 자체
from airflow.operators.python import PythonOperator # 파이썬 함수를 실행시켜주는 기계
from airflow.operators.bash import BashOperator     # 터미널 명령어를 실행시켜주는 기계

# 2. 공통 규칙 설정 (Default Args)
# 이 DAG 안에 있는 모든 Task들에게 적용되는 '기본 규칙'입니다.
default_args = {
    'owner': 'de-student',      # 작업 주인 (나중에 로그에서 보임)
    'depends_on_past': False,   # "어제 실패했어도 오늘은 그냥 실행해" (True면 어제 성공해야 오늘 실행됨)
    'start_date': datetime(2024, 1, 1), # "이 작업은 2024년 1월 1일부터 시작된 걸로 쳐" (과거 데이터 채울 때 중요)
    'email_on_failure': False,  # 실패하면 이메일 보낼까? (귀찮으니 끔)
    'email_on_retry': False,    # 재시도할 때 이메일 보낼까?
    'retries': 1,               # 실패하면 1번은 봐준다 (재시도 횟수)
    'retry_delay': timedelta(minutes=5), # 재시도는 5분 뒤에 해라
}

# 3. DAG(작업 지시서) 만들기
# 여기서 실제 'hello_world_dag'라는 이름의 작업 지시서가 생성됩니다.
dag = DAG(
    'hello_world_dag',          # DAG의 ID (Airflow 웹 화면에 이 이름으로 뜸)
    default_args=default_args,  # 위에서 정한 규칙 적용
    description='첫 번째 DAG 테스트', # 설명
    schedule_interval='@daily', # "매일 자정(00:00)에 실행해!" (크론탭 문법)
    catchup=False,              # "과거(1월 1일~오늘) 안 한 거 굳이 하지 마. 오늘부터 해."
)

# ---------------------------------------------------------
# 여기서부터는 실제 작업(Task)들을 정의합니다.
# ---------------------------------------------------------
# ⭐️⭐️⭐️⭐️⭐️ 테스크 별로 작업공간이나 로그기록들이 나눠서 있는거임. 이떄 return을 통해서 공통 저장에 넣는거고 task3은 1이 저장한 거를 불러와서 쓰는 연습을 하는거임. 
# "각 Task는 **자기 방(로그)**에서 일하고, 결과물은 **공용 사물함(XCom)**에 넣어서 공유한다." 병렬 처리라서 통합로그는 존재하지않는다. 
# 공통 창고자체는 매우 작어서 데이터를 넣는다기 보다는 데이터의 위치정보를 저장한다고 보면 된다. XCom: "야, 내가 S3 어디 어디에 저장해놨어"라는 **위치(Path)**만 넘겨주는 겁니다.

# Task 1을 위한 파이썬 함수 정의
def say_hello():
    print("🎉 Hello Airflow!")
    print(f"현재 시간: {datetime.now()}")
    return "Hello World!"  # ★ 🐚 중요: return한 값은 자동으로 Airflow 저장소(XCom)에 저장됨

# Task 1: 파이썬 함수 실행하기
hello_task = PythonOperator(
    task_id='say_hello',        # 이 작업의 이름 (웹 화면에 뜸)  
    python_callable=say_hello,  # 실행할 함수 이름 (위에서 만든 거) say_hello(): 지금 당장 실행해라! (X)
    dag=dag,                    # 이 작업은 'hello_world_dag' 소속이다
)

# Task 2: 터미널 명령어 실행하기 🐚 그냥 커맨드도 쓸수있다는걸 보여주기용 return을 안해서 창고에 저장을 안함. 
date_task = BashOperator(
    task_id='print_date',       # 작업 이름
    bash_command='date',        # 터미널에 'date'라고 치는 것과 같음 (날짜 출력)
    dag=dag,
)

# Task 3를 위한 파이썬 함수 (데이터 받기)
# **context: Airflow가 주는 '마법의 가방' (이전 작업 정보 등이 들어있음)  파이썬의 **문법(Syntax)**과 에어플로우의 **기능(Feature)**이 합쳐진 개념
# 🐚 어디서 그럼 저정보들이 나오는가? : context는 그냥 airflow에서 제공하는 하나의 프레임인거임. 그게이 우리가 끼워 넣는거고. 지금과정은 DAG: 공장장(Scheduler)에게 주는 업무 매뉴얼이고 

def process_data(**context):
    
    # 1. 가방(context)에서 '무전기(ti)'를 꺼낸다.
    ti = context['ti']
    
    # 2. 무전기(ti)로 'say_hello'라는 애가 남긴 메시지(XCom)를 듣는다.
    # task_ids='say_hello': 누구한테 온 메시지인지 지정
    message = ti.xcom_pull(task_ids='say_hello')
    
    print(f"받은 메시지: {message}")
    print("데이터 처리 완료!")

# Task 3: 데이터 받아서 처리하기
process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,  # "함수에 마법의 가방(**context) 넣어줘!" (Airflow 2.0부터는 자동이라 안 써도 되지만 명시적)
    dag=dag,
)

# 4. 순서 연결하기 (Dependency)
# "hello_task가 끝나면 -> date_task를 하고 -> 그게 끝나면 process_task를 해라"
hello_task >> date_task >> process_task