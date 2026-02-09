from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ==========================================
# 1. 작전 규칙 정하기 (기본 설정)
# ==========================================
default_args = {
    'owner': 'de-student',      # 이 작전의 대장님 이름
    'start_date': datetime(2024, 2, 1), # 언제부터 시작했니?
    'retries': 2,               # ★ 중요: 실패하면 2번 더 기회를 줘라! (총 3번 시도)
    'retry_delay': timedelta(seconds=10), # 다시 시도할 때 10초만 쉬고 해라.
}

# ==========================================
# 2. 작전명 만들기 (DAG 생성)
# ==========================================
dag = DAG(
    'failure_test_dag',         # 작전 이름 (웹사이트에 이렇게 뜸)
    default_args=default_args,  # 위에서 정한 규칙 따르기
    schedule_interval=None,     # "지금 당장" 버튼 누를 때만 실행 (자동 실행 X)
    catchup=False,              # 밀린 숙제(과거 작업)는 하지 마라.
)

# ==========================================
# 3. 로봇들(Tasks) 만들기
# ==========================================

# [1번 로봇] 무조건 성공하는 모범생
def success_func():
    print("✅ 1번 로봇: 야호! 나는 문제없이 성공했어!")
    return "OK"

t1_success = PythonOperator(
    task_id='success_task',     # 이름표
    python_callable=success_func, # 시킬 일
    dag=dag,
)

# [2번 로봇] 무조건 실패하는 사고뭉치
def fail_func():
    print("💥 2번 로봇: 으악! 나는 무조건 넘어질 거야!")
    raise ValueError("일부러 에러 내기! 콰광!") # 컴퓨터한테 "에러 났다!"고 소리치는 명령어

t2_fail = PythonOperator(
    task_id='fail_task',
    python_callable=fail_func,
    dag=dag,
)

# [3번 로봇] 3번 만에 성공하는 끈기남
def retry_func(**context):
    # Airflow한테 "나 지금 몇 번째 시도야?" 라고 물어보기
    ti = context['task_instance']
    try_num = ti.try_number 
    
    print(f"현재 {try_num}번째 도전 중...")
    
    if try_num < 3:
        print("❌ 아직 3번째가 아니네.. 일부러 실패한다!")
        raise ValueError("아직 멀었어!") # 1번, 2번 시도 때는 여기서 에러 내고 죽음
    else:
        print("✅ 드디어 3번째다! 이번엔 성공!") # 3번 시도 때 비로소 성공
        return "Success"

t3_retry = PythonOperator(
    task_id='retry_task',
    python_callable=retry_func,
    provide_context=True, # "몇 번째 시도인지" 정보를 받으려면 이게 필요함
    dag=dag,
)

# [4번 로봇] 뒤처리는 내가 한다 (청소부)
def cleanup_func():
    print("🧹 4번 로봇: 앞사람이 망해도 나는 청소를 한다.")
    return "Cleanup Done"

t4_cleanup = PythonOperator(
    task_id='cleanup_task',
    python_callable=cleanup_func,
    trigger_rule='all_done',  # ★ ⭐️ 핵심: "앞 팀이 성공하든 실패하든 상관없이 나는 무조건 출동한다!"
    dag=dag,
)

# ==========================================
# 4. 순서 정하기 (이어달리기 배치)
# ==========================================

# 1번이 성공하면 -> [2번]과 [3번]이 동시에 출발하고 -> 둘 다 끝나면 [4번]이 출동
t1_success >> [t2_fail, t3_retry] >> t4_cleanup