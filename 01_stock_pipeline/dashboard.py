import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# 1. 웹 브라우저 탭에 표시될 제목과 아이콘, 화면 너비를 설정합니다.
st.set_page_config(
    page_title="주식 실시간 대시보드",
    page_icon="📈",
    layout="wide" # 화면을 넓게 사용합니다.
)

st.title("📈 주식 실시간 대시보드")
st.markdown("---") # 가로 구분선을 긋습니다.

# 2. DB 연결을 캐싱합니다. (앱이 새로고침될 때마다 DB에 새로 연결하면 느려지기 때문입니다.)
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )

conn = get_connection()

# 🏗️ 핵심 기술 설명: 캐싱 (Caching)
# Why: 만약 사용자가 100명인데 대시보드를 열 때마다 DB에 접속한다면 DB가 과부하로 뻗어버릴 수 있습니다.
# 해결: @st.cache_data(ttl=60)를 쓰면 60초 동안은 저장된 데이터를 보여주고 1분 뒤에만 새로 가져오므로 시스템이 훨씬 안정적입니다.

# 3. 데이터를 읽어오는 함수입니다. ttl=60은 "60초 동안은 DB에 다시 가지 말고 이 데이터를 재사용해라"는 뜻입니다.
@st.cache_data(ttl=60)
def load_data():
    query = """
    SELECT * FROM stock_prices
    ORDER BY timestamp DESC -- 최신 데이터가 위로 오게 정렬합니다.
    LIMIT 100 -- 너무 많으면 느려지니 최근 100개만 가져옵니다.
    """
    return pd.read_sql(query, conn)

df = load_data()

# 4. 화면을 3개의 칸(컬럼)으로 나눕니다.
col1, col2, col3 = st.columns(3)

with col1:
    # 가장 최신 행(iloc[0])의 가격과 변동률을 멋진 숫자판으로 표시합니다.
    st.metric(
        "현재가",
        f"{df.iloc[0]['price']:,.0f}원", # 천 단위 콤마를 찍습니다.
        f"{df.iloc[0]['change_rate']:.2f}%" # 소수점 둘째 자리까지 표시합니다.
    )

with col2:
    # 가져온 100개 데이터 중 상위 10개의 평균값을 계산해 보여줍니다.
    st.metric(
        "평균가 (최근 10건)",
        f"{df.head(10)['price'].mean():,.0f}원"
    )

with col3:
    # 현재 화면에 로드된 전체 데이터 개수를 보여줍니다.
    st.metric(
        "데이터 수",
        f"{len(df)}개"
    )

# 5. 가격 변화를 한눈에 볼 수 있는 선 그래프를 그립니다.
st.subheader("📊 가격 추이")
# 그래프를 그리려면 시간을 인덱스(기준)로 설정해야 합니다.
chart_data = df[['timestamp', 'price']].set_index('timestamp')
st.line_chart(chart_data)

# 6. 실제 데이터가 어떻게 들어있는지 표 형태로 보여줍니다.
st.subheader("📋 최근 데이터")
st.dataframe(
    df[['timestamp', 'symbol', 'price', 'change_rate', 'volume']].head(20),
    use_container_width=True # 표를 화면 너비에 꽉 채웁니다.
)

# 7. 사용자가 직접 최신 데이터를 불러올 수 있는 버튼입니다.
if st.button("🔄 새로고침"):
    st.cache_data.clear() # 저장해둔 캐시를 지우고 DB에서 새로 가져오게 합니다.
    st.rerun() # 앱을 다시 실행합니다.