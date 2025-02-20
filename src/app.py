import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(
    page_title='TOPページ',
    layout='wide',
    page_icon='🚖'
)

st.title('シカゴ タクシービッグデータ分析')

# クエリの定義
QUERY="""
    select
        count(*) as cnt,
        min(trip_start_timestamp) as min_time,
        max(trip_start_timestamp) as max_time
    from bigquery-public-data.chicago_taxi_trips.taxi_trips
"""

def run_query():
    # クライアントの初期化
    client = bigquery.Client()
    
    try:
        # クエリの実行
        query_job = client.query(QUERY)
        return query_job.result().to_dataframe()
    except Exception as e:
        print(f'クエリ実行中にエラーが発生しました:{e}')
        return None

if st.button('データの概要を確認'):
    st.code(QUERY, language='sql')
    df = run_query()
    if df is None:
        st.write('クエリ実行中にエラーが発生しました')
    else:
        st.write(df)