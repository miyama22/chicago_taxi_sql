import streamlit as st
from google.cloud import bigquery
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt

st.set_page_config(
    page_title='TOPページ',
    layout='wide',
)

st.title('シカゴ タクシービッグデータ分析')

def run_query():
    # クライアントの初期化
    client = bigquery.Client()
    
    # クエリの定義
    query="""
        select
            min(trip_start_timestamp) as min_time,
            max(trip_start_timestamp) as max_time
        from bigquery-public-data.chicago_taxi_trips.taxi_trips
    """
    try:
        # クエリの実行
        query_job = client.query(query)
        return query_job.result().to_dataframe()
    except Exception as e:
        print(f'クエリ実行中にエラーが発声しました:{e}')
        return None

if st.button('クエリ実行'):
    df = run_query()
    st.write(df)