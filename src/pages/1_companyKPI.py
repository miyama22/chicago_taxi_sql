import streamlit as st
from google.cloud import bigquery
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import plotly.express as px

st.set_page_config(
    layout='wide',
    page_icon='🚖'
)

st.title('Monthly KPI')

QUERY="""
    with
    --月のリストを作成
    month_list as (
        select format_date('%Y-%m', tmp_month)as month
        from unnest(
        generate_date_array('2013-01-01', '2023-12-31', interval 1 month)
        ) as tmp_month
    ),
    
    --companyリストを作成
    company_list as (
        select distinct company
        from bigquery-public-data.chicago_taxi_trips.taxi_trips
        where company is not null
    ),

    --クロス結合してcompanyごとに全ての月の行を生成
    frame as (
        select 
        company_list.company,
        month_list.month
        from company_list
        cross join month_list
    ),

    --集計
    monthly_kpi as (
        select
        company,
        month,
        sum(trip_total) as monthly_sales,
        count(trip_total) as trip_count,
        avg(trip_total) as avg_sales_per_customer,
        avg(trip_miles) as avg_miles,
        avg(trip_seconds) as avg_seconds

        from(
        select
            company,
            format_timestamp('%Y-%m', trip_start_timestamp, 'America/Chicago') as month,
            trip_total,
            trip_miles,
            trip_seconds
        from bigquery-public-data.chicago_taxi_trips.taxi_trips)
        where company is not null
        group by company, month
    )

    --月の行と集計値を結合
    select
    company,
    month,
    --結合されなかった行を0埋め
    coalesce(monthly_sales, 0) as monthly_sales,
    coalesce(trip_count, 0) as trip_count,
    coalesce(avg_sales_per_customer, 0) as avg_sales_per_customer,
    coalesce(avg_miles, 0) as avg_miles,
    coalesce(avg_seconds, 0) as avg_seconds
    from frame
    left join monthly_kpi using(company, month)
    order by company, month
"""


if 'df' not in st.session_state:
    st.session_state['df'] = None

def run_query(query_str:str)->pd.DataFrame:
    # クライアントの初期化
    client = bigquery.Client()

    try:
        # クエリの実行
        with st.spinner("クエリ実行中"):
            job_config=bigquery.QueryJobConfig(
                destination="taxi-eda.taxi_eda.monthly_kpi",
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            query_job = client.query(query_str, job_config=job_config)
            query_job.result()
            
            # テーブルからデータを読み込む
            table_ref = bigquery.TableReference.from_string("taxi-eda.taxi_eda.monthly_kpi")
            table = client.get_table(table_ref)
        return client.list_rows(table).to_dataframe()
    except Exception as e:
        print(f'クエリ実行中にエラーが発声しました:{e}')
        return None

if st.button('クエリ実行'):
    #クエリを表示
    st.write('実行するクエリ')
    st.code(QUERY, language='sql')
    
    #クエリ実行
    df = run_query(QUERY)
    if df is None:
        st.write('クエリ実行中にエラーが発声しました')
    else:
        df['month'] = pd.to_datetime(df['month'], format='%Y-%m')
        st.session_state['df'] = df
        st.write(df)
        
        csv_data=df.to_csv(index=False)
        st.download_button(
            label='結果を保存',
            data=csv_data,
            file_name='monthly_kpi.csv',
            mime='text/csv'
        )

# 上位3社のデータを抽出しグラフを作成する関数
def make_top3_lineplot(kpi:str, top_kpi:str):
    #データの読み込み
    df = st.session_state['df']
    
    # 2021年以降のその指標でのTOP3社のデータを抽出
    df_2021 = df[df['month'] >= pd.to_datetime('2021-01-01')]
    df_top3 = (
        df_2021.groupby('company', as_index=False)[top_kpi]
        .sum()
        .nlargest(3, top_kpi)
    )
    top3_companies = df_top3['company'].unique()
    
    df_filterd = df[df['company'].isin(top3_companies)]
    
    #グラフを作成
    fig = px.line(
        df_filterd,
        x='month',
        y=kpi,
        color='company',
        title=f'{kpi} by Top3 Companies',
        height=800
    )
    fig.update_layout(font_size=20, hoverlabel_font_size=20, legend=dict(font=dict(size=20)))
    return fig

# KPIごとにグラフを描画
st.write('1.会社別乗車回数(2021年以降の上位3社)')
if st.button('乗車回数グラフを表示'):
    if st.session_state['df'] is None:
        st.write('クエリを実行してください')
    else:
        fig_trip_count = make_top3_lineplot('trip_count', 'trip_count')
        st.plotly_chart(fig_trip_count, theme='streamlit')
        
st.write('2.会社別売り上げ')
if st.button('売り上げグラフを表示'):
    if st.session_state['df'] is None:
        st.write('クエリを実行してください')
    else:
        fig_monthly_sales = make_top3_lineplot('monthly_sales', 'monthly_sales')
        st.plotly_chart(fig_monthly_sales, theme='streamlit')

st.write('3.顧客単価')
if st.button('顧客単価グラフを表示'):
    if st.session_state['df'] is None:
        st.write('クエリを実行してください')
    else:
        fig_avg_sales_per_customer = make_top3_lineplot('avg_sales_per_customer', 'monthly_sales')
        st.plotly_chart(fig_avg_sales_per_customer, theme='streamlit')

st.write('4.平均乗車距離')
if st.button('平均乗車距離グラフを表示'):
    if st.session_state['df'] is None:
        st.write('クエリを実行してください')
    else:
        fig_avg_miles = make_top3_lineplot('avg_miles', 'monthly_sales')
        st.plotly_chart(fig_avg_miles, theme='streamlit')

st.write('5.平均乗車時間')
if st.button('平均乗車時間グラフを表示'):
    if st.session_state['df'] is None:
        st.write('クエリを実行してください')
    else:
        fig_avg_seconds = make_top3_lineplot('avg_seconds', 'monthly_sales')
        st.plotly_chart(fig_avg_seconds, theme='streamlit')

