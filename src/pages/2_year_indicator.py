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

st.title('Year Indicator')

def run_query(query_str:str)->pd.DataFrame:
    # クライアントの初期化
    client = bigquery.Client()

    try:
        # クエリの実行
        with st.spinner("クエリ実行中"):
            job_config=bigquery.QueryJobConfig(
                destination="taxi-eda.taxi_eda.year_indicator",
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            query_job = client.query(query_str, job_config=job_config)
            query_job.result()
            
            # テーブルからデータを読み込む
            table_ref = bigquery.TableReference.from_string("taxi-eda.taxi_eda.year_indicator")
            table = client.get_table(table_ref)
        return client.list_rows(table).to_dataframe()
    except Exception as e:
        print(f'クエリ実行中にエラーが発生しました:{e}')
        return None


#-------------------------------
# 1.チップ発生率
#-------------------------------

TIP_QUERY="""
    with
    --2019年から2013年までのリストを取得
    year_list as (
        select format_date('%Y', year) as year
        from unnest(
        generate_date_array('2019-01-01', '2023-12-31', interval 1 year)
        ) as year
    ),
    --会社のリストを取得
    company_list as (
        select distinct company
        from bigquery-public-data.chicago_taxi_trips.taxi_trips
        where company is not null
    ),
    --クロス結合
    frame as (
        select
        company_list.company,
        year_list.year
        from company_list
        cross join year_list
    ),
    --集計
    year_indicator as (
        select
        company,
        year,
        countif(tips > 0) * 100 / countif(fare > 0) as tips_ratio,

        from (
        select
            company,
            format_timestamp('%Y', trip_start_timestamp) as year,
            fare,
            tips
        from bigquery-public-data.chicago_taxi_trips.taxi_trips
        where trip_start_timestamp between '2019-01-01' and '2023-12-31'
        )
        where company is not null
        group by company, year
    )
    --結合
    select
        company,
        year,
        coalesce(tips_ratio, 0) as tips_ratio
    from frame
    left join year_indicator using(company, year)
    order by company, year
"""

if 'df_year' not in st.session_state:
    st.session_state['df_year'] = None

# 上位3社のデータを抽出しグラフを作成する関数
def make_top3_barplot():
    #データの読み込み
    df = st.session_state['df_year']
    
    top3_companies = ['Taxi Affiliation Services', 'Flash Cab', 'Sun Taxi']
    df_filterd = df[df['company'].isin(top3_companies)]
    
    #グラフを作成
    fig = px.bar(
        df_filterd,
        x='year',
        y='tips_ratio',
        color='company',
        title='tips_ratio by Top3 Companies',
        height=800
    )
    fig.update_layout(font_size=20, hoverlabel_font_size=20, legend=dict(font=dict(size=20)))
    return fig


st.write('1.会社別チップ発生率')
if st.button('チップ発生率クエリ実行'):
    #クエリを表示
    st.write('実行するクエリ')
    st.code(TIP_QUERY, language='sql')
    
    #クエリ実行
    df = run_query(TIP_QUERY)
    if df is None:
        st.write('クエリ実行中にエラーが発生しました')
    else:
        df['year'] = pd.to_datetime(df['year'], format='%Y')
        st.session_state['df_year'] = df
        st.write(df)
        
        csv_data=df.to_csv(index=False)
        st.download_button(
            label='結果を保存',
            data=csv_data,
            file_name='year_indicator.csv',
            mime='text/csv'
        )

# KPIごとにグラフを描画
if st.button('チップ発生率グラフを表示'):
    if st.session_state['df_year'] is None:
        st.write('クエリを実行してください')
    else:
        fig_tip_ratio = make_top3_barplot()
        fig_tip_ratio.update_layout(barmode='group')
        st.plotly_chart(fig_tip_ratio, theme='streamlit')


#-------------------------------
#2.支払いタイプの推移
#-------------------------------

TYPE_QUERY = """
    with
    --2019年から2023年までの年のリストを取得
    year_list as (
        select format_date('%Y', year) as year
        from unnest(
        generate_date_array('2019-01-01', '2023-12-31', interval 1 year)
        ) as year
    ),
    --支払い種別のリストを取得
    payment_type_list as (
        select distinct payment_type
        from bigquery-public-data.chicago_taxi_trips.taxi_trips
        where payment_type is not null
    ),
    --クロス結合
    frame as (
        select
        payment_type_list.payment_type,
        year_list.year
        from payment_type_list
        cross join year_list
    ),
    -- 集計
    year_indicator as (
        select
        payment_type,
        year,
        count(payment_type) as payment_count
        from (
        select
            payment_type,
            format_timestamp('%Y', trip_start_timestamp) as year
        from bigquery-public-data.chicago_taxi_trips.taxi_trips
        where trip_start_timestamp between '2019-01-01' and '2024-01-01'
        )
        where payment_type is not null
        group by payment_type, year
    )
    --結合
    select
        year,
        payment_type,
        payment_count
    from frame
    left join year_indicator using(payment_type, year)
    order by year, payment_type
"""

if 'df_type' not in st.session_state:
    st.session_state['df_type'] = None

def area_plot():
    df = st.session_state['df_type']
    fig = px.area(
        data_frame=df,
        x='year',
        y='payment_count',
        color='payment_type',
        height=800,
        title='payment_type_transition'
    )
    fig.update_layout(font_size=20, hoverlabel_font_size=20, legend=dict(font=dict(size=20)))
    return fig

st.write('2.支払い方法推移')
if st.button('支払いタイプクエリ実行'):
    #クエリを表示
    st.write('実行するクエリ')
    st.code(TYPE_QUERY, language='sql')
    
    #クエリ実行
    df = run_query(TYPE_QUERY)
    if df is None:
        st.write('クエリ実行中にエラーが発生しました')
    else:
        df['year'] = pd.to_datetime(df['year'], format='%Y')
        st.session_state['df_type'] = df
        st.write(df)
        
        csv_data=df.to_csv(index=False)
        st.download_button(
            label='結果を保存',
            data=csv_data,
            file_name='year_indicator2.csv',
            mime='text/csv'
        )

if st.button('支払い方法のグラフを表示'):
    if st.session_state['df_type'] is None:
        st.write('クエリを実行してください')
    else:
        fig_payment_type = area_plot()
        st.plotly_chart(fig_payment_type, theme='streamlit')