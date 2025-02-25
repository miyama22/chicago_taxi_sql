import streamlit as st
from google.cloud import bigquery
import pandas as pd
from keplergl import KeplerGl
import streamlit.components.v1 as components

st.set_page_config(
    layout='wide',
    page_icon='🚖'
)

st.title('Mapping')

H3_QUERY = f"""
    --h3インデックスへの変換関数を定義
    CREATE TEMP FUNCTION h3_latLngToCell(lat FLOAT64, lng FLOAT64, res INT64)
    RETURNS STRING
    LANGUAGE js
    OPTIONS (library=["gs://taxi_h3/h3-js.umd.js"]) AS '''
    return h3.latLngToCell(lat, lng, res);
    ''';
    --変換と集計
    select
        h3_latLngToCell(pickup_latitude, pickup_longitude, 8) as h3_index_res8,
        count(*) as cnt,
        avg(fare) as avg_fare 
    from bigquery-public-data.chicago_taxi_trips.taxi_trips
    where pickup_latitude is not null
        and pickup_longitude is not null
    group by h3_latLngToCell(pickup_latitude, pickup_longitude, 8)
"""

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


if 'df_map' not in st.session_state:
    st.session_state['df_map'] = None


if st.button('H3クエリ実行'):
    #クエリを表示
    st.write('実行するクエリ')
    st.code(H3_QUERY, language='sql')
    
    #クエリ実行
    df = run_query(H3_QUERY)
    if df is None:
        st.write('クエリ実行中にエラーが発生しました')
    else:
        st.session_state['df_map'] = df
        st.write(df)
        
        csv_data=df.to_csv(index=False)
        st.download_button(
            label='結果を保存',
            data=csv_data,
            file_name='taxi_h3.csv',
            mime='text/csv'
        )
        
st.write('エリアマップ表示')
if st.button('地図を表示'):
    if st.session_state['df_map'] is not None:
        df_map = st.session_state['df_map']
        config = {
            'version': 'v1',
            'config': {
                'mapState': {
                    'latitude': 41.8379,
                    'longitude': -87.6828,
                    'zoom': 10,
                }
            }
        }
        m = KeplerGl(data={'data1':df_map}, config=config)
        map_html = m._repr_html_().decode('utf-8')
        html_code = f"""
        {map_html}
        <script>
        // ページ読み込み後、10ms 後にリサイズイベントを発生させる
        window.addEventListener('load', function() {{
            setTimeout(function() {{
            window.dispatchEvent(new Event('resize'));
            }}, 10);
        }});
        </script>
        """
        
        components.html(html_code, height=1000, width=1600)
