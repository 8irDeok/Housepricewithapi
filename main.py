import streamlit as st
import pandas as pd
import folium
from folium.features import GeoJson
from datetime import datetime, timedelta
import json
import requests
import numpy as np # numpy import 추가 (만약 NaN 등을 사용한다면 필요)

# Streamlit secrets에서 API 키 가져오기
api_key = st.secrets["API_KEY"]

@st.cache_data
def load_and_process_data(api_key):
    # API 요청을 위한 파라미터 설정
    url = "https://api.odcloud.kr/api/15096561/v1/uddi:731e21b8-7a13-40e8-8b77-d31e98d9e4a3"
    params = {
        "page": 1,
        "perPage": 100000, # 충분히 큰 값으로 설정하여 모든 데이터 가져오기
        "serviceKey": api_key
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # HTTP 오류 발생 시 예외 발생
        data = response.json()
        df = pd.DataFrame(data['data'])

        # 필요한 컬럼만 선택
        df = df[['조사기준일', 'CLS_ID', '매매지수']]

        # '조사기준일' 컬럼을 datetime 형식으로 변환
        df['날짜'] = pd.to_datetime(df['조사기준일'])
        df = df.drop(columns=['조사기준일'])

        # '매매지수' 컬럼을 숫자로 변환 (오류 발생 시 NaN)
        df['매매지수'] = pd.to_numeric(df['매매지수'], errors='coerce')

        # NaN 값 제거 (매매지수가 없는 데이터는 제외)
        df.dropna(subset=['매매지수'], inplace=True)

        return df

    except requests.exceptions.RequestException as e:
        st.error(f"API 요청 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"데이터 처리 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

def calc_change(df_full, start_date, end_date):
    df_e = df_full[(df_full['날짜'] >= start_date) & (df_full['날짜'] <= end_date)].copy()
    df_s = df_full[(df_full['날짜'] >= start_date) & (df_full['날짜'] <= start_date)].copy()

    if df_e.empty or df_s.empty:
        return pd.DataFrame()

    result_data = []
    
    # df_full의 CLS_ID 고유값을 기준으로 반복 (모든 지역을 커버하기 위함)
    for cls_id_str in df_full["CLS_ID"].unique():
        filtered_end_data = df_e[df_e["CLS_ID"] == cls_id_str]["매매지수"]
        if not filtered_end_data.empty:
            b = filtered_end_data.iloc[0]
        else:
            b = 0 # 데이터가 없을 경우 0으로 처리

        filtered_start_data = df_s[df_s["CLS_ID"] == cls_id_str]["매매지수"]
        if not filtered_start_data.empty:
            a = filtered_start_data.iloc[0]
        else:
            a = 0 # 데이터가 없을 경우 0으로 처리

        change_rate = ((b - a) / a) * 100 if a != 0 else 0
        result_data.append({"CLS_ID": cls_id_str, "증감률": change_rate})

    result_df = pd.DataFrame(result_data)
    return result_df

@st.cache_data
def load_geojson():
    try:
        with open('SIG.geojson', 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        return geojson_data
    except FileNotFoundError:
        st.error("SIG.geojson 파일을 찾을 수 없습니다. 파일이 프로젝트 루트 디렉토리에 있는지 확인해주세요.")
        return None
    except json.JSONDecodeError:
        st.error("SIG.geojson 파일이 올바른 JSON 형식이 아닙니다.")
        return None

def create_choropleth_map(df, geojson_data):
    if df.empty or geojson_data is None:
        return None

    m = folium.Map(location=[36.5, 127.5], zoom_start=7, tiles="cartodbpositron")

    # 증감률 데이터 스케일 정규화 (색상 매핑을 위해)
    min_val = df['증감률'].min()
    max_val = df['증감률'].max()

    # 값의 범위가 너무 작거나 같으면 색상 매핑이 어려울 수 있으므로 기본값 설정
    if min_val == max_val:
        color_scale = ['#FFFFFF'] # 모든 값이 같으면 흰색
    else:
        # cmap을 'RdYlGn'으로 설정하여 빨간색(감소), 노란색(변화없음), 초록색(증가)으로 표현
        # vmin과 vmax는 데이터의 실제 최솟값과 최댓값을 사용
        choropleth = folium.Choropleth(
            geo_data=geojson_data,
            data=df,
            columns=['CLS_ID', '증감률'],
            key_on='feature.properties.SIG_KOR_NM', # GeoJSON의 시군구 한글 이름 속성
            fill_color='RdYlGn', # Red-Yellow-Green (감소-변화없음-증가)
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name='Change Rate (증감률)',
            highlight=True,
            name='Choropleth',
            # vmin=min_val,
            # vmax=max_val
            # bounds=[[min_val, max_val]] # vmin, vmax 대신 bounds 사용도 가능
        ).add_to(m)

        # 툴팁 추가 (지역명, 증감률)
        choropleth.geojson.add_child(
            folium.features.GeoJsonTooltip(
                fields=['SIG_KOR_NM'], # GeoJSON의 시군구 이름 필드
                aliases=['지역명'], # 사용자에게 보여질 라벨
                localize=True # 한글 폰트 문제 방지
            )
        )

        # Choropleth 객체에서 색상 스케일을 가져와서 툴팁에 증감률 추가
        choropleth.geojson.add_child(folium.features.GeoJson(
            data=geojson_data,
            style_function=lambda x: {
                'fillColor': choropleth.colormap(df.set_index('CLS_ID').loc[x['properties']['SIG_KOR_NM'], '증감률'])
                if x['properties']['SIG_KOR_NM'] in df['CLS_ID'].values else '#FFFFFF', # 데이터 없으면 흰색
                'color': 'black',
                'weight': 0.5,
                'fillOpacity': 0.7
            },
            tooltip=folium.features.GeoJsonTooltip(
                fields=['SIG_KOR_NM'],
                aliases=['지역명'],
                labels=True,
                sticky=True,
                style="""
                    background-color: #F0EFEF;
                    border: 2px solid black;
                    border-radius: 3px;
                    box-shadow: 3px;
                    font-size: 12px;
                    padding: 5px;
                """,
                # Custom function to add '증감률'
                # 이 부분에서 증감률 정보를 추가해야 함
                # Python 3.10+ 버전에서 f-string 안에 람다 사용이 가능하지만,
                # Streamlit 환경에서 안정성을 위해 함수 밖에서 데이터를 가져와서 전달
                get_html=lambda x: f"<b>지역명:</b> {x['properties']['SIG_KOR_NM']}<br>"
                                   f"<b>증감률:</b> {df[df['CLS_ID'] == x['properties']['SIG_KOR_NM']]['증감률'].iloc[0]:.2f}%"
                                   if x['properties']['SIG_KOR_NM'] in df['CLS_ID'].values and not df[df['CLS_ID'] == x['properties']['SIG_KOR_NM']]['증감률'].empty
                                   else f"<b>지역명:</b> {x['properties']['SIG_KOR_NM']}<br><b>증감률:</b> 데이터 없음"
            )
        ))
    return m

# Streamlit 앱 시작
st.set_page_config(layout="wide", page_title="지역별 주택 매매지수 증감률", page_icon="🏠")

st.title("🏡 지역별 주택 매매지수 증감률 지도")

# 데이터 로드
df_full = load_and_process_data(api_key)
geojson_data = load_geojson()

if df_full.empty:
    st.info("데이터 로드에 실패했습니다. API 키 또는 네트워크 연결을 확인해주세요.")
else:
    min_date = df_full['날짜'].min()
    max_date = df_full['날짜'].max()

    if pd.isna(min_date) or pd.isna(max_date):
        st.error("데이터에 유효한 날짜 범위가 없습니다.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            start_date_input = st.date_input(
                "시작 날짜",
                min_value=min_date.date(),
                max_value=max_date.date(),
                value=min_date.date()
            )
        with col2:
            end_date_input = st.date_input(
                "종료 날짜",
                min_value=min_date.date(),
                max_value=max_date.date(),
                value=max_date.date()
            )

        if start_date_input > end_date_input:
            st.error("시작 날짜는 종료 날짜보다 빠르거나 같아야 합니다.")
        else:
            result_df = calc_change(df_full, pd.to_datetime(start_date_input), pd.to_datetime(end_date_input))

            if not result_df.empty:
                # Folium 지도 생성
                map_object = create_choropleth_map(result_df, geojson_data)

                if map_object:
                    st.components.v1.html(folium.Figure().add_child(map_object).render(), height=700)
                else:
                    st.info("지도를 생성할 수 없습니다. 데이터 또는 GeoJSON 파일을 확인해주세요.")
            else:
                st.info("선택된 기간에 대한 증감률 데이터를 계산할 수 없습니다. 날짜 범위를 다시 확인해주세요.")
