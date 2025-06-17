import pandas as pd
import geopandas as gpd
import requests
import json
from shapely.geometry import shape
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import streamlit as st
import folium
from streamlit_folium import st_folium
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import altair as alt

# ========== 사용자 경로 ==========
CSV_PATH = "regioncode.csv"
GEOJSON_PATH = "koreamap.geojson"

API_KEY = "211b9b11afcd4fa1af0f4743cee9ea18"
STATBL_ID = "A_2024_00045"
ITM_ID = "100001"
DTACYCLE_CD = "MM"

def get_latest_yyyymm():
    return datetime.today().strftime("%Y%m")

@st.cache_data
def load_csv(path):
    df = pd.read_csv(path)
    cls_ids = df["분류코드"].astype(str).tolist()
    cls_id_to_name_map = df.set_index("분류코드")["분류명"].to_dict()
    return cls_ids, cls_id_to_name_map

@st.cache_data
def load_geojson(path):
    with open(path, encoding="utf-8") as f:
        geojson = json.load(f)
    records = []
    for feature in geojson["features"]:
        props = feature["properties"]
        props["geometry"] = shape(feature["geometry"])
        records.append(props)
    return gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")

def fetch_index(cls_id, yyyymm):
    url = "https://www.reb.or.kr/r-one/openapi/SttsApiTblData.do"
    params = {
        "ServiceKey": API_KEY,
        "STATBL_ID": STATBL_ID,
        "ITM_ID": ITM_ID,
        "DTACYCLE_CD": DTACYCLE_CD,
        "CLS_ID": cls_id,
        "WRTTIME_IDTFR_ID": yyyymm,
        "Type": "json"
    }
    try:
        resp = requests.get(url, params=params, timeout=5)
        if resp.status_code != 200:
            return None
        j = resp.json()
        rows = j.get("SttsApiTblData", [None, {}])[1].get("row", [])
        if not rows:
            return None
        row = rows[0]
        return {
            "날짜": pd.to_datetime(yyyymm + "01"),
            "CLS_ID": row["CLS_ID"],
            "매매지수": float(row["DTA_VAL"])
        }
    except:
        return None

def batch_fetch(cls_ids, start_yyyymm, end_yyyymm, cls_id_to_name_map):
    periods = pd.period_range(start=start_yyyymm, end=end_yyyymm, freq="M").strftime("%Y%m")
    tasks = [(cid, y) for y in periods for cid in cls_ids]
    results = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        fetched_data = executor.map(fetch_index, [t[0] for t in tasks], [t[1] for t in tasks])
        for rec in fetched_data:
            if rec:
                results.append(rec)

    df_full = pd.DataFrame(results)
    if not df_full.empty:
        df_full["CLS_NM"] = df_full["CLS_ID"].map(cls_id_to_name_map)
        df_full = df_full.dropna(subset=["CLS_NM"])
    return df_full

@st.cache_data(show_spinner=False)
def cached_fetch(cls_ids, start_yyyymm, end_yyyymm, cls_id_to_name_map):
    return batch_fetch(cls_ids, start_yyyymm, end_yyyymm, cls_id_to_name_map)

def calc_change(df, start_date, end_date):
    df["날짜"] = pd.to_datetime(df["날짜"])
    s = min(df["날짜"], key=lambda x: abs(x - pd.to_datetime(start_date)))
    e = min(df["날짜"], key=lambda x: abs(x - pd.to_datetime(end_date)))
    st.write("📅 실제 시작일:", s.date(), "종료일:", e.date())

    df_s = df[df["날짜"] == s]
    df_e = df[df["날짜"] == e]

    out = []
    for cls_id_str in df_s["CLS_ID"].unique():
        loc_name = df_s[df_s["CLS_ID"] == cls_id_str]["CLS_NM"].iloc[0]
        a = df_s[df_s["CLS_ID"] == cls_id_str]["매매지수"].iloc[0]
        b = df_e[df_e["CLS_ID"] == cls_id_str]["매매지수"].iloc[0]
        out.append({
            "지역코드": cls_id_str,
            "지역명": loc_name,
            "시작지수": round(a,2),
            "종료지수": round(b,2),
            "증감률(%)": round((b - a)/a*100, 2)
        })
    return pd.DataFrame(out)

def merge_geo_data(geo_df, result_df):
    return geo_df.merge(result_df, left_on="SIG_KOR_NM", right_on="지역명", how="left")

def create_colormap(min_val, max_val):
    norm = mcolors.Normalize(vmin=min_val, vmax=max_val)
    cmap = cm.get_cmap("RdYlGn")
    def get_color(val):
        rgba = cmap(norm(val))
        return mcolors.to_hex(rgba)
    return get_color

def plot_colormap_with_geojson(merged_gdf):
    m = folium.Map(location=[36.5, 127.5], zoom_start=7)

    min_val = merged_gdf["증감률(%)"].min()
    max_val = merged_gdf["증감률(%)"].max()
    get_color = create_colormap(min_val, max_val)

    folium.GeoJson(
        merged_gdf,
        name="지역정보",
        style_function=lambda f: {
            "fillColor": get_color(f["properties"]["증감률(%)"])
            if f["properties"]["증감률(%)"] is not None else "#8c8c8c",
            "color": "black",
            "weight": 0.2,
            "fillOpacity": 0.7,
        },
        highlight_function=lambda f: {
            "color": "black",
            "weight": 2,
            "fillOpacity": 0.9,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["SIG_KOR_NM", "증감률(%)"],
            aliases=["지역명", "증감률"],
            localize=True,
            sticky=True
        )
    ).add_to(m)

    return st_folium(m, width=800, height=600)

# ========== Streamlit 실행 ==========
if __name__ == "__main__":
    st.set_page_config(layout="wide")
    st.title("📊 부동산 매매지수 증감률 분석 지도")

    cls_ids, cls_id_to_name_map = load_csv(CSV_PATH)

    with st.sidebar:
        st.header("📅 분석 기간")
        start_date_input = st.date_input("시작일", datetime(2022,1,1))
        end_date_input = st.date_input("종료일", datetime.today())

    latest_yyyymm = get_latest_yyyymm()
    df_full = cached_fetch(cls_ids, start_date_input.strftime("%Y%m"), latest_yyyymm, cls_id_to_name_map)

    if df_full.empty:
        st.error("❌ 데이터 없음")
    else:
        result_df = calc_change(df_full, start_date_input, end_date_input)
        st.subheader("📈 증감률 데이터")
        st.dataframe(result_df.sort_values("증감률(%)", ascending=False))

        geo_df = load_geojson(GEOJSON_PATH)
        merged_gdf = merge_geo_data(geo_df, result_df)

        st.subheader("🗺️ 지역별 증감률 지도")
        map_data = plot_colormap_with_geojson(merged_gdf)

        clicked_name = None
        if map_data and map_data.get("last_active_drawing"):
            props = map_data["last_active_drawing"].get("properties", {})
            clicked_name = props.get("SIG_KOR_NM")

        if clicked_name:
            matched_code = None
            for code, name in cls_id_to_name_map.items():
                if name.endswith(clicked_name.strip()):
                    matched_code = code
                    break

            if matched_code:
                region_data = df_full[df_full["CLS_ID"] == matched_code].sort_values("날짜")
                if not region_data.empty:
                    st.subheader(f"📉 {clicked_name} 지수 추이")
                    chart = alt.Chart(region_data).mark_line(point=True).encode(
                        x="날짜:T",
                        y=alt.Y("매매지수:Q", title="지수", scale=alt.Scale(zero=False)),
                        tooltip=["날짜:T", "매매지수"]
                    ).properties(width=700, height=300)
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.warning("❗ 해당 지역 데이터 없음")
