import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go
import altair as alt
from config.path_config import lakefs_s3_path

# ---------- CONFIG ----------
st.set_page_config(page_title="Dashboard 📊", layout="wide")
st.title("📊 Twitter(X) Dashboard")
st.divider()

# ---------- LOAD DATA ----------
@st.cache_data(show_spinner="📥 กำลังโหลดข้อมูลจาก LakeFS ...")
def data_from_lakefs(lakefs_endpoint: str = "http://localhost:8001/"):
    storage_options = {
        "key": os.getenv("ACCESS_KEY"),
        "secret": os.getenv("SECRET_KEY"),
        "client_kwargs": {
            "endpoint_url": lakefs_endpoint
        }
    }
    df = pd.read_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        engine='pyarrow',
    )
    df.drop_duplicates(subset='tweetText', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

df = data_from_lakefs()

# ---------- CLEAN & TRANSFORM ----------
df['scrapeTime'] = pd.to_datetime(df['scrapeTime'], errors='coerce')
df['postTimeRaw'] = pd.to_datetime(df['postTimeRaw'], errors='coerce')
df = df.dropna(subset=['scrapeTime', 'postTimeRaw', 'tag', 'category'])
df['post_date'] = df['postTimeRaw'].dt.date  # เพิ่มคอลัมน์วันที่อย่างเดียว (ไม่รวมเวลา)

# ---------- SIDEBAR FILTERS ----------
with st.sidebar:
    st.subheader("🔎 Filter Options")

    # Category Filter
    available_categories = sorted(df['category'].dropna().unique())
    category_options = ["All"] + available_categories
    selected_category = st.selectbox("Select Category", category_options)

    # Tag Filter
    available_tags = sorted(df['tag'].dropna().unique())
    select_all_tags = st.checkbox("Select All Tags", value=True)
    selected_tags = available_tags if select_all_tags else st.multiselect("Filter by Tag", available_tags)

    # Date Filter
    min_date = df['post_date'].min()
    max_date = df['post_date'].max()
    date_range = st.date_input("📅 เลือกช่วงวันที่ (จาก postTimeRaw)", [min_date, max_date], min_value=min_date, max_value=max_date)

    # View mode: รวม หรือ แยก category
    view_mode = st.radio("🧩 เลือกรูปแบบการดูกราฟ", ["รวมทั้งหมด", "แยกตาม Category"])

    # Refresh Button
    if st.button("🔄 Refresh Data"):
        df = data_from_lakefs()
        st.success("โหลดข้อมูลใหม่เรียบร้อย ✅")

# ---------- APPLY FILTER ----------
filtered_df = df[
    (df['tag'].isin(selected_tags)) &
    (df['post_date'] >= date_range[0]) &
    (df['post_date'] <= date_range[1])
]
if selected_category != "All":
    filtered_df = filtered_df[filtered_df['category'] == selected_category]

# ---------- GRAPH 1: BAR CHART ----------
st.subheader("📌 จำนวนของ Hashtag (#) แยกตาม Category")

tag_counts = filtered_df.groupby(['category', 'tag']).size().reset_index(name='count')

if not tag_counts.empty:
    fig = go.Figure()
    for cat in tag_counts['category'].unique():
        data = tag_counts[tag_counts['category'] == cat]
        fig.add_trace(go.Bar(
            x=data['tag'],
            y=data['count'],
            name=cat,
            text=data['count'],
            textposition='auto'
        ))

    fig.update_layout(
        xaxis_title='Hashtag',
        yaxis_title='จำนวนโพสต์ (Tweet)',
        barmode='group',
        bargap=0.2,
        width=1000,
        height=450
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("ไม่พบข้อมูลสำหรับเงื่อนไขที่เลือก 🚨")

# ---------- GRAPH 2: LINE CHART ----------
st.subheader("📈 แนวโน้มจำนวนโพสต์จาก Twitter(X) รายวัน")

if view_mode == "รวมทั้งหมด":
    line_data = (
        filtered_df.groupby('post_date')
        .size()
        .reset_index(name='tweet_count')
    )

    chart = alt.Chart(line_data).mark_line(point=True).encode(
        x=alt.X('post_date:T', title='วันที่', axis=alt.Axis(format='%d %b')),
        y=alt.Y('tweet_count:Q', title='จำนวนโพสต์'),
        tooltip=['post_date:T', 'tweet_count:Q']
    ).properties(
        width=1000,
        height=450,
        title="📊 จำนวนโพสต์รวมรายวัน"
    ).interactive()

else:
    line_data = (
        filtered_df.groupby(['post_date', 'category'])
        .size()
        .reset_index(name='tweet_count')
    )

    chart = alt.Chart(line_data).mark_line(point=True).encode(
        x=alt.X('post_date:T', title='วันที่', axis=alt.Axis(format='%d %b')),
        y=alt.Y('tweet_count:Q', title='จำนวนโพสต์'),
        color=alt.Color('category:N', title='Category'),
        tooltip=['post_date:T', 'tweet_count:Q', 'category:N']
    ).properties(
        width=1000,
        height=450,
        title="📊 จำนวนโพสต์แยกตาม Category รายวัน"
    ).interactive()

if not line_data.empty:
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("ไม่พบข้อมูลในช่วงเวลานี้ 👀")
