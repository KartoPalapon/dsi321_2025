import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go
import altair as alt
from config.path_config import lakefs_s3_path

st.set_page_config(page_title="Dashboard 📊", layout="wide")
st.title("📊 Dashboard")

# โหลดข้อมูลจาก lakeFS
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
    return df

# ปุ่ม Refresh
if st.button("🔄 Refresh Data"):
    df = data_from_lakefs()
    st.success("โหลดข้อมูลใหม่เรียบร้อย ✅")
else:
    df = data_from_lakefs()

# แปลงเวลาสำหรับ filter
df['scrapeTime'] = pd.to_datetime(df['scrapeTime'], errors='coerce')
df = df.dropna(subset=['scrapeTime', 'tag', 'category'])

# --- Sidebar filters (ใช้ร่วมกันทั้งสองกราฟ) ---
with st.sidebar:
    st.subheader("🔎 Filter Options")
    available_tags = sorted(df['tag'].dropna().unique())
    selected_tags = st.multiselect("Filter by Tag", available_tags, default=available_tags)

    available_categories = sorted(df['category'].dropna().unique())
    selected_category = st.selectbox("Select Category to Highlight", available_categories)

# --- Filtered Data สำหรับใช้ทั้งสองกราฟ ---
filtered_df = df[df['tag'].isin(selected_tags)]
filtered_df = filtered_df[filtered_df['category'] == selected_category]

# --- กราฟ 1: จำนวน tag ต่อ category (Plotly) ---
tag_counts = filtered_df.groupby(['category', 'tag']).size().reset_index(name='count')

if not tag_counts.empty:
    fig = go.Figure()
    categories = tag_counts['category'].unique()

    for cat in categories:
        data = tag_counts[tag_counts['category'] == cat]
        fig.add_trace(go.Bar(
            x=data['tag'],
            y=data['count'],
            name=cat,
            text=data['count'],
            textposition='auto'
        ))

    fig.update_layout(
        title='📌 จำนวนของ Hashtag (#) แยกตาม Category',
        xaxis_title='Hashtag',
        yaxis_title='จำนวนโพสต์',
        barmode='group',
        bargap=0.2,
        width=900,
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("ไม่พบข้อมูลสำหรับการกรองนี้ 🚨")

# --- กราฟ 2: Volume ตาม scrapeTime (Altair) ---
agg_df = (
    filtered_df.groupby(['scrapeTime'])
    .size()
    .reset_index(name='tweet_count')
)

if not agg_df.empty:
    bar_chart = alt.Chart(agg_df).mark_bar().encode(
        x=alt.X('scrapeTime:T', title='Scrape Timestamp'),
        y=alt.Y('tweet_count:Q', title='Number of Tweets'),
        tooltip=['scrapeTime:T', 'tweet_count:Q']
    ).properties(
        title=f"⏱️ Tweet Volume for Category: {selected_category}",
        width=900,
        height=450
    ).interactive()

    st.altair_chart(bar_chart, use_container_width=True)
else:
    st.warning("ไม่พบข้อมูล scrape สำหรับช่วงที่เลือก 👀")
