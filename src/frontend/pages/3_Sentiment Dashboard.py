import os
import pandas as pd
from pathlib import Path
from hashlib import sha256
from datetime import datetime
import streamlit as st
import sys
import ast
import plotly.express as px

# ---------- PATH CONFIG ----------
sys.path.append(r'C:\dsi321_2025')  # เปลี่ยนให้ตรงกับเครื่องคุณ
from config.path_config import lakefs_s3_path, tags
from config.message_classifier import classify_messages

# ---------- CONFIG ----------
lakefs_endpoint = "http://localhost:8001/"
today = pd.Timestamp.now().strftime("%Y-%m-%d")

# ---------- PATH SETUP ----------
file_dir = Path(os.getcwd())
root_dir = file_dir.parent
data_dir = root_dir / "data"
processed_hash_dir = data_dir / "processed_hashes" / "all_tags"
processed_hash_dir.mkdir(parents=True, exist_ok=True)
processed_hash_file = processed_hash_dir / f"{today}.txt"
sentiment_file = data_dir / f"{today}.csv"

# ---------- FUNCTION ----------
def hash_string(s):
    return sha256(s.encode()).hexdigest()

def data_from_lakefs(lakefs_endpoint: str = "http://localhost:8001/"):
    storage_options = {
        "key": os.getenv("ACCESS_KEY"),
        "secret": os.getenv("SECRET_KEY"),
        "client_kwargs": {"endpoint_url": lakefs_endpoint}
    }
    df = pd.read_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        engine='pyarrow'
    )
    df.drop_duplicates(subset='tweetText', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# ---------- MAIN STREAMLIT APP ----------
st.title("📊 Sentiment Dashboard")
st.divider()

# Sidebar
with st.sidebar:
    st.header("🛠️ Control Panel")
    if st.button("🔄 Refresh Data"):
        df = data_from_lakefs()
        df['index'] = df.index + 1
        df['postTime'] = pd.to_datetime(df['postTimeRaw'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['scrapeTime_Date'] = pd.to_datetime(df['scrapeTime'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['hash'] = (df['tweetText'] + df['postTime'].astype(str)).apply(hash_string)

        if processed_hash_file.exists():
            with open(processed_hash_file, "r") as f:
                processed_hashes = set(f.read().splitlines())
        else:
            processed_hashes = set()

        df_new = df[~df['hash'].isin(processed_hashes)].copy()

        if sentiment_file.exists():
            sentiment_df = pd.read_csv(sentiment_file)
        else:
            sentiment_df = pd.DataFrame()

        if not df_new.empty:
            df_dict = df_new.to_dict(orient="records")
            step = 50
            prev_stop = 0
            all_response = []

            for ind in range(step, len(df_dict) + step, step):
                start = prev_stop
                stop = ind
                prev_stop = stop
                rows = df_dict[start:stop]
                st.write(f"Processing rows {start} to {stop}")
                response = classify_messages(rows)
                all_response.append(response)

            tweets = [t for res in all_response for t in res['Tweets']]
            new_sentiment_df = pd.DataFrame(tweets)

            new_sentiment_df = new_sentiment_df.merge(
                df[['index', 'tweetText', 'postTime', 'scrapeTime_Date']],
                how='left', on='index'
            )

            new_sentiment_df.dropna(subset=['topic', 'subtopic'], inplace=True)
            sentiment_df = pd.concat([sentiment_df, new_sentiment_df], ignore_index=True)
            sentiment_df.to_csv(sentiment_file, index=False)

            new_hashes = df_new['hash'].tolist()
            with open(processed_hash_file, "a") as f:
                f.write("\n".join(new_hashes) + "\n")

            st.success("✅ Data refreshed successfully!")
        else:
            st.success("✅ No new tweets to process.")

    st.markdown("---")

    # Load data for filter
    if sentiment_file.exists():
        sentiment_df = pd.read_csv(sentiment_file)

        for col in ['topic', 'subtopic', 'sentiment']:
            sentiment_df[col] = sentiment_df[col].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])

        all_topics = sorted({item for sublist in sentiment_df['topic'] for item in sublist})
        all_subtopics = sorted({item for sublist in sentiment_df['subtopic'] for item in sublist})
        all_sentiments = sorted({item for sublist in sentiment_df['sentiment'] for item in sublist})

        st.markdown("### 🎯 Filter Options")

        # Topic Filter
        select_all_topics = st.checkbox("Select All Topics", value=True)
        selected_topics = all_topics if select_all_topics else st.multiselect("Topic", all_topics)

        # Subtopic Filter
        select_all_subtopics = st.checkbox("Select All Subtopics", value=True)
        selected_subtopics = all_subtopics if select_all_subtopics else st.multiselect("Subtopic", all_subtopics)

        # Sentiment Filter
        select_all_sentiments = st.checkbox("Select All Sentiments", value=True)
        selected_sentiments = all_sentiments if select_all_sentiments else st.multiselect("Sentiment", all_sentiments)

# ---------- Main Display ----------
if sentiment_file.exists():
    sentiment_df = pd.read_csv(sentiment_file)
    sentiment_df = sentiment_df.drop(columns=['index', 'tweetText'], errors='ignore')
    cols_to_check = ['topic', 'subtopic', 'sentiment']

    sentiment_df = sentiment_df.dropna(subset=cols_to_check)
    sentiment_df = sentiment_df[~sentiment_df[cols_to_check].isin(['', '[]', 'nan', 'None']).any(axis=1)].reset_index(drop=True)

    for col in cols_to_check:
        sentiment_df[col] = sentiment_df[col].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])

    # Apply filters
    df_filtered = sentiment_df.copy()
    if selected_topics:
        df_filtered = df_filtered[df_filtered['topic'].apply(lambda x: any(t in x for t in selected_topics))]
    if selected_subtopics:
        df_filtered = df_filtered[df_filtered['subtopic'].apply(lambda x: any(s in x for s in selected_subtopics))]
    if selected_sentiments:
        df_filtered = df_filtered[df_filtered['sentiment'].apply(lambda x: any(s in x for s in selected_sentiments))]

    # Display
    col1, col2 = st.columns([1, 2])

    with col1:
        if not df_filtered.empty:
            sentiment_counts = {}
            for s_list in df_filtered['sentiment']:
                for s in s_list:
                    sentiment_counts[s] = sentiment_counts.get(s, 0) + 1
            pie_df = pd.DataFrame({'Sentiment': sentiment_counts.keys(), 'Count': sentiment_counts.values()})
            fig = px.pie(pie_df, names='Sentiment', values='Count', title='Sentiment Distribution')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No data to display chart.")

    with col2:
        st.subheader("📑 Tweet Sentiment Data")
        st.dataframe(df_filtered)

else:
    st.info("ยังไม่มีข้อมูล sentiment")
