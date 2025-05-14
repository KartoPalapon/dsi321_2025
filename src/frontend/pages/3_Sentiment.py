import os
import pandas as pd
from pathlib import Path
from hashlib import sha256
import json
from datetime import datetime
import streamlit as st
import sys

# ---------- PATH CONFIG ----------
# เพิ่ม path ให้ Python มองเห็น dsi321_2025 เป็น module
sys.path.append(r'C:\dsi321_2025')

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
st.title("📊 Sentiment Dashboard (มหาวิทยาลัย)")

if st.button("🔄 Refresh Data"):
    # Load Data from LakeFS
    df = data_from_lakefs()

    # Add helper columns
    df['index'] = df.index + 1
    df['postTime'] = pd.to_datetime(df['postTimeRaw'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['scrapeTime_Date'] = pd.to_datetime(df['scrapeTime'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['hash'] = (df['tweetText'] + df['postTime'].astype(str)).apply(hash_string)

    # Load processed hashes
    if processed_hash_file.exists():
        with open(processed_hash_file, "r") as f:
            processed_hashes = set(f.read().splitlines())
    else:
        processed_hashes = set()

    # Filter only new records
    df_new = df[~df['hash'].isin(processed_hashes)].copy()

    # Load sentiment data if exists
    if sentiment_file.exists():
        sentiment_df = pd.read_csv(sentiment_file)
    else:
        sentiment_df = pd.DataFrame()

    # Classify new data if available
    if not df_new.empty:
        df_dict = df_new.to_dict(orient="records")
        step = 50
        prev_stop = 0
        all_response = []
        topic, subtopic = set(), set()

        for ind in range(step, len(df_dict) + step, step):
            start = prev_stop
            stop = ind
            prev_stop = stop
            rows = df_dict[start:stop]
            st.write(f"Processing rows {start} to {stop}")

            response = classify_messages(rows)
            all_response.append(response)

            for row in response['Tweets']:
                for t in row['topic']:
                    topic.add(t)
                for stp in row['subtopic']:
                    subtopic.add(stp)

        # Flatten results
        tweets = [t for res in all_response for t in res['Tweets']]
        new_sentiment_df = pd.DataFrame(tweets)

        # Join metadata back
        new_sentiment_df = new_sentiment_df.merge(
            df[['index', 'tweetText', 'postTime', 'scrapeTime_Date']],
            how='left', on='index'
        )

        new_sentiment_df.dropna(subset=['topic', 'subtopic'], inplace=True)

        # Combine old and new
        sentiment_df = pd.concat([sentiment_df, new_sentiment_df], ignore_index=True)

        # Save updated sentiment data
        sentiment_df.to_csv(sentiment_file, index=False)

        # Append new hashes
        new_hashes = df_new['hash'].tolist()
        with open(processed_hash_file, "a") as f:
            f.write("\n".join(new_hashes) + "\n")

    else:
        st.success("✅ No new tweets to process.")

    st.success("✅ Data refreshed successfully!")

# Load sentiment data (for display)
if sentiment_file.exists():
    sentiment_df = pd.read_csv(sentiment_file)
    sentiment_df = sentiment_df.dropna(subset=["topic", "subtopic"])
    sentiment_df = sentiment_df.drop(subset=['tweettext'])
    st.subheader("🎯 All Sentiment Data")
    st.dataframe(sentiment_df)
else:
    st.info("ยังไม่มีข้อมูล sentiment ครับ")


