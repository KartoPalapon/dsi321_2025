import streamlit as st 
import os
import pandas as pd 

# Import path configuration
from config.path_config import lakefs_s3_path


st.title('DataFrame For Tweet Real-time Data📋')

st.divider() 

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
st.dataframe(df)

# --- Sidebar filters (ใช้ร่วมกันทั้งสองกราฟ) ---
with st.sidebar:
        # ปุ่ม Refresh
    if st.button("🔄 Refresh Data"):
        df = data_from_lakefs()
        st.success("โหลดข้อมูลใหม่เรียบร้อย ✅")
    else:
        df = data_from_lakefs()

with st.expander("📋 Metadata of Tweet DataFrame"):
    st.markdown("""
| Column Name   | Description |
|---------------|-------------|
| `category`    | หมวดหมู่ของ Tweet ที่จัดตามหัวข้อ เช่น การเมือง บันเทิง กีฬา เป็นต้น |
| `tag`         | คำแท็กหรือคีย์เวิร์ดที่อธิบายว่า Tweet นั้นเกี่ยวกับเรื่องอะไร |
| `username`    | ชื่อผู้ใช้ Twitter ที่โพสต์ Tweet |
| `tweetText`   | ข้อความที่ถูกโพสต์ใน Tweet |
| `postTimeRaw` | เวลาแบบดิบที่ Tweet ถูกโพสต์ (อาจเป็น timestamp หรือข้อความที่แสดงเวลาจริง) |
| `scrapeTime`  | เวลาที่ข้อมูลนี้ถูกเก็บหรือดึงจาก Twitter |
| `year`        | ปีของเวลาที่ Tweet ถูกโพสต์ |
| `month`       | เดือนของเวลาที่ Tweet ถูกโพสต์ |
| `day`         | วันที่ของเวลาที่ Tweet ถูกโพสต์ |
    """)
