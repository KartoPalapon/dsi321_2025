import streamlit as st
import subprocess
import os

st.set_page_config(page_title="Tweet Analysis App", layout="wide")
st.title("Tweet Analyzer For DSI321 Project📊")

tab1, tab2 = st.tabs(["Overview", "Readme.md"])

with tab1:
    st.header("About Our Project")

    st.markdown("""
    ### 📘 รายละเอียดโครงการ
    
    โครงการนี้เป็นส่วนหนึ่งของรายวิชา **DSI321: Data Science Infrastructure** มีวัตถุประสงค์เพื่อติดตามและวิเคราะห์ **ความคิดเห็นของผู้ใช้ใน Social Media**  
    โดยเฉพาะในประเด็นที่เกี่ยวข้องกับ **Application X** ผ่านกระบวนการ Social Listening
    
    ### 🧩 โครงสร้างระบบประกอบด้วย 3 ส่วนหลักในหน้าเว็บ:

    #### 1. Overview & Metadata
    - แสดงข้อมูลตัวอย่างที่ดึงมาจาก Social Media
    - รายละเอียด metadata ของ tweets เช่น `tweet_id`, `scrape_time`, `source`, ฯลฯ
    - ใช้ **LakeFS** เป็นระบบจัดการเวอร์ชันของข้อมูล tweet (Data Lake)

    #### 2. Dashboard Monitoring
    - แสดงสถานะของกระบวนการดึงข้อมูล (scraping) แบบ Real-time
    - ตรวจสอบความสมบูรณ์ของข้อมูล และ performance เช่น ความถี่, ปริมาณ, เวลา
    - ทำงานร่วมกับ **Prefect** สำหรับการ Monitoring pipeline และ scheduling งานแบบอัตโนมัติ

    #### 3. Sentiment & Topic Classification
    - วิเคราะห์ข้อความจาก Social Media ด้วย **LLM (เช่น Gemini / GPT)**
    - จำแนก **topic** และ **subtopic** ของข้อความ
    - วิเคราะห์ **sentiment** ว่าผู้คนรู้สึกอย่างไร (positive / negative / neutral)
    - สร้างเป็นตารางสรุปผล และจัดเก็บข้อมูลให้นำไปใช้ต่อได้

    ### 🛠️ เทคโนโลยีที่ใช้
    - `Streamlit` สำหรับ frontend UI
    - `LakeFS` + `S3` สำหรับจัดการข้อมูล tweet แบบ version control
    - `Prefect` สำหรับ monitoring และ orchestration pipeline
    - `LLM API` (เช่น Gemini 2.0 หรือ OpenAI) สำหรับวิเคราะห์ข้อความ
    
    ---

    ⚙️ โครงการนี้ช่วยให้สามารถติดตามและเข้าใจความรู้สึกของผู้ใช้งาน Application X  
    ได้อย่างเป็นระบบและมีโครงสร้าง พร้อมรองรับการทำงานแบบ Real-time & ขยายต่อในเชิงธุรกิจ
    """)

with tab2:
    st.header("Virtual Environment Setup⚙️")
    st.markdown("""
    1. **Create a virtual environment**
    ```bash
    python -m venv .venv
    ```
    2. **Activate the virtual environment**
    - Windows:
    ```bash
    .venv\\Scripts\\activate
    ```
    - macOS/Linux:
    ```bash
    source .venv/bin/activate
    ```
    3. **Run startup script**
    ```bash
    bash start.sh
    ```
    """)

