import streamlit as st
import subprocess
import os

st.set_page_config(page_title="Tweet Analysis App", layout="wide")
st.title("Tweet Analyzer For DSI321 Project📊")

tab1, tab2 = st.tabs(["Overview", "Readme.md"])

with tab1:
    st.header("A dog")
    st.image("https://static.streamlit.io/examples/dog.jpg", width=200)

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

