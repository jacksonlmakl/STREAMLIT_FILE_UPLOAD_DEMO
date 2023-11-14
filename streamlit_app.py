import pandas as pd
import streamlit as st
import json
st.header('Upload Data Files')
uploaded_file = st.file_uploader('Upload a Parquet File')
uploaded_json_file = st.file_uploader('Upload JSON Data Transformation Files')
if uploaded_file != None:
  df = pd.read_parquet(uploaded_file, engine='pyarrow')
  st.write(df)
if uploaded_json_file != None:
  json_file = json.loads(uploaded_json_file)
  st.write(json_file)
