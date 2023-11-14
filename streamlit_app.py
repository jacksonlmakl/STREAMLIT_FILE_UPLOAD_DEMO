import pandas as pd
import streamlit as st
st.header('Upload Data Files')
uploaded_file = st.file_uploader('Upload a Parquet File')
uploaded_json_file = st.file_uploader('Upload JSON Data Transformation Files')

df = pd.read_parquet(uploaded_file, engine='pyarrow')
st.write(df)
