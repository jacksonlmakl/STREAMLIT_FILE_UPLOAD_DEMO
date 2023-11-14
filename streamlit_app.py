import pandas as pd
import streamlit as st
st.header('Multiple File Upload')
uploaded_files = st.file_uploader('Upload your files',
 accept_multiple_files=True)

for f in uploaded_files:
    st.write(f)

data_list = []
for f in uploaded_files:
    data = pd.read_csv(f)
    data_list.append(data)

for df in data_list:
    st.write(df)
