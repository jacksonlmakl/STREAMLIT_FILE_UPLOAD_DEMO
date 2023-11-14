import pandas as pd
import streamlit as st
import json
import psycopg2
import requests
import json

def convert_df_to_csv(df):
  # IMPORTANT: Cache the conversion to prevent computation on every rerun
  return df.to_csv().encode('utf-8')
    
def get_desc_stats(df):
    json_data = df.head(100).to_dict(orient="records")
    api_key = st.secrets["api_key"]
    payload = {
      "model": "gpt-3.5-turbo",
      "messages": [
        {
          "role": "system",
          "content": "You will be given data in JSON, you must return descriptive analytics on the data provided"
        },
        {
          "role": "user",
          "content": json.dumps(json_data)
        }
      ],
      "temperature": 0,
      "max_tokens": 256
        }
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.post(url = "https://api.openai.com/v1/chat/completions", json = payload ,headers = headers)
    return response.json()['choices'][0]['message']['content']
    
def get_trend_stats(df):
    json_data = df.head(100).to_dict(orient="records")
    api_key = st.secrets["api_key"]
    payload = {
      "model": "gpt-3.5-turbo",
      "messages": [
        {
          "role": "system",
          "content": "You will be given data in JSON, you must return a trend analysis on the data provided"
        },
        {
          "role": "user",
          "content": json.dumps(json_data)
        }
      ],
      "temperature": 0,
      "max_tokens": 256
        }
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.post(url = "https://api.openai.com/v1/chat/completions", json = payload ,headers = headers)
    return response.json()['choices'][0]['message']['content']  
 
def insert_statement(SOURCE, TARGET):
    sql_texts = []
    for index, row in SOURCE.iterrows():       
        sql_texts.append('INSERT INTO '+TARGET+' ('+ str(', '.join(SOURCE.columns))+ ') VALUES '+ str(tuple(row.values)))        
    return sql_texts


st.header('Connect Postgres Database')
host = st.text_input('Enter Host')
username = st.text_input('Enter Username')
password = st.text_input('Enter Password',type='password')
database = st.text_input('Enter Database')
if st.button('Connect'):
    conn = psycopg2.connect(host=host,database=database,user=username,password=password)
    st.write("Connected Successfully")
    st.header('Execute SQL in Database')
    sql_code = st.text_area('Enter SQL code to execute')
  
    if st.button('Execute'):
        cur = conn.cursor()
        cur.execute(sql_code)
        conn.commit()
        cur.close()
  

st.header('Upload Data Files')
uploaded_file = st.file_uploader('Upload a Parquet File')
uploaded_json_file = st.file_uploader('Upload JSON Data Transformation Files')
if uploaded_file != None:
    df = pd.read_parquet(uploaded_file, engine='pyarrow')
    st.write(df)
    if st.button('Get Automated Descriptive Stats'):
        resp = get_desc_stats(df)
        st.write(resp)
    if st.button('Get Automated Trend Analysis'):
        resp = get_desc_stats(df)
        st.write(resp)
        
    if st.button('Create Table From Uploaded Data'):
        table_name = st.text_input('Table Name')
        if st.button('Execute'):
            sql_code = pd.io.sql.get_schema(df.reset_index(), table_name)
            cur = conn.cursor()
            cur.execute(sql_code)
            conn.commit()
            cur.close()
  
    if st.button('Insert Data From Uploaded Table Into Existing SQL table '):
        table_name = st.text_input('Table Name')
    if st.button('Execute'):
        sql_codes = insert_statement(df_new, table_name)
        for sql_code in sql_codes:
            cur = conn.cursor()
            cur.execute(sql_code)
            conn.commit()
            cur.close()



if uploaded_json_file != None:
    json_file = json.load(uploaded_json_file)
if st.button('Transform Data'):
    df_new = df
    transform_cols = list(json_file.keys())
    transformations = []
    for i in transform_cols:
        actions = list(json_file[i].keys())
    for action_name in actions:
        action = json_file[i][action_name]
        transformations.append((i,action_name,action))
    for transformation in transformations:
        col_name = transformation[0]
        pd_method = transformation[1]
        pd_method_value = transformation[2]
        eval_str = f"df_new['{col_name}'].{pd_method}({pd_method_value})"
        df_new[col_name] = eval(eval_str)
    st.write(df_new)

    st.download_button(
      label="Download Transformed Data as CSV",
      data=convert_df_to_csv(my_large_df),
      file_name='large_df.csv',
      mime='text/csv',
    )
    
    if st.button('Create Table From Transformed Table'):
        table_name = st.text_input('Table Name')
    if st.button('Execute'):
        sql_code = pd.io.sql.get_schema(df_new.reset_index(), table_name)
        cur = conn.cursor()
        cur.execute(sql_code)
        conn.commit()
        cur.close()
    if st.button('Insert Data From Transformed Table Into Existing SQL table '):
        table_name = st.text_input('Table Name')
        if st.button('Execute'):
            sql_codes = insert_statement(df_new, table_name)
        for sql_code in sql_codes:
            cur = conn.cursor()
            cur.execute(sql_code)
            conn.commit()
            cur.close()
    
  
