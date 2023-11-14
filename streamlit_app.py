import pandas as pd
import streamlit as st
import json
import psycopg2
st.header('Connect Postgres Database')
host = st.text_input('Enter Host')
username = st.text_input('Enter Username')
password = st.text_input('Enter Password',type='password')
database = st.text_input('Enter Database')
if st.button('Connect'):
  conn = psycopg2.connect(
      host=host,
      database=database,
      user=username,
      password=password)
  st.write("Connected Successfully")
  sql_code = st.text_areaa('Enter SQL code to execute')
  st.code(body = sql_code,language="sql")
  st.header('Execute SQL in Database')
  if st.button('Execute'):
    cur = conn.cursor()
    cur.execute(code)
    conn.commit()
    cur.close()
  

st.header('Upload Data Files')
uploaded_file = st.file_uploader('Upload a Parquet File')
uploaded_json_file = st.file_uploader('Upload JSON Data Transformation Files')
if uploaded_file != None:
  df = pd.read_parquet(uploaded_file, engine='pyarrow')
  if st.button('Create Table From Uploaded Data'):
    table_name = st.text_input('Table Name')
    sql_code = pd.io.sql.get_schema(df.reset_index(), table_name)
    cur = conn.cursor()
    cur.execute(sql_code)
    conn.commit()
    cur.close()
  st.write(df)
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
  if st.button('Create Table From Transformed Data'):
    table_name = st.text_input('Table Name')
    sql_code = pd.io.sql.get_schema(df_new.reset_index(), table_name)
    cur = conn.cursor()
    cur.execute(sql_code)
    conn.commit()
    cur.close()
    
  
