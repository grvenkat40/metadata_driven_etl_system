import streamlit as st
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import subprocess

st.title("🚀 Metadata-Driven ETL Control Panel")

con = create_engine("postgresql://postgres:2040@localhost:5432/metadataetl")

conn = psycopg2.connect(
    user="postgres",
    host = 'localhost',
    password = '2040',
    port = '5432',
    database = 'metadataetl'
)


cursor = conn.cursor()

cursor.execute("SELECT source_id, source_name FROM source_config")
sources = cursor.fetchall()

source_dict = {name: s_id for s_id, name in sources}

selected_source = st.selectbox("Select Data Source", list(source_dict.keys()))
source_id = source_dict[selected_source]

if st.button("Run ETL Pipeline"):
    try:
        subprocess.run(["python", "../etl/spark_job.py"], check=True)
        st.success("ETL executed successfully!")
    except Exception as e:
        st.error(f"Error {e}")

st.subheader(" ETL Run Logs")

query = """
SELECT source_id, run_time, status, rows_processed 
FROM etl_run_log 
ORDER BY run_time DESC LIMIT 10
"""

df_logs = pd.read_sql(query, con)

st.dataframe(df_logs)

st.subheader("📁 Processed Data Preview")

try:
    df_preview = pd.read_parquet(f"data/processed/clean_sales")
    st.dataframe(df_preview.head(10))

except Exception as e:
    st.warning("No processed data found yet.")

