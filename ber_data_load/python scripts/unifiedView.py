import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

server_name = "localhost"
database_name = "master"
username = "sa"
password = "someThingComplicated1234"

start_time = datetime.now()

# Create the SQLAlchemy engine for SQL Server
engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Fetch from SQL
measurement_query = "SELECT * FROM measurement"
event_query = "SELECT * FROM event"
sample_query = "SELECT * FROM sample"
process_query = "SELECT * FROM process"

# Read SQL into DataFrames
df_measurement = pd.read_sql(measurement_query, engine)
df_event = pd.read_sql(event_query, engine)
df_sample = pd.read_sql(sample_query, engine)
df_process = pd.read_sql(process_query, engine)

columns_to_drop = ['modifiedOnDateTime', 'extractOnDateTime']

df_measurement = df_measurement.drop(columns=columns_to_drop, errors="ignore")
df_event = df_event.drop(columns=columns_to_drop, errors="ignore")
df_sample = df_sample.drop(columns=columns_to_drop, errors="ignore")
df_process = df_process.drop(columns=columns_to_drop, errors="ignore")

# Merge based on FKs
df_unified = pd.merge(df_event, df_sample, on="EventID", how="inner")
df_unified = pd.merge(df_unified, df_measurement, on="SampleID", how="inner")
df_unified = pd.merge(df_unified, df_process, on="MeasurementID", how="inner")

table_name = "esmpo_ber"
df_unified.to_sql(table_name, engine, if_exists="replace", index=False)

# Time to complete
end_time = datetime.now()
completion_time = end_time - start_time

print(f"Done in {completion_time}")
