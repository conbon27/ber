import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

server_name = "localhost"
database_name = "master"
username = "sa"
password = "someThingComplicated1234"

start_time = datetime.now()

engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Fetch data from SQL
cleaned_ber_data_query = """
SELECT samplecode, TypeofRating, DateOfAssessment
FROM cleaned_ber_data
WHERE DateOfAssessment >= '2010-01-19'
"""
reference_lists_query = "SELECT ParameterName FROM referenceLists"
df_cleaned_ber_data = pd.read_sql(cleaned_ber_data_query, engine)
df_reference_lists = pd.read_sql(reference_lists_query, engine)

# data manipulation
df_combined = pd.merge(df_cleaned_ber_data, df_reference_lists, how="cross")
df_combined["MeasurementID"] = (
    "BER-"
    + df_combined["samplecode"].astype(str)
    + "-"
    + df_combined["TypeofRating"].astype(str)
    + "-"
    + df_combined.groupby(["samplecode", "TypeofRating"])
    .cumcount()
    .add(1)
    .astype(str)
    .str.zfill(3)
)
df_combined["ProcessID"] = "Process-" + df_combined["ParameterName"]
df_combined["ProcessDescription"] = "blahblah"
df_combined["extractonDateTime"] = datetime.now()
df_combined["modifiedOnDatetime"] = datetime.now()

# Filter NULLs
df_combined = df_combined.dropna(subset=["samplecode", "TypeofRating", "ParameterName"])

table_name = "process"
df_combined.to_sql(table_name, engine, if_exists="replace", index=False)

# Time to complete
end_time = datetime.now()
completion_time = end_time - start_time

print(f"Done in {completion_time}")
