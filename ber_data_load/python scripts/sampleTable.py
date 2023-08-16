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

# Fetch data from SQL
query = "SELECT DISTINCT samplecode, TypeofRating FROM cleaned_ber_data"
df = pd.read_sql(query, engine)

# Create SampleID and ParentSampleID
df["EventID"] = "BER-" + df["samplecode"].astype(str)
df["SampleID"] = df["EventID"] + "-" + df["TypeofRating"].astype(str)
df["ParentSampleID"] = None
df["extractonDateTime"] = datetime.now()
df["modifiedOnDatetime"] = datetime.now()

# Filter out NULLs
df = df.dropna(subset=["samplecode", "TypeofRating"])

table_name = "sample"
df.to_sql(table_name, engine, if_exists="replace", index=False)

# time to complete
end_time = datetime.now()
CompletionTime = end_time - start_time

print(f"Done in {CompletionTime}")
