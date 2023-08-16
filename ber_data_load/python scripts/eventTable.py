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
ber_query = (
    "SELECT distinct samplecode, DateOfAssessment, CountyName FROM cleaned_ber_data"
)
df_ber = pd.read_sql(ber_query, engine)

# Fetch data from geo_coordinates table
geo_query = "SELECT CountyName, Latitude, Longitude FROM geo_coordinates"
df_geo = pd.read_sql(geo_query, engine)

# Merge data from cleaned_ber_data and geo_coordinates
df_event = pd.merge(df_ber, df_geo, on="CountyName", how="inner")

# table content
df_event["EventID"] = "BER-" + df_event["samplecode"].astype(str)
df_event["ParentEventID"] = None
df_event["DatasetID"] = "12345xyz"
df_event["DatasetName"] = "@DATASETNAME"
df_event["DateTime"] = df_event["DateOfAssessment"]
df_event["DateTimeAccuracy"] = df_event.apply(
    lambda row: "Date Only"
    if pd.Timestamp(row["DateTime"]).time() == pd.Timestamp("00:00:00").time()
    else "Date & Time",
    axis=1,
)
df_event["DateTimeAttribute"] = "Point"
df_event["TimeZoneOffset"] = 0.0
df_event["DateTimeISO"] = pd.to_datetime(df_event["DateTime"]).apply(
    lambda x: x.isoformat() + "+00:00"
)
df_event["StartDateTime"] = df_event["DateOfAssessment"]
df_event["EndDateTime"] = df_event["DateOfAssessment"]
df_event["LatLongAttribute"] = "Point"
df_event["LocationLabel"] = df_event["CountyName"]
df_event["FootprintWKT"] = (
    "POINT ("
    + df_event["Latitude"].astype(str)
    + " "
    + df_event["Longitude"].astype(str)
    + ")"
)
df_event["coordinateuncertaintyinmeters"] = 1.11
df_event["extractTime"] = datetime.now()
df_event["modifiedonDateTime"] = datetime.now()

# Filter out NULLs
df_event = df_event.dropna(
    subset=["Latitude", "Longitude", "DateOfAssessment", "CountyName"]
)

table_name = "event"
df_event.to_sql(table_name, engine, if_exists="replace", index=False)

# time to complete
end_time = datetime.now()
CompletionTime = end_time - start_time

print(f"Done in {CompletionTime}")
