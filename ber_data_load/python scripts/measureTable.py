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

# Fetch data from cleaned_ber_data and referenceLists tables
query_cleaned_ber_data = "SELECT * FROM cleaned_ber_data"
query_reference_lists = "SELECT * FROM referenceLists"
df_cleaned_ber_data = pd.read_sql(query_cleaned_ber_data, engine)
df_reference_lists = pd.read_sql(query_reference_lists, engine)

# Recreate SampleID and MeasurementID
df_cleaned_ber_data["SampleID"] = (
    "BER-"
    + df_cleaned_ber_data["samplecode"].astype(str)
    + "-"
    + df_cleaned_ber_data["TypeofRating"]
)
df_cleaned_ber_data["MeasurementID"] = (
    "BER-"
    + df_cleaned_ber_data["samplecode"].astype(str)
    + "-"
    + df_cleaned_ber_data["TypeofRating"]
    + "-"
    + df_cleaned_ber_data.groupby(["samplecode", "TypeofRating"])
    .cumcount()
    .add(1)
    .astype(str)
    .str.zfill(3)
)

# Empty list for transformed data
transformed_data = []

# Iterate through each row
for _, row in df_cleaned_ber_data.iterrows():
    # Get the SampleID and MeasurementID
    sample_id = row["SampleID"]
    measurement_id = row["MeasurementID"]
    date_of_assessment = row["DateOfAssessment"]

    # Iterate through referenceLists rows
    for _, param_row in df_reference_lists.iterrows():
        parameter_name = param_row["ParameterName"]
        parameter_id = param_row["ParameterID"]
        observed_property = parameter_name  # Assumes this matches

        # Get the value from cleaned_ber_data
        parameter_value = row[parameter_name]

        # Append transformed data to the list
        transformed_data.append(
            {
                "SampleID": sample_id,
                "MeasurementID": measurement_id,
                "ObservedProperty": observed_property,
                "Parameter": parameter_id,
                "ParameterValue": parameter_value,
                "extractOnDateTime": datetime.now(),
                "modifiedOnDateTime": datetime.now(),
            }
        )

df_transformed = pd.DataFrame(transformed_data)

# Filter out NULLs
df_transformed = df_transformed.dropna(subset=["ParameterValue"])

table_name = "measurements"
df_transformed.to_sql(table_name, engine, if_exists="replace", index=False)

# time to complete
end_time = datetime.now()
CompletionTime = end_time - start_time

print(f"Done in {CompletionTime}")
