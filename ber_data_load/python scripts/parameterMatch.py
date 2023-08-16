from sqlalchemy import create_engine
import pandas as pd

# Set up SQLAlchemy connection
server_name = "localhost"
database_name = "master"
username = "sa"
password = "someThingComplicated1234"

# Create the SQLAlchemy engine for SQL Server
engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"
)
# Fetch parameter names from the referenceLists table
query_parameters = "SELECT ParameterName FROM referenceLists"
parameter_names = pd.read_sql(query_parameters, engine)["ParameterName"]

# Fetch data from the cleaned_ber_data table using pandas
query_data = "SELECT * FROM cleaned_ber_data"
df = pd.read_sql(query_data, engine)

# Unpivot the data using pandas
melted_df = df.melt(
    id_vars=["samplecode"],
    value_vars=parameter_names,
    var_name="ParameterName",
    value_name="ParameterValue",
)

# Filter out rows with NULL values
melted_df = melted_df.dropna(subset=["ParameterValue"])

# Print the melted dataframe
print(melted_df)
