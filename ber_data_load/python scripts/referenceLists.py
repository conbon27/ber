import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer

server_name = "localhost"
database_name = "master"
username = "sa"
password = "someThingComplicated1234"

engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"
)
query = "SELECT * FROM cleaned_ber_data"
df = pd.read_sql(query, engine)

# Create the new table
metadata = MetaData()
reference_lists = Table(
    "referenceLists",
    metadata,
    Column("ParameterID", Integer, primary_key=True),
    Column("ParameterName", String),
    Column("ParameterDescription", String),
    Column("Source", String),  
    Column("Unit", String), # Add more as needed
)
metadata.create_all(engine)

# Initialize ParameterID
param_id = 1000

# Insert data into the new table
with engine.connect() as connection:
    for column in df.columns:
        if column != "ParameterID":
            parameter_name = column
            parameter_description = (
                "Dummy Description"  
            )
            source = "Dummy Source"  
            unit = "unit"
            connection.execute(
                reference_lists.insert().values(
                    ParameterID=param_id,
                    ParameterName=parameter_name,
                    ParameterDescription=parameter_description,
                    Source=source,
                    Unit=unit,
                )
            )
            param_id += 1
