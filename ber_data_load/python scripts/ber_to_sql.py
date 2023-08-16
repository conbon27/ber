import pandas as pd
from sqlalchemy import create_engine

# Connection details
server = "localhost"  # Use the name of the SQL Server container
database = "master"
username = "sa"
password = "someThingComplicated1234"

# connection string
conn_str = (
    "mssql+pyodbc://" + username + ":" + password + "@" + server + "/" + database + "?driver=ODBC+Driver+17+for+SQL+Server"
)

# Create SQLAlchemy engine
engine = create_engine(conn_str)

# Read the large text file into a pandas DataFrame
file_path = r"ber_data_load/files/normalized_output.csv"  # Update the file path based on container file location
df = pd.read_csv(file_path)
print(df.head())

# Create table using the DataFrame headers as column names
table_name = "cleaned_ber_data"
df.to_sql(table_name, engine, if_exists="replace", index=False)

# Close connection
engine.dispose()
