import pandas as pd
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Set up SQLAlchemy connection
server_name = "localhost"
database_name = "master"
username = "sa"
password = "someThingComplicated1234"

# Create the SQLAlchemy engine for SQL Server
engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Query unique CountyNames from the table
query = "SELECT DISTINCT CountyName FROM cleaned_ber_data"
county_df = pd.read_sql(query, engine)

# Initialize geolocator
geolocator = Nominatim(user_agent="geoapiExercises")


# Function to get latitude and longitude
def get_lat_lon(location_name):
    try:
        location = geolocator.geocode(location_name)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except GeocoderTimedOut:
        return None, None


# Add latitude and longitude columns to the dataframe
county_df["Latitude"], county_df["Longitude"] = zip(
    *county_df["CountyName"].apply(get_lat_lon)
)

# If geocoding fails, set default latitude and longitude values
default_latitude = 0.0
default_longitude = 0.0
county_df["Latitude"].fillna(default_latitude, inplace=True)
county_df["Longitude"].fillna(default_longitude, inplace=True)

# Print the resulting dataframe
print(county_df)

# Update the original table with the latitude and longitude values
county_df.to_sql(
    "cleaned_ber_data", con=engine, index=False, if_exists="append", chunksize=1000
)
