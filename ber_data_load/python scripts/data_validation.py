import pandas as pd

# Read the file into a DataFrame, handling rows with parsing errors
file_path = r"/Users/macbon/Documents/code/stash/ber_data_load/files/BERPublicsearch.txt"
try:
    df = pd.read_csv(file_path)
except pd.errors.ParserError as e:
    print(f"Error parsing file: {e}")
    df = pd.DataFrame()  # Create an empty DataFrame to continue processing

# Drop rows with missing values (NaN)
df.dropna(inplace=True)

# Reset the index
df.reset_index(drop=True, inplace=True)

# Perform additional data cleaning or validation as needed
# ...

# Write the cleaned data to a new file (e.g., CSV)
cleaned_file_path = "/Users/macbon/Documents/code/stash/ber_data_load/files/clean.csv"
df.to_csv(cleaned_file_path, index=False)

# Display the cleaned DataFrame
print("Cleaned DataFrame:")
# print(df)
