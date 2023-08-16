import pandas as pd


def normalize_input_file(input_file, output_file, delimiter):
    # Read the input file
    with open(input_file, "r") as file:
        lines = file.readlines()

    # Normalize the length and width of each line
    normalized_lines = []
    for line in lines:
        fields = line.strip().split(delimiter)
        normalized_fields = [field if field != "" else None for field in fields]
        normalized_lines.append(normalized_fields)

    # Create a DataFrame from the normalized lines
    df = pd.DataFrame(normalized_lines)

    # Write the normalized data to the output file
    df.to_csv(output_file, index=False, header=False)

    print(f"Normalized data saved to '{output_file}'")


def validate_data(file_path):
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(file_path)
    except pd.errors.ParserError:
        print("Error: Parsing error occurred. Problematic lines were removed.")
        return False

    # Validate column names
    if df.columns.duplicated().any():
        print("Error: Duplicate column names found!")
        return False

    # Validate expected data types
    expected_types = {"column1": int, "column2": str, "column3": float}
    invalid_columns = [
        (col, df[col].dtype)
        for col, dtype in expected_types.items()
        if df[col].dtype != dtype
    ]
    if invalid_columns:
        print("Error: Incorrect data types for columns:")
        for col, dtype in invalid_columns:
            print(f"  - Column '{col}': Expected '{dtype}', got '{df[col].dtype}'")
        return False

    # Handle missing values
    if df.isnull().values.any():
        print("Warning: Missing values found in the data")
        # You can choose to handle missing values based on your requirements, such as imputation or data exclusion

    # TODO: Check data integrity (e.g., unique keys, duplicates)

    # TODO: Validate data formats (e.g., dates, numbers, strings)

    return True


def clean_data(file_path, output_file):
    if validate_data(file_path):
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(file_path)

        # Perform data cleaning/transformation operations
        df_cleaned = df.drop_duplicates()

        # Write the cleaned data to a new CSV file
        df_cleaned.to_csv(output_file, index=False)

        print(f"Cleaned data saved to '{output_file}'")
    else:
        print("Data validation failed. Please review and fix the issues.")


# Input file paths
input_file = "ber_data_load/files/BERPublicsearch.txt"
normalized_output_file = "ber_data_load/files/normalized_output.csv"
cleaned_output_file = "ber_data_load/files/cleaned_data.csv"

# Normalize the input file
normalize_input_file(input_file, normalized_output_file, delimiter="\t")

# Clean the normalized file
clean_data(normalized_output_file, cleaned_output_file)
