import pandas as pd
import chardet

def detect_encoding(path):
    """
    Detect the encoding of a file.
    """
    with open(path, 'rb') as file:
        result = chardet.detect(file.read())
    return result['encoding']

def clean_and_prepare_data(df, file_name):
    """
    Clean and prepare the data from the DataFrame.
    """
    print(f"\nProcessing {file_name} Data")

    # Replace NaN with None
    df = df.where(pd.notnull(df), None)

    # Remove duplicates
    if file_name == "Customer":
        df.drop_duplicates(subset='CustomerKey', keep='first', inplace=True)
    elif file_name == "Sales":
        df.drop_duplicates(subset='Order Number', keep='first', inplace=True)
    elif file_name == "Products":
        df.drop_duplicates(subset='ProductKey', keep='first', inplace=True)

    # Convert date formats
    if file_name == "Customer":
        df['Birthday'] = pd.to_datetime(df['Birthday'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
    elif file_name == "Sales": 
        df['Order Date'] = pd.to_datetime(df['Order Date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        df['Delivery Date'] = pd.to_datetime(df['Delivery Date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        df['Delivery Date'].replace('NaT', None, inplace=True)  # Replace NaT with None
    elif file_name == "Exchange_Rates":
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')

    # Clean decimal columns for Products
    if file_name == "Products":
        try:
            df['Unit Cost USD'] = df['Unit Cost USD'].replace('[\$,]', '', regex=True).astype(float)
            df['Unit Price USD'] = df['Unit Price USD'].replace('[\$,]', '', regex=True).astype(float)
        except ValueError as e:
            print(f"Error converting decimal columns in {file_name}: {e}")

    # Print data types
    print(f"Data types in {file_name}:")
    print(df.dtypes)

    # Check for missing values after cleaning
    print(f"Missing values in {file_name} after cleaning:")
    print(df.isnull().sum())

    # Print first few rows
    print(f"First few rows of {file_name} after cleaning:")
    print(df.head())

    return df
