import pandas as pd
from data_cleaning import clean_and_prepare_data, detect_encoding
from eda_analysis import eda_summary, analyze_customer_demographics, analyze_customer_purchases, analyze_sales_trends, analyze_products
from database_operations import create_table, insert_data, connect_to_mysql, close_mysql_connection

# File paths for the CSV files
file_paths = {
    "Customer": "C:/Users/harik/OneDrive/Desktop/project2/Customers.csv",
    "Sales": "C:/Users/harik/OneDrive/Desktop/project2/Sales.csv",
    "Products": "C:/Users/harik/OneDrive/Desktop/project2/Products.csv",
    "Exchange_Rates": "C:/Users/harik/OneDrive/Desktop/project2/Exchange_Rates.csv",
    "Data_Dictionary": "C:/Users/harik/OneDrive/Desktop/project2/Data_Dictionary.csv"
}

data_frames = {}
for key, path in file_paths.items():
    try:
        encoding = detect_encoding(path)
        df = pd.read_csv(path, encoding=encoding)
        print(f"File read successfully with encoding {encoding}: {path}")
        data_frames[key] = clean_and_prepare_data(df, key)
        
        # Perform EDA for each dataframe
        eda_summary(df, key)
        if key == "Customer":
            analyze_customer_demographics(df)
        elif key == "Sales":
            analyze_customer_purchases(df)
            analyze_sales_trends(df)
        elif key == "Products":
            analyze_products(df)
        
    except Exception as e:
        print(f"Failed to read and process {key} data: {e}")

# Connect to MySQL
conn, cursor = connect_to_mysql()

try:
    if conn.is_connected():
        # Create tables if they do not exist
        for table_name in ["Customer", "Sales", "Products", "Exchange_Rates", "Data_Dictionary"]:
            create_table(table_name, cursor)
        
        # Insert data into tables
        for table_name, df in data_frames.items():
            insert_data(table_name, df, cursor)
            conn.commit()

finally:
    close_mysql_connection(conn, cursor)
