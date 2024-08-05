import pandas as pd
import mysql.connector
from mysql.connector import Error
import chardet

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
        # Handle Delivery Date, convert to None if not a valid date
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

def detect_encoding(path):
    """
    Detect the encoding of a file.
    """
    with open(path, 'rb') as file:
        result = chardet.detect(file.read())
    return result['encoding']

def create_table(table_name, cursor):
    """
    Create table in MySQL if it does not exist.
    """
    create_table_sql = {
        "Customer": """
            CREATE TABLE IF NOT EXISTS `Customer` (
                `CustomerKey` INT PRIMARY KEY,
                `Gender` VARCHAR(10),
                `Name` VARCHAR(255),
                `City` VARCHAR(100),
                `State Code` VARCHAR(50),
                `State` VARCHAR(100),
                `Zip Code` VARCHAR(20),
                `Country` VARCHAR(100),
                `Continent` VARCHAR(100),
                `Birthday` DATE
            )
        """,
        "Sales": """
            CREATE TABLE IF NOT EXISTS `Sales` (
                `Order Number` INT PRIMARY KEY,
                `Line Item` INT,
                `Order Date` DATE,
                `Delivery Date` DATE,
                `CustomerKey` INT,
                `StoreKey` INT,
                `ProductKey` INT,
                `Quantity` INT,
                `Currency Code` VARCHAR(10),
                FOREIGN KEY (`CustomerKey`) REFERENCES `Customer`(`CustomerKey`),
                FOREIGN KEY (`ProductKey`) REFERENCES `Products`(`ProductKey`)
            )
        """,
        "Products": """
            CREATE TABLE IF NOT EXISTS `Products` (
                `ProductKey` INT PRIMARY KEY,
                `Product Name` VARCHAR(255),
                `Brand` VARCHAR(100),
                `Color` VARCHAR(50),
                `Unit Cost USD` DECIMAL(10, 2),
                `Unit Price USD` DECIMAL(10, 2),
                `SubcategoryKey` INT,
                `Subcategory` VARCHAR(100),
                `CategoryKey` INT,
                `Category` VARCHAR(100)
            )
        """,
        "Exchange_Rates": """
            CREATE TABLE IF NOT EXISTS `Exchange_Rates` (
                `Date` DATE,
                `Currency` VARCHAR(10),
                `Exchange` DECIMAL(10, 4),
                PRIMARY KEY (`Date`, `Currency`)
            )
        """,
        "Data_Dictionary": """
            CREATE TABLE IF NOT EXISTS `Data_Dictionary` (
                `Table_Name` VARCHAR(100),
                `Field_Name` VARCHAR(100),
                `Description` TEXT
            )
        """
    }
    create_table_sql_statement = create_table_sql.get(table_name, "")
    if create_table_sql_statement:
        try:
            cursor.execute(create_table_sql_statement)
            print(f"Table `{table_name}` created or already exists.")
        except Error as e:
            print(f"Error creating table `{table_name}`: {e}")

def insert_data(table_name, df, cursor):
    """
    Insert data into the specified table.
    """
    if not df.empty:
        columns = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Ensure no None values are inserted into the database
        data_tuples = [tuple(None if pd.isna(x) else x for x in row) for row in data_tuples]

        try:
            # Handle primary key duplicates
            if table_name in ["Sales", "Products", "Customer"]:
                primary_key_column = {
                    "Sales": '`Order Number`',
                    "Products": '`ProductKey`',
                    "Customer": '`CustomerKey`'
                }.get(table_name)
                
                if primary_key_column:
                    existing_keys_query = f"SELECT {primary_key_column} FROM `{table_name}`"
                    cursor.execute(existing_keys_query)
                    existing_keys = {row[0] for row in cursor.fetchall()}

                    # Filter out rows with existing primary keys
                    df = df[~df[primary_key_column.strip('`')].isin(existing_keys)]

                    if df.empty:
                        print(f"No new data to insert into {table_name}.")
                        return

            cursor.executemany(insert_sql, data_tuples)
            print(f"Data inserted into {table_name} successfully.")
        except Error as e:
            print(f"Error inserting data into {table_name}: {e}")
    else:
        print(f"No data to insert for {table_name}.")

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
    except Exception as e:
        print(f"Failed to read and process {key} data: {e}")

# Connect to MySQL
conn = None
try:
    conn = mysql.connector.connect(
        host='localhost',
        database='data_spark',
        user='root',
        password='Iloveall@12345'  # Update this with your MySQL password
    )
    
    if conn.is_connected():
        print("Connected to MySQL database")
        cursor = conn.cursor()

        # Create tables if they do not exist
        for table_name in ["Customer", "Sales", "Products", "Exchange_Rates", "Data_Dictionary"]:
            create_table(table_name, cursor)
        
        # Insert data into tables
        for table_name, df in data_frames.items():
            insert_data(table_name, df, cursor)
            conn.commit()

except Error as e:
    print(f"Error connecting to MySQL: {e}")

finally:
    if conn is not None and conn.is_connected():
        cursor.close
