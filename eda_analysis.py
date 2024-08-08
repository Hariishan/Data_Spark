import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def eda_summary(df, file_name):
    """
    Perform summary statistics and basic EDA.
    """
    print(f"\nEDA Summary for {file_name}")

    # Print summary statistics
    print("\nSummary Statistics:")
    print(df.describe(include='all'))

    # Correlation matrix for numeric columns
    if df.select_dtypes(include=['number']).shape[1] > 1:
        plt.figure(figsize=(10, 8))
        sns.heatmap(df.corr(), annot=True, cmap='coolwarm', fmt='.2f')
        plt.title(f'Correlation Matrix for {file_name}')
        plt.show()

    # Check for unique values
    print("\nUnique values per column:")
    for col in df.columns:
        print(f"{col}: {df[col].nunique()} unique values")

def analyze_customer_demographics(df):
    """
    Analyze customer demographics including gender distribution, age (calculated from birthday), and location.
    """
    print("\nCustomer Demographics Analysis")
    
    # Ensure 'Birthday' column is a datetime
    df['Birthday'] = pd.to_datetime(df['Birthday'], errors='coerce')
    
    # Calculate age in years
    today = pd.Timestamp('today')
    df['Age'] = (today - df['Birthday']).dt.days // 365
    
    # Gender distribution
    plt.figure(figsize=(8, 6))
    sns.countplot(data=df, x='Gender')
    plt.title('Gender Distribution')
    plt.show()
    
    # Age distribution
    plt.figure(figsize=(8, 6))
    sns.histplot(df['Age'].dropna(), bins=20, kde=True)
    plt.title('Age Distribution')
    plt.show()
    
    # Location distribution
    plt.figure(figsize=(10, 8))
    df['City'].value_counts().head(10).plot(kind='bar')
    plt.title('Top 10 Cities by Number of Customers')
    plt.xlabel('City')
    plt.ylabel('Number of Customers')
    plt.show()

def analyze_customer_purchases(df):
    """
    Analyze customer purchase patterns, including average order value, frequency of purchases, and preferred products.
    """
    print("\nCustomer Purchase Patterns Analysis")

    if 'Quantity' in df.columns and 'Unit_Price_USD' in df.columns:
        df['Order_Value'] = df['Quantity'] * df['Unit_Price_USD']
        
        # Plot Order Value Distribution
        plt.figure(figsize=(8, 6))
        sns.histplot(df['Order_Value'].dropna(), bins=20, kde=True)
        plt.title('Order Value Distribution')
        plt.show()

        # Average Order Value
        avg_order_value = df['Order_Value'].mean()
        print(f"Average Order Value: ${avg_order_value:.2f}")

        # Total Orders by Product
        plt.figure(figsize=(10, 8))
        df['Product_Name'].value_counts().head(10).plot(kind='bar')
        plt.title('Top 10 Products by Number of Orders')
        plt.xlabel('Product')
        plt.ylabel('Number of Orders')
        plt.show()

def analyze_sales_trends(df):
    """
    Analyze sales trends over time.
    """
    print("\nSales Trends Analysis")

    if 'Order Date' in df.columns:
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
        df.set_index('Order Date', inplace=True)
        monthly_sales = df.resample('M').size()

        plt.figure(figsize=(12, 6))
        monthly_sales.plot()
        plt.title('Monthly Sales Trends')
        plt.xlabel('Date')
        plt.ylabel('Number of Sales')
        plt.show()

def analyze_products(df):
    """
    Analyze products including price distributions and product category breakdown.
    """
    print("\nProducts Analysis")

    if 'Unit_Price_USD' in df.columns:
        plt.figure(figsize=(8, 6))
        sns.histplot(df['Unit_Price_USD'].dropna(), bins=20, kde=True)
        plt.title('Product Price Distribution')
        plt.show()

    if 'Category' in df.columns:
        plt.figure(figsize=(10, 8))
        df['Category'].value_counts().plot(kind='bar')
        plt.title('Product Category Distribution')
        plt.xlabel('Category')
        plt.ylabel('Number of Products')
        plt.show()
