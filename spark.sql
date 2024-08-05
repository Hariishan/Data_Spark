
use  data_spark;

CREATE TABLE Customer (
    CustomerKey INT PRIMARY KEY,
    Gender VARCHAR(10),
    Name VARCHAR(255),
    City VARCHAR(100),
    `State Code` VARCHAR(50),
    State VARCHAR(100),
    `Zip Code` VARCHAR(20),
    Country VARCHAR(100),
    Continent VARCHAR(100),
    Birthday DATE
);

CREATE TABLE Sales (
    `Order Number` INT PRIMARY KEY,
    `Line Item` INT,
    `Order Date` DATE,
    `Delivery Date` DATE,
    CustomerKey INT,
    StoreKey INT,
    ProductKey INT,
    Quantity INT,
    `Currency Code` VARCHAR(10)
);

CREATE TABLE Products (
    ProductKey INT PRIMARY KEY,
    `Product Name` VARCHAR(255),
    Brand VARCHAR(100),
    Color VARCHAR(50),
    `Unit Cost USD` DECIMAL(10, 2),
    `Unit Price USD` DECIMAL(10, 2),
    SubcategoryKey INT,
    Subcategory VARCHAR(100),
    CategoryKey INT,
    Category VARCHAR(100)
);

CREATE TABLE Exchange_Rates (
    `Date` DATE,
    Currency VARCHAR(10),
    Exchange DECIMAL(10, 4)
);

CREATE TABLE Data_Dictionary (
    Table_Name VARCHAR(100),
    Field_Name VARCHAR(100),
    Description TEXT
);
use data_spark;
select * from products;
select *  from  data_dictionary;
select * from customer;
select * from sales;
select * from exchange_rates;
