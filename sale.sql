use data_spark;
describe sales;
describe products;
SELECT 
    DATE_FORMAT(`Order Date`, '%Y-%m') AS Month,
    SUM(s.Quantity * p.`Unit Price USD`) AS TotalSales
FROM sales s
JOIN products p ON s.ProductKey = p.ProductKey
GROUP BY Month
ORDER BY Month;



describe products;
SELECT 
    p.`Product Name` AS ProductName,
    SUM(s.Quantity) AS TotalQuantitySold,
    SUM(s.Quantity * p.`Unit Price USD`) AS TotalRevenue
FROM sales s
JOIN products p ON s.ProductKey = p.ProductKey
GROUP BY p.`Product Name`
ORDER BY TotalRevenue DESC;



SELECT 
    s.StoreKey, 
    SUM(s.Quantity * p.`Unit Price USD`) AS TotalSales
FROM sales s
JOIN products p ON s.ProductKey = p.ProductKey
GROUP BY s.StoreKey;


