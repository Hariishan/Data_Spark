use data_spark;
describe products;
SELECT 
    p.`Product Name`,
    SUM(s.Quantity) AS TotalQuantitySold
FROM sales s
JOIN products p ON s.ProductKey = p.ProductKey
GROUP BY p.`Product Name`
ORDER BY TotalQuantitySold DESC;

SELECT 
    p.`Product Name`,
    SUM(s.Quantity) AS TotalQuantitySold
FROM sales s
JOIN products p ON s.ProductKey = p.ProductKey
GROUP BY p.`Product Name`
ORDER BY TotalQuantitySold ASC;


SELECT 
    p.Category,
    SUM(s.Quantity * p.`Unit Price USD`) AS TotalRevenue
FROM sales s
JOIN products p ON s.ProductKey = p.ProductKey
GROUP BY p.Category;
