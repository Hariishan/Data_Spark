SELECT 
    s.StoreKey,
    COUNT(s.`Order Number`) AS NumberOfOrders,
    SUM(s.Quantity * p.`Unit Price USD`) AS TotalSales
FROM sales s
JOIN products p ON s.ProductKey = p.ProductKey
GROUP BY s.StoreKey;

SELECT 
    c.City, 
    c.State, 
    c.Country, 
    SUM(s.Quantity * p.`Unit Price USD`) AS TotalSales
FROM sales s
JOIN customer c ON s.CustomerKey = c.CustomerKey
JOIN products p ON s.ProductKey = p.ProductKey
GROUP BY c.City, c.State, c.Country;

