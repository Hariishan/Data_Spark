SELECT 
    `Table_Name`,
    `Field_Name`
FROM 
    `Data_Dictionary`
ORDER BY 
    `Table_Name`, `Field_Name`;
-------
SELECT 
    `Field_Name`,
    `Description`
FROM 
    `Data_Dictionary`
WHERE 
    `Table_Name` = 'Sales';
------
SELECT 
    `Field_Name`,
    `Description`
FROM 
    `Data_Dictionary`
WHERE 
    `Table_Name` = 'Customer' AND `Field_Name` = 'Birthday';
------
SELECT 
    `Table_Name`,
    COUNT(`Field_Name`) AS `Field_Count`
FROM 
    `Data_Dictionary`
GROUP BY 
    `Table_Name`
HAVING 
    `Field_Count` > 5;
------
SELECT 
    `Field_Name`,
    `Description`
FROM 
    `Data_Dictionary`
WHERE 
    `Table_Name` = 'Products';
------
SELECT 
    COUNT(*) AS `Field_Count`
FROM 
    `Data_Dictionary`;
------
SELECT 
    `Table_Name`,
    COUNT(*) AS `Missing_Descriptions`
FROM 
    `Data_Dictionary`
WHERE 
    `Description` IS NULL OR `Description` = ''
GROUP BY 
    `Table_Name`;
-----
SELECT 
    `Field_Name`,
    COUNT(*) AS `Occurrences`
FROM 
    `Data_Dictionary`
GROUP BY 
    `Field_Name`
ORDER BY 
    `Occurrences` DESC
LIMIT 10;
-----
SELECT 
    `Field_Name`,
    `Description`
FROM 
    `Data_Dictionary`
WHERE 
    `Table_Name` = 'Exchange_Rates'
ORDER BY 
    `Field_Name`;
-----
SELECT 
    `Field_Name`,
    `Description`
FROM 
    `Data_Dictionary`
WHERE 
    `Table_Name` = 'Customer'
ORDER BY 
    `Field_Name`;
