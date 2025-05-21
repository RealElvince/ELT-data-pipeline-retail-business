SELECT 
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    Country
FROM {{ source('retail_dataset','online_retail')}}
WHERE InvoiceNo IS NOT NULL
    AND StockCode IS NOT NULL
    AND Description IS NOT NULL
    AND Quantity IS NOT NULL
    AND InvoiceDate IS NOT NULL
    AND UnitPrice IS NOT NULL
    AND CustomerID IS NOT NULL
    AND Country IS NOT NULL