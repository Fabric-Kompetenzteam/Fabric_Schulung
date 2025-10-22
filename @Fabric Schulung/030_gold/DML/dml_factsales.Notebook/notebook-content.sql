-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "c8c79dd1-6d40-477c-9fd0-0001662a441b",
-- META       "default_lakehouse_name": "FabricSchulungLakehouse",
-- META       "default_lakehouse_workspace_id": "4cf3f36b-c6f2-4a2f-b02f-2dfc15f9936e",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "c8c79dd1-6d40-477c-9fd0-0001662a441b"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- #### Delta Load Fact Table 

-- CELL ********************

-- Load unique transactions from stage table 
INSERT INTO gold.fact_sales (CustomerID, ItemID, OrderDate, Quantity, UnitPrice, Tax)
SELECT
    dc.CustomerID,
    dp.ItemID,
    s.OrderDate,
    s.Quantity,
    s.UnitPrice,
    s.Tax
FROM silber.salesorder s
JOIN gold.dim_customer dc
    ON s.CustomerName = dc.CustomerName AND s.Email = dc.Email
JOIN gold.dim_product dp
    ON split(s.Item, ', ')[0] = dp.ItemName
       AND
       CASE
           WHEN size(split(s.Item, ', ')) > 1 THEN split(s.Item, ', ')[1]
           ELSE ''
       END = dp.ItemInfo
WHERE s.CustomerName IS NOT NULL AND s.Email IS NOT NULL AND s.Item IS NOT NULL;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM gold.fact_sales LIMIT 10

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
