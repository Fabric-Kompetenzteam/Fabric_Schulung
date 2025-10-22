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

-- #### Load Dim Date Table 

-- CELL ********************

CREATE OR REPLACE TABLE gold.dim_date AS
SELECT
    d AS OrderDate,
    day(d)   AS Day,
    month(d) AS Month,
    year(d)  AS Year,
    date_format(d, 'MMM-yyyy') AS mmmyyyy,
    date_format(d, 'yyyyMM')   AS yyyymm,
    dayofweek(d) AS DayOfWeek,
    weekofyear(d) AS WeekOfYear,
    CASE WHEN dayofweek(d) IN (1,7) THEN True ELSE False END AS IsWeekend
FROM (
    SELECT explode(sequence(
        to_date('2018-01-01'),
        to_date('2050-12-31'),
        interval 1 day
    )) AS d
) tmp
ORDER BY OrderDate;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
