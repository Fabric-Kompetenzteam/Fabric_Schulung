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

-- #### Erstellung der Dim Date Tabelle

-- CELL ********************

-- MAGIC %%sql 
-- MAGIC CREATE SCHEMA IF NOT EXISTS gold

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************


-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE IF NOT EXISTS gold.dim_date (
-- MAGIC     OrderDate DATE,
-- MAGIC     Day INT,
-- MAGIC     Month INT,
-- MAGIC     Year INT,
-- MAGIC     mmmyyyy STRING,
-- MAGIC     yyyymm STRING
-- MAGIC );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
