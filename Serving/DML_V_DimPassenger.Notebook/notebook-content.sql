-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse_name": "",
-- META       "default_lakehouse_workspace_id": ""
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "5ed16993-6022-4842-8a5c-31969cc18c20",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "5ed16993-6022-4842-8a5c-31969cc18c20",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

Create View serving.V_DimPassenger AS
SELECT * from curated.dimpassenger

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
