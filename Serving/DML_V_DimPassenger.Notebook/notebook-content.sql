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
-- META       "default_warehouse": "555ba980-892c-4887-b163-2a211e3af276",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "555ba980-892c-4887-b163-2a211e3af276",
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
