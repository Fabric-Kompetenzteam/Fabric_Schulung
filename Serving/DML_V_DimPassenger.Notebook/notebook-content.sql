-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "34bfacb5-5e04-4ed9-a5ea-aaac0559d9ba",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "34bfacb5-5e04-4ed9-a5ea-aaac0559d9ba",
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
