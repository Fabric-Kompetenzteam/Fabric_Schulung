-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "2133fd60-127a-495e-bb59-794a9bba8168",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "2133fd60-127a-495e-bb59-794a9bba8168",
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
