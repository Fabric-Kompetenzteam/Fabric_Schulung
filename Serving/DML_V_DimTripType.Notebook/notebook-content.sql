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
-- META       "default_warehouse": "39458ee6-675b-4328-ac84-415d77c61d51",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "39458ee6-675b-4328-ac84-415d77c61d51",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

Create View serving.V_FactTaxitrip AS
SELECT * FROM curated.facttaxitrip

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
