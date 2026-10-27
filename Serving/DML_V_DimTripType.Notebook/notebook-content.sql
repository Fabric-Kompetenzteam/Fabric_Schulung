-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "2c61c0b7-0586-4af6-a62c-ff62bc3bf893",
-- META       "default_lakehouse_name": "Lakehouse",
-- META       "default_lakehouse_workspace_id": "e12200f8-607f-4f0d-9d2f-2b449b15ed36",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "2c61c0b7-0586-4af6-a62c-ff62bc3bf893"
-- META         }
-- META       ]
-- META     },
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

CREATE VIEW serving.v_DimTripTypes AS
select * from curated.dimtriptype


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
