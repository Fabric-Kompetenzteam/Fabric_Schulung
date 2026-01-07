-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "8da25e8a-250a-4838-9cc3-208424194ae2",
-- META       "default_lakehouse_name": "Schulung",
-- META       "default_lakehouse_workspace_id": "1d8f6de1-70b1-4989-ac36-66143ee32cfd",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "8da25e8a-250a-4838-9cc3-208424194ae2"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "e9e600c8-8fbf-4537-a04d-a01bc2063fb8",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "e9e600c8-8fbf-4537-a04d-a01bc2063fb8",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

create schema serving

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

create view serving.FaktSales as 
select * from curatedzone.facttaxitrip

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
