-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "e0f42022-000a-46b1-9596-6667f06047dc",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "e0f42022-000a-46b1-9596-6667f06047dc",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE VIEW serving.V_DimTripType AS
SELECT * FROM [LakehouseVonHendrik].[curated].[dimtryiptype]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [TripTypeSID],
			[TripTypeCode],
			[TripTypeName],
			[TripTypeDescription]
FROM [LakehouseVonHendrik].[curated].[dimtryiptype]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
