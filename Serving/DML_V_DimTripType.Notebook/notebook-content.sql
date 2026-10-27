-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "0d9986bc-9b02-42ae-96f2-65f4f10d62bb",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "0d9986bc-9b02-42ae-96f2-65f4f10d62bb",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE VIEW serving.V_DimTripType
AS
SELECT *
FROM curated.dimtriptype;

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
FROM [AlexLakehouse].[serving].[V_DimTripType]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
