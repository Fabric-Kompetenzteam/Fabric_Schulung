-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "779f45d9-bcc0-4161-a911-0fe4cf002b1b",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "779f45d9-bcc0-4161-a911-0fe4cf002b1b",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

drop view if exists serving.v_DimTripType
GO
create view serving.v_DimTripType
AS
select *
from curated.dimtriptype
GO

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
FROM [Lakehouse_WS01].[serving].[v_DimTripType]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
