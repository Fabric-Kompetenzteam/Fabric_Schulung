# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "warehouse": {
# META       "default_warehouse": "555ba980-892c-4887-b163-2a211e3af276",
# META       "known_warehouses": [
# META         {
# META           "id": "555ba980-892c-4887-b163-2a211e3af276",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW Serving.DimTripTypes_View AS
# MAGIC SELECT * FROM curated.DimTripType;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
