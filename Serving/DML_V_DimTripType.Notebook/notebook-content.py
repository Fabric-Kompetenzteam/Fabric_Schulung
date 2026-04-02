# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d924b6f1-0b81-4bbb-a4b3-c0931bd46a70",
# META       "default_lakehouse_name": "HaSee",
# META       "default_lakehouse_workspace_id": "dcb08e0d-cea9-4b7a-bf78-34058b44b2eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "d924b6f1-0b81-4bbb-a4b3-c0931bd46a70"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Key Change: Use the correct table name as it exists in the lakehouse (case sensitive: 'dimtriptype', not 'DimTripTypes').
# MAGIC CREATE OR REPLACE VIEW serving.v_DimTripTypes AS
# MAGIC SELECT *
# MAGIC FROM curated.dimtriptype;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
