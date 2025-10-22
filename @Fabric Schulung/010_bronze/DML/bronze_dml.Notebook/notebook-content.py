# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c8c79dd1-6d40-477c-9fd0-0001662a441b",
# META       "default_lakehouse_name": "FabricSchulungLakehouse",
# META       "default_lakehouse_workspace_id": "4cf3f36b-c6f2-4a2f-b02f-2dfc15f9936e",
# META       "known_lakehouses": [
# META         {
# META           "id": "c8c79dd1-6d40-477c-9fd0-0001662a441b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

from pyspark.sql.functions import col, input_file_name
from pyspark.sql.types import DecimalType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Delta Load Raw Table

# CELL ********************

# 1. Define your column names
columns = [
    "SalesOrderNumber",
    "SalesOrderLineNumber",
    "OrderDate",
    "CustomerName",
    "Email",
    "Item",
    "Quantity",
    "UnitPrice",
    "Tax"
]

# 2. Read all CSV files (no headers)
df = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load("Files/SalesRawData/") \
    .toDF(*columns)

# 4. Add source file column for lineage
df = df.withColumn("source_file", input_file_name())

# 5. Optional: filter out accidental header rows (if any file has a header by mistake)
df = df.filter(col("SalesOrderLineNumber").cast("int").isNotNull())

# 6. Register DataFrame as a temp view
df.createOrReplaceTempView("staging_salesorder")

# Run Spark SQL MERGE (upsert)
spark.sql("""
MERGE INTO bronze.salesorder AS target
USING staging_salesorder AS source
ON target.SalesOrderNumber = source.SalesOrderNumber
   AND target.SalesOrderLineNumber = source.SalesOrderLineNumber
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM bronze.salesorder LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
