# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE if not EXISTS curated.dimtriptype_loaded
# MAGIC (
# MAGIC     TripTypeSID INT,
# MAGIC     TripTypeCode VARCHAR(10),
# MAGIC     TripTypeName VARCHAR(50),
# MAGIC     TripTypeDescription VARCHAR(100),
# MAGIC     InsertTimestamp DATETIME2
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO curated.dimtriptype
# MAGIC (
# MAGIC     TripTypeSID,
# MAGIC     TripTypeCode,
# MAGIC     TripTypeName,
# MAGIC     TripTypeDescription,
# MAGIC     InsertTimestamp
# MAGIC )
# MAGIC SELECT
# MAGIC     TripTypeSID,
# MAGIC     TripTypeCode,
# MAGIC     TripTypeName,
# MAGIC     TripTypeDescription,
# MAGIC     CURRENT_TIMESTAMP AS InsertTimestamp
# MAGIC FROM curated.dimtriptype;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
