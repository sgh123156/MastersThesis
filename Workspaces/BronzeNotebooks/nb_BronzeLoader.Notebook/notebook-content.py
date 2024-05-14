# PARAMETERS CELL ********************

Schema = "Spark DataFrame schema converted to JSON to enable passing it as a parameter"
SourceSystem = "Name of the source system"
SourceSchema = "Name of the schema of a table in the source (if applicable)"
SourceTable = "Name of the object in the source"
LoadMode = "0 for full-loaded tables, 1 for incrementally-loaded ones"
SourceKey = "PK of the table in the source system (if applicable)"
IncrementalPath = "Granular path for the recently ingested data, applicable only when LoadMode == 1"
FileFormat = "Format in which a data files are ingested"

# CELL ********************

import json
from datetime import datetime

from pyspark.sql.types import *
from pyspark.sql.functions import sha2, concat_ws, lit
from delta.tables import DeltaTable

# CELL ********************

parsed_schema = StructType.fromJson(json.loads(Schema))
write_mode = 'overwrite' if LoadMode == 0 else 'append'
overwrite_schema = 'true' if LoadMode == 0 else 'false'
timestamp = datetime.now()

# CELL ********************
# Load the Bronze-Landing data into Spark DataFrames with schema declared upfront
df = spark.read.format(FileFormat) \
        .schema(parsed_schema) \
        .load(f'abfss://Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Files/{SourceSystem}/{SourceSchema}/{LoadMode}/{SourceTable}/{IncrementalPath}/{SourceTable}.parquet')

df = df.withColumn("HashKey", sha2(concat_ws('|', 'Source', *[key.strip() for key in SourceKey.split(',')]), 256)) \
        .withColumn("BronzeRawTimestamp",lit(timestamp))

# Write data into Bronze-Raw
df.write.mode(write_mode) \
    .format('delta') \
    .option("overwriteSchema", overwrite_schema) \
    .save(f'abfss://Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Tables/{SourceSystem}_{SourceSchema}_{SourceTable}')

# Lookup staticts in the Delta log
dt = DeltaTable.forPath(spark, f'abfss://Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Tables/{SourceSystem}_{SourceSchema}_{SourceTable}')
rows_inserted = dt.history(1).collect()[0]["operationMetrics"]['numOutputRows']

# CELL ********************
# Return the statistics to the IngestionFramework pipeline
returnmsg = json.dumps({'RowsInserted': rows_inserted, 'Watermark':timestamp.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
