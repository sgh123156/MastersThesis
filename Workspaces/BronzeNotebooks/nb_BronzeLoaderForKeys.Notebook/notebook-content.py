# PARAMETERS CELL ********************

SourceSystem = "Name of the source system"
SourceSchema = "Name of the schema of a table in the source (if applicable)"
SourceTable = "Name of the object in the source"
LoadMode = "0 for full-loaded tables, 1 for incrementally-loaded ones"
SourceKey = "PK of the table in the source system (if applicable)"
FileFormat = "Format in which a data files are ingested"

# CELL ********************

from pyspark.sql.functions import sha2, concat_ws

# CELL ********************
# Load the Bronze-Landing data into Spark DataFrame 
df = spark.read.format(FileFormat) \
        .load(f'abfss://Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Files/{SourceSystem}/{SourceSchema}/{LoadMode}/{SourceTable}/keys/{SourceTable}.parquet')

df = df.withColumn('HashKey', sha2(concat_ws('|', 'Source', *[key.strip() for key in SourceKey.split(',')]), 256))

# Write into Bronze-Raw (only hashkey) to later used for soft deletions
df.select('HashKey').write.mode('overwrite') \
    .format('delta') \
    .option("overwriteSchema", 'true') \
    .save(f'abfss://Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Tables/{SourceSystem}_{SourceSchema}_{SourceTable}_keys')

# CELL ********************

mssparkutils.notebook.exit(f'Data has been loaded to {SourceSystem}_{SourceSchema}_{SourceTable}_keys table.')
