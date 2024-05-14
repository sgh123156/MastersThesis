# CELL ********************

from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col, row_number
from pyspark.sql.types import *
import json

from delta.tables import DeltaTable

# CELL ********************

class IncrementalLoader:
    def __init__(self, load_mode, schema=None, source_watermark=None, sdvalid = '', soft_deletes=False, df=None):

        self.envs = ['BronzeRaw', 'SilverOperational'] if Stage == "Silver Operational" else ['SilverOperational', 'SilverConformed']

        self.source_path = f'abfss://Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Tables/{SourceSystem}_{SourceSchema}_{SourceTable}'       
        self.target_path = f'abfss://Lakehouse@onelake.dfs.fabric.microsoft.com/Silver.Lakehouse/Tables/{SourceSystem}_{SourceSchema}_{SourceTable}'
        self.schema = schema
        self.load_mode = load_mode
        self.source_watermark = datetime.strptime(source_watermark.rstrip('Z'), '%Y-%m-%dT%H:%M:%S')
        self.soft_deletes = soft_deletes
        self.sdvalid = sdvalid
        self.target_watermark = datetime.now()
        self.df = df


        # Check if the target table exists
    def analyze(self):
            if not DeltaTable.isDeltaTable(spark, self.target_path):
                print('Delta table with provided parameters does not exist yet.')
                self.create_reload_target_table()
            else:
                self.load_target_table()

    def create_reload_target_table(self):
        # Load source data
        print(f'Initializing loading to {self.envs[1]} from {self.envs[0]}...')
        mergeschema = 'false'

        if not self.df:  # For Bronze Raw -> Silver Operational phase only
            self.df = spark.read.format("delta").load(self.source_path)
            mergeschema = 'true'

            # To be safe, apply deduplication, which spares the most recent record versions only
            windowfunc = Window.partitionBy('HashKey').orderBy(col("BronzeRawTimestamp").desc())

            self.df = self.df.withColumn('ordered_rows', row_number().over(windowfunc)) \
                .withColumn(f'{self.envs[1]}Timestamp', lit(self.target_watermark)) \
                .filter(col('ordered_rows') == 1) \
                .drop('ordered_rows')

            if self.soft_deletes:
                self.df = self.df.withColumn("IsDeleted", lit(False))
            
        # Create a new table if not exists or reload existing one
        self.df.write.format("delta").mode("overwrite").option("mergeSchema", mergeschema).save(self.target_path)
        self.target_dt = DeltaTable.forPath(spark, self.target_path)

        # Get statistics about a recent operation
        delta_history = self.target_dt.history(1).collect()[0]['operationMetrics']
        self.rows_inserted = delta_history['numOutputRows']
        self.rows_updated = 0

        print(f'Loading to {self.envs[1]} from {self.envs[0]} finished.')

    def load_target_table(self):
        # Determine strategy based on LoadMode
        print(f'Starting processing for LoadMode = {self.load_mode}...')
        if self.load_mode == 0: # Can be hit only for Silver Operational -> Silver Conformed
            self.create_reload_target_table() 
        elif self.load_mode == 1:
            self.load_incremental()

        if self.soft_deletes and self.sdvalid != '':
            self.apply_soft_deletes()

    def load_incremental(self):

        # Load source and target data
        self.target_dt = DeltaTable.forPath(spark, self.target_path)

        if not self.df:  # For Bronze Raw -> Silver Operational phase only
            # Again, to be safe, apply deduplication, which spares the most recent record versions
            source_df = spark.read.format("delta").load(self.source_path).where(f"{self.envs[0]}Timestamp >= '{self.source_watermark}'")
            windowfunc = Window.partitionBy('HashKey').orderBy(col(f"{self.envs[0]}Timestamp").desc())

            source_df = source_df.withColumn('ordered_rows', row_number().over(windowfunc)) \
                .filter(col('ordered_rows') == 1) \
                .drop('ordered_rows')
        else:
            self.schema = self.target_dt.toDF().schema
            source_df = self.df.where(f"{self.envs[0]}Timestamp >= '{self.source_watermark}'")

        print(f'Merging {self.envs[0]} to {self.envs[1]}...')

        self.target_dt.alias("t").merge(
            source_df.alias("s"),
            "s.HashKey = t.HashKey"
        ).whenMatchedUpdate(
            set = {
                **{field.name: f"s.{field.name}" for field in self.schema.fields if field.name not in ['BronzeLandingTimestamp']},  
                f'{self.envs[1]}Timestamp': lit(self.target_watermark)
            }
        ) \
        .whenNotMatchedInsert(
            values = {
                **{field.name: f"s.{field.name}" for field in self.schema.fields if field.name not in ['BronzeLandingTimestamp']},  
                f'{self.envs[1]}Timestamp': lit(self.target_watermark),
                'HashKey': 's.HashKey'
            }
        ) \
        .execute()

        delta_history = self.target_dt.history(1).collect()[0]['operationMetrics']
        self.rows_inserted = delta_history['numTargetRowsInserted']
        self.rows_updated = delta_history['numTargetRowsUpdated']

        print(f'Merging {self.envs[0]} to {self.envs[1]} completed. Inserted {self.rows_inserted} and updated {self.rows_updated} rows.')

    def apply_soft_deletes(self):
        # Implement soft delete logic comparing keys in the target table against a keys table

        keys_df = spark.read.format("delta").load(f'{self.source_path}_keys')

        missing_keys = self.target_dt.toDF().alias('t').join(keys_df.alias('k'), col('t.HashKey') == col('k.HashKey'), 'left_anti')

        print('Applying soft deletions...')
        self.target_dt.alias("t").merge(
        missing_keys.alias("m"),
            "t.HashKey = m.HashKey"
        ).whenMatchedUpdate(
            set = {
                'IsDeleted': lit(True),
                f'{self.envs[1]}Timestamp': lit(self.target_watermark)
            }
        ).execute()

        print(f"Soft deletions finished. Marked {self.target_dt.history(1).collect()[0]['operationMetrics']['numTargetRowsUpdated']} rows.")
