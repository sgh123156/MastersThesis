# CELL ********************

from datetime import datetime
from pyspark.sql.functions import lit, col, row_number, sha2, concat_ws
import json

from delta.tables import DeltaTable

# CELL ********************

class ModelLoader:
    def __init__(self, df, load_mode, scd2columns=None, source_watermark=None):

        self.table_name = f'gold_{GoldSchema}_{GoldTable}'
        self.scd2columns = scd2columns
        self.load_mode = load_mode
        self.source_watermark = datetime.strptime(source_watermark.rstrip('Z'), '%Y-%m-%dT%H:%M:%S')
        self.target_watermark = datetime.now()
        self.df = df

    def analyze(self):
        if self.load_mode == 1:
            self.load_type_1()
        elif self.load_mode ==2:
            self.load_type_2()

    def load_type_1(self):

        # Load Gold table
        self.gold_dt = DeltaTable.forName(spark, self.table_name)

        # Get the schema of Gold table
        self.schema = self.gold_dt.toDF().schema 

        # Select only records from the latest Silver run
        source_df = self.df.where(f"SilverConformedTimestamp >= '{self.source_watermark}'") 

        print(f'Merging Silver to Gold...')

        # Update if a matching record already exists, insert if it does not and generate HashBusinessKeys in runtime
        self.gold_dt.alias("t").merge(
            source_df.alias("s"),
            "s.HashKey = t.HashKey AND t.IsActive = True"
        ).whenMatchedUpdate(
            set = {
                **{field.name: f"s.{field.name}" for field in self.schema.fields 
                    if field.name not in ['ActiveFrom', 'GoldTimestamp', 'Test', 'HashKey', 'ActiveTo']
                        and f'HBK_{GoldTable}' not in field.name},  
                'GoldTimestamp': lit(self.target_watermark),
            } 
        ) \
        .whenNotMatchedInsert(
            values = {
                **{field.name: f"s.{field.name}" for field in self.schema.fields 
                    if field.name not in ['ActiveFrom', 'GoldTimestamp', 'Test', 'ActiveTo']},  
                'GoldTimestamp': lit(self.target_watermark),
                'ActiveFrom': lit(self.target_watermark),
                f'HBK_{GoldTable}': sha2(concat_ws('|', 's.HashKey', lit(self.target_watermark)), 256)
            } 
        ) \
        .execute()

        # Get statistics about the MERGE execution from Delta log
        delta_history = self.gold_dt.history(1).collect()[0]['operationMetrics']
        self.rows_inserted = delta_history['numTargetRowsInserted']
        self.rows_updated = delta_history['numTargetRowsUpdated']

        print(f'Merging Silver to Gold completed. Inserted {self.rows_inserted} and updated {self.rows_updated} row(s).')

        # Lastly, perform marking records as inactive if they are not seen in Silver
        self.apply_soft_deletes()

    def load_type_2(self):

        # Load Gold table
        self.gold_dt = DeltaTable.forName(spark, self.table_name)

        # Get the schema of Gold table
        self.schema = self.gold_dt.toDF().schema 

        # Select only records from the latest Silver run
        source_df = self.df.where(f"SilverConformedTimestamp >= '{self.source_watermark}'") 

        # Build dynamic join condition based on SCD2 columns declared in GoldEntity table
        conditions = ' OR '.join([
            f"COALESCE(s.{col}, '') <> COALESCE(t.{col}, '')" 
            for col in (col.strip() for col in self.scd2columns.split(', '))
        ])

        # Build a temporary DF to store records impacted by changes in scope of SCD2 handling
        impacted_by_scd2 = source_df.alias('s') \
            .join(self.gold_dt.toDF().alias('t'), 'HashKey') \
            .where(f't.IsActive = True AND ({conditions})') \
            .withColumn('MergeKey', lit(None)) \
            .select(col('MergeKey'), 's.*')  # Ensure all columns from source_df are included

        # Append the previous DF by all records from the latest Silver run
        full_scope_scd2 = impacted_by_scd2.union(
            source_df \
                .withColumn("MergeKey", col("HashKey")) \
                .select(['MergeKey'] + [col for col in source_df.columns])  # Explicitly select columns from source_df
        )
        # Inactivate obsolete records (SCD2), update if a matching record already exists (SCD1, SCD2), insert if it does not and generate HashBusinessKeys in runtime
        self.gold_dt.alias("t").merge(
            full_scope_scd2.alias("s"),
            "t.HashKey = s.mergeKey") \
        .whenMatchedUpdate(
            condition = f"t.IsActive = True AND ({conditions})",
            set = {
                'IsActive': lit(False),
                'ActiveTo': lit(self.target_watermark),
                'GoldTimestamp': lit(self.target_watermark)
            }
        ).whenMatchedUpdate(
            condition = f"t.IsActive = True AND !({conditions})",
            set = {
                **{field.name: f"s.{field.name}" for field in self.schema.fields 
                    if field.name not in ['ActiveFrom', 'GoldTimestamp', 'Test', 'HashKey', 'ActiveTo']
                        and f'HBK_{GoldTable}' not in field.name},  
                'GoldTimestamp': lit(self.target_watermark),
            } 
        ).whenNotMatchedInsert(
            values = {
                **{field.name: f"s.{field.name}" for field in self.schema.fields 
                    if field.name not in ['ActiveFrom', 'GoldTimestamp', 'Test', 'ActiveTo']},  
                'GoldTimestamp': lit(self.target_watermark),
                'ActiveFrom': lit(self.target_watermark),
                'ActiveTo': lit("2099-12-31").cast("timestamp"),
                f'HBK_{GoldTable}': sha2(concat_ws('|', 's.HashKey', lit(self.target_watermark)), 256)
            } 
        ).execute()

        # Get statistics about the MERGE execution from Delta log
        delta_history = self.gold_dt.history(1).collect()[0]['operationMetrics']
        self.rows_inserted = delta_history['numTargetRowsInserted']
        self.rows_updated = delta_history['numTargetRowsUpdated']

        print(f'Merging Silver to Gold completed (SCD2). Inserted {self.rows_inserted} and updated {self.rows_updated} row(s).')

        # Lastly, perform marking records as inactive if they are not seen in Silver
        self.apply_soft_deletes()

    def apply_soft_deletes(self):

        print('Starting soft deletions...')

        # Mark records not seen in Silver as inactive in Gold
        self.gold_dt.alias("t").merge(
            self.df.alias("s"),
            "s.HashKey = t.HashKey"
        ).whenNotMatchedBySourceUpdate(
            set = {"t.IsActive": lit(False)}
        ).execute()

        print(f"Soft deletions finished. Marked {self.gold_dt.history(1).collect()[0]['operationMetrics']['numTargetRowsUpdated']} row(s).")
