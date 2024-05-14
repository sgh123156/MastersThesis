# PARAMETERS CELL ********************

SourceSystem = "Name of the source system"
SourceSchema = "Name of the schema of a table in the source (if applicable)"
SourceTable = "Name of the object in the source"
LoadMode = "0 for full-loaded tables, 1 for incrementally-loaded ones"
BronzeLayer = "GUID of the Fabric item - Bronze Lakehouse"
SilverLayer = "GUID of the Fabric item - Silver Lakehouse"
Workspace = "GUID of the Fabric workspace where Fabric items are deployed"
Schema = "Spark DataFrame schema converted to JSON to enable passing it as a parameter"
ValidForSoftDeletes = "Indicator if a processed table is valid for soft deletes in a current run"
Watermark = "Timestamp of the last processing"
Stage = "Stage of the processing, either Silver Operational or Silver Conformed"

# CELL ********************

%run nb_ShortcutHandler

# CELL ********************

%run nb_IncrementalLoader

# CELL ********************

if Stage == 'Silver Operational':
    if LoadMode == 0:
        shortcut_name_source = f'{SourceSystem}_{SourceSchema}_{SourceTable}'
        shortcut_name_target = f'{SourceSystem}_{SourceSchema}_{SourceTable}'

        sh = ShorcutHandler(BronzeLayer, SilverLayer, Workspace, shortcut_name_target, shortcut_name_source, 'Tables', 'Tables' )
        sh.analyze()
        returnmsg = json.dumps({'RowsInserted': 0, 'RowsUpdated': 0, 'Watermark': sh.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})

    elif LoadMode ==1:
        parsed_schema = StructType.fromJson(json.loads(Schema))
        il = IncrementalLoader(LoadMode, parsed_schema, Watermark, ValidForSoftDeletes, SoftDeletesEnabled)
        il.analyze()
        returnmsg = json.dumps({'RowsInserted': il.rows_inserted, 'RowsUpdated': il.rows_updated, 'Watermark': il.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
elif Stage == 'Silver Conformed':
    
    # Build DAG to later use in mssparkutils.notebook.runMultiple, which allows for passing parameters to the triggered notebooks: first DDL and then DML
    DAG = {
        "activities": [
            {
                "name": f"DDL_{SourceSchema}_{SourceTable}", 
                "path": f"DDL_{SourceSchema}_{SourceTable}", 
                "timeoutPerCellInSeconds": 3600, 
            },
            {
                "name": f"DML_{SourceSchema}_{SourceTable}",
                "path": f"DML_{SourceSchema}_{SourceTable}",
                "timeoutPerCellInSeconds": 3600,   
                "args": {"LoadMode": LoadMode, "Watermark": Watermark, "SourceSystem": SourceSystem, "SourceSchema":SourceSchema, "SourceTable":SourceTable, "Stage":Stage},
                "dependencies": [f"DDL_{SourceSchema}_{SourceTable}"] 
            }
        ]
    }
    combined_exec = mssparkutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})
    returnmsg = json.loads(combined_exec[f"DML_{SourceSchema}_{SourceTable}"]['exitVal'])

# CELL ********************

mssparkutils.notebook.exit(returnmsg)
