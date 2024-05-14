# CELL ********************

GoldSchema = "Name of the schema of the processed Gold table"
GoldTable = "Name of the processed Gold table"
LoadMode = "Type of loading the data; 1 - incremental (SCD1), 0 - full, 2 - incremental with SCD2"
SCD2Columns = "Names of columns in LoadMode2 to be considered for SCD2"
Watermark = "Timestamp of the recent load to Silver-Confomed"

# CELL ********************

import json

# CELL ********************
# Build DAG to later use in mssparkutils.notebook.runMultiple, which allows for passing parameters to the triggered notebooks: first DDL and then DML

DAG = {
        "activities": [
            {
                "name": f"DDL_{GoldSchema}_{GoldTable}", 
                "path": f"DDL_{GoldSchema}_{GoldTable}", 
                "timeoutPerCellInSeconds": 3600, 
            },
            {
                "name": f"DML_{GoldSchema}_{GoldTable}",
                "path": f"DML_{GoldSchema}_{GoldTable}",
                "timeoutPerCellInSeconds": 3600,   
                "args": {"LoadMode": LoadMode, "Watermark": Watermark, "GoldSchema": GoldSchema, "GoldTable": GoldTable, "SCD2Columns":SCD2Columns},
                "dependencies": [f"DDL_{GoldSchema}_{GoldTable}"] 
            }
        ]
    }
combined_exec = mssparkutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})
returnmsg = json.loads(combined_exec[f"DML_{GoldSchema}_{GoldTable}"]['exitVal'])

# CELL ********************

mssparkutils.notebook.exit(returnmsg)
