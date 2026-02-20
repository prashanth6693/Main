# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fd6dfac9-3786-4827-9b43-812872ba642f",
# META       "default_lakehouse_name": "LH_Retail_Bronze",
# META       "default_lakehouse_workspace_id": "0ef96a5d-6189-4a8b-a938-4fc18e107cf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "fd6dfac9-3786-4827-9b43-812872ba642f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime
import random
import pandas as pd

products = ["Hoodie", "Jacket", "TShirt", "Jeans"]
stores = ["BLR01", "MUM01", "DEL01"]
channels = ["POS", "Online"]

TABLE_NAME = "retail_events"

def generate_row():
    qty = random.randint(-5, 30)
    inventory = random.randint(-3, 120)
    txn_status = "FAULT" if qty < 0 or inventory < 0 else "OK"

    return {
        "event_time": datetime.now(),
        "raw_text": f"Store={random.choice(stores)};Prod={random.choice(products)};Qty={qty};Inv={inventory}",
        "channel": random.choice(channels),
        "region": "India",
        "temp_celsius": random.randint(10, 40),
        "txn_status": txn_status
    }

# IMPORTANT: run this ONCE if table exists from previous tests
# spark.sql("DROP TABLE IF EXISTS retail_events")

table_exists = spark.catalog.tableExists(TABLE_NAME)

if not table_exists:
    print("Table does NOT exist → Creating initial 10,000 rows")
    row_count = 10000
    write_mode = "overwrite"
else:
    print("Table exists → Appending 10 rows")
    row_count = 10
    write_mode = "append"

rows = [generate_row() for _ in range(row_count)]
df = pd.DataFrame(rows)

spark.createDataFrame(df) \
     .write \
     .mode(write_mode) \
     .saveAsTable(TABLE_NAME)

print(f"{row_count} rows written ({write_mode})")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("retail_events").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
