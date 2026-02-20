# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ed55df3f-b99a-4de5-bc0a-8bd843a8d91a",
# META       "default_lakehouse_name": "LH_Retail_Silver",
# META       "default_lakehouse_workspace_id": "0ef96a5d-6189-4a8b-a938-4fc18e107cf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "ed55df3f-b99a-4de5-bc0a-8bd843a8d91a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import sum, weekofyear

df = spark.table("retail_clean")

weekly = df.withColumn("Week", weekofyear("event_time")) \
           .groupBy("Product", "Store", "Week") \
           .agg(sum("Qty").alias("Weekly_Sales"))

weekly.write.mode("overwrite").saveAsTable("weekly_sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

weekly.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
