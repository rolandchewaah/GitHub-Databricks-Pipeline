from pyspark.sql.functions import current_timestamp

# 1. Define where the data is coming from (Azure Data Lake Storage)
source_path = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/2010-summary.csv"
target_table = "main.default.flights_ingested"

# 2. Read the raw CSV data
df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(source_path))

# 3. Add a transformation (metadata column for lineage)
df_transformed = df.withColumn("ingestion_timestamp", current_timestamp())

# 4. Write to a Delta table
(df_transformed.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(target_table))