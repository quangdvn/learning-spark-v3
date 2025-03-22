import gc

from pyspark.sql import SparkSession

# Create a SparkSession ưith Hive enabled
spark: SparkSession = (SparkSession
                       .builder
                       .appName("SparkParquetExampleApp_02")
                       .getOrCreate())

file = "./chapter_4/data/2010-summary.parquet/"
df = spark.read.format("parquet").load(file)
df.show()

(df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("./chapter_4/tmp_data/parquet/df_parquet"))

# -- In SQL
# CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
#  USING parquet
#  OPTIONS (
#  path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
#  2010-summary.parquet/" )

# # In Python
# (df.write
#  .mode("overwrite")
#  .saveAsTable("us_delay_flights_tbl"))


if 'spark' in locals():
  spark.stop()
  del spark  # Remove the variable reference
  gc.collect()  # Trigger garbage collection

print("Spark session deleted successfully.")
