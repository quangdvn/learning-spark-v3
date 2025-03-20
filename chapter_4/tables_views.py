import gc

from pyspark.sql import SparkSession

# Create a SparkSession ưith Hive enabled
spark: SparkSession = (SparkSession
                       .builder
                       .appName("SparkSQLExampleApp_02")
                       .config("spark.sql.warehouse.dir", "./spark-warehouse")  # Set warehouse location
                       .config("spark.sql.catalogImplementation", "hive")  # Enable Hive
                       .enableHiveSupport()  # Ensure Hive metastore is used
                       .getOrCreate())

# Create and use database
spark.sql("CREATE DATABASE IF NOT EXISTS learn_spark_db")
spark.sql("USE learn_spark_db")


# Create managed and unmanaged tables
# csv_file = "./chapter_4/data/departuredelays.csv"
# # Schema as defined in the preceding example
# schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
# flights_df = spark.read.csv(csv_file, schema=schema)
# flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

# (flights_df
#  .write
#  .option("path", "./tmp_data/us_flights_delay")
#  .saveAsTable("us_delay_flights_tbl"))

# spark.sql("DESCRIBE EXTENDED us_delay_flights_tbl").show(truncate=False)

# Create a temporary and global temporary view
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin='JFK'")
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin='SFO'")

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

spark.read.table("us_origin_airport_JFK_tmp_view").show()
spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view").show()

spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

# Convert to Dataframe
us_flights_df2 = spark.table("us_delay_flights_tbl")
us_flights_df2.select("distance", "origin", "destination") \
    .where("distance > 1000") \
    .orderBy("distance", ascending=False) \
    .show(10)

# Garbage collection
print("Databases:", spark.catalog.listDatabases())
print("Tables:", spark.catalog.listTables())
print("Columns in 'us_delay_flights_tbl':", spark.catalog.listColumns("us_delay_flights_tbl"))

spark.sql("SHOW DATABASES").show()
spark.sql("SELECT current_database()").show()

if 'spark' in locals():
  spark.stop()
  del spark  # Remove the variable reference
  gc.collect()  # Trigger garbage collection

print("Spark session deleted successfully.")
