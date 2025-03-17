from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark: SparkSession = SparkSession.builder.appName("Dataframe").getOrCreate()

# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "./data/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

few_fire_df = (fire_df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)

(fire_df.select("CallType").where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())

# In Python, filter for only distinct non-null CallTypes from all the rows
(fire_df.select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))

new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 .show(5, False))

fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
              .drop("CallDate")
              .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
              .drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
                                                        "MM/dd/yyyy hh:mm:ss a"))
              .drop("AvailableDtTm"))
# Select the converted columns
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show())

(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))

(fire_ts_df
 .select(sum("NumAlarms"), avg("ResponseDelayedinMins"),
         min("ResponseDelayedinMins"), max("ResponseDelayedinMins"))
 .show())

# TODO
# • What were all the different types of fire calls in 2018?
# • What months within the year 2018 saw the highest number of fire calls?
# • Which neighborhood in San Francisco generated the most fire calls in 2018?
# • Which neighborhoods had the worst response times to fire calls in 2018?
# • Which week in the year in 2018 had the most fire calls?
# • Is there a correlation between neighborhood, zip code, and number of fire calls?
# • How can we use Parquet files or SQL tables to store this data and read it back?

# Dataframewriter
# parquet_path = "./export/sf-fire-calls-parquet"
# fire_df.write.format("parquet").save(parquet_path)

# # registers metadata with the Hive metastore
# parquet_table = "fireServiceCalls"
# fire_df.write.format("parquet").saveAsTable(parquet_table)
