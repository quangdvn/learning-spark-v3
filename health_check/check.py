from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
print(spark)
if spark:
  print("A Spark session is active.")
else:
  print("No active Spark session found.")
