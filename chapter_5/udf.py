import gc

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType


# Create cubed function
def cubed(s):
  return s * s * s


if __name__ == "__main__":
  spark: SparkSession = (SparkSession
                         .builder
                         .appName("WorkWithUDF")
                         .getOrCreate())
  try:
    # Register UDF
    spark.udf.register("cubed", cubed, LongType())

    # Generate temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    # Run SQL query
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    # Keep running until interrupted
    input("Press Ctrl+C to stop the Spark session...\n")

  except KeyboardInterrupt:
    print("Received Ctrl+C. Shutting down Spark...")

  finally:
    spark.stop()
    print("Spark session stopped gracefully.")
    gc.collect()
