import gc

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType


# Declare the cubed function
def cubed(a: pd.Series) -> pd.Series:
  return a * a * a


if __name__ == "__main__":
  spark: SparkSession = (SparkSession
                         .builder
                         .appName("WorkWithPandas")
                         .getOrCreate())
  try:
    # Create the pandas UDF for the cubed function
    cubed_udf = pandas_udf(cubed, returnType=LongType())

    # Create a Pandas Series
    x = pd.Series([1, 2, 3])
    # The function for a pandas_udf executed with local Pandas data
    print(cubed(x))

    # Create a Spark DataFrame, 'spark' is an existing SparkSession
    df = spark.range(1, 4)
    # Execute function as a Spark vectorized UDF
    df.select("id", cubed_udf(col("id"))).show()

    # Keep running until interrupted
    input("Press Ctrl+C to stop the Spark session...\n")

  except KeyboardInterrupt:
    print("Received Ctrl+C. Shutting down Spark...")

  finally:
    spark.stop()
    print("Spark session stopped gracefully.")
    gc.collect()
