from pyspark.sql import SparkSession


def print_configs(session: SparkSession):
  print("++++++++++++ START +++++++++++\n")
  # Get all Spark configs as a dictionary
  conf_items = session.sparkContext.getConf().getAll()
  for k, v in conf_items:
    print(f"{k} -> {v}\n")

  print("++++++++++++ END +++++++++++\n")


def main():
  # Create a Spark session with initial configs
  spark = (SparkSession.builder
           .config("spark.sql.shuffle.partitions", 10)
           .config("spark.executor.memory", "2g")
           .master("local[*]")
           .appName("TestSparkConfig")
           .getOrCreate())

  print_configs(spark)

  # Change shuffle partitions to default parallelism
  spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
  print(" ****** Setting Shuffle Partitions to Default Parallelism")

  print_configs(spark)

  # Stop the Spark session (optional)
  spark.stop()


if __name__ == "__main__":
  main()
