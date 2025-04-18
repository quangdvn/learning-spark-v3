import gc

from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank, expr
from pyspark.sql.window import Window

spark: SparkSession = (SparkSession
                       .builder
                       .appName("SparkDataframeOperations")
                       .getOrCreate())

tripdelaysFilePath = "./chapter_5/data/departuredelays.csv"
airportsnaFilePath = "./chapter_5/data/airport-codes-na.txt"

# Obtain airports data set
airportsna = (spark.read
              .format("csv")
              .options(header="true", inferSchema="true", sep="\t")
              .load(airportsnaFilePath))
airportsna.createOrReplaceTempView("airports_na")

# Obtain departure delays data set
departureDelays = (spark.read
                   .format("csv")
                   .options(header="true")
                   .load(tripdelaysFilePath))
departureDelays = (departureDelays
                   .withColumn("delay", expr("CAST(delay as INT) as delay"))
                   .withColumn("distance", expr("CAST(distance as INT) as distance")))
departureDelays.createOrReplaceTempView("departureDelays")

# Create temporary small table for deep analysis
foo = (departureDelays
       .filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date like '0101%' AND delay > 0""")))
foo.createOrReplaceTempView("foo")

# Select * SQL
# spark.sql("SELECT * FROM airports_na LIMIT 10").show()
# spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
# spark.sql("SELECT * FROM foo LIMIT 10").show()

# UNION
# bar = departureDelays.union(foo)
# bar.show()
# bar.createOrReplaceTempView("bar")
# bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'""")).show()

# JOIN
# inner(default), cross, outer, full, full_outer, left, left_outer,
# right, right_outer, left_semi, left_anti
# foo.join(
#   airportsna,
#   airportsna.IATA == foo.origin
# ).select("City", "State", "date", "delay", "distance").show()

# spark.sql("""
# SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
#  FROM foo f
#  JOIN airports_na a
#  ON a.IATA = f.origin
# """).show()

# WINDOW FUNCTION
departureDelaysWindows = spark.sql("""
SELECT origin, destination, SUM(delay) AS TotalDelays
	FROM departureDelays
	WHERE origin IN ('SEA', 'SFO', 'JFK')
	AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
	GROUP BY origin, destination
	ORDER BY origin
""")
departureDelaysWindows.createOrReplaceTempView("departureDelaysWindows")

# spark.sql("""
# 				SELECT origin, destination, TotalDelays, rank
#  FROM (
#  SELECT origin, destination, TotalDelays, dense_rank()
#  OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
#  FROM departureDelaysWindow
#  ) t
#  WHERE rank <= 3
#           """).show()

(departureDelaysWindows.selectExpr(
    "origin",
    "destination",
    "TotalDelays",
    "dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank")
    .filter("rank <= 3")
    .show())

windowSpec = Window.partitionBy("origin").orderBy(expr("TotalDelays DESC"))
departureDelaysWindows.withColumn("rank", dense_rank().over(windowSpec)).filter("rank <= 3").show()

# MODIFICATIONS


if 'spark' in locals():
  spark.stop()
  del spark  # Remove the variable reference
  gc.collect()  # Trigger garbage collection

print('Spark session deleted successfully.')
