from pyspark.sql import SparkSession

# Create a SparkSession
spark: SparkSession = (SparkSession
                       .builder
                       .appName("SparkSQLExampleApp")
                       .getOrCreate())

# Path to data set
csv_file = "./chapter_4/data/departuredelays.csv"

schema = "`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING"


# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
      .schema(schema)
      # .option("inferSchema", "true")
      .option("header", "true")
      .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

# Create a temporary view (view is the same as a table). It's a logical
# view of the underlying data
spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)

spark.sql("""
          SELECT DATE_FORMAT(TO_DATE(date, 'MMddHHmm'), 'yyyy-MM-dd') AS formatted_date,
          delay,
          origin,
          destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)

# spark.sql("""
#     SELECT YEAR(TO_DATE(date, 'MMddHHmm')) AS year,
# 						MONTH(TO_DATE(date, 'MMddHHmm')) AS month,
# 						DAY(TO_DATE(date, 'MMddHHmm')) AS day,
# 						COUNT(*) AS total_delay
#     FROM us_delay_flights_tbl
#     GROUP BY year, month, day
#     ORDER BY year, month, day
# """).show(100)

spark.sql("""SELECT delay, origin, destination,
	CASE
	WHEN delay > 360 THEN 'Very Long Delays'
	WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
	WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
	WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
	WHEN delay = 0 THEN 'No Delays'
	ELSE 'Early'
	END AS Flight_Delays
	FROM us_delay_flights_tbl
	ORDER BY origin, delay DESC""").show(10)
