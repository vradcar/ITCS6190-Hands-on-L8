
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Task3_WindowedAnalytics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
parsed_stream = parsed_stream.withColumn(
    "event_time", to_timestamp("timestamp")
).withWatermark("event_time", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_df = parsed_stream.groupBy(
    window("event_time", "5 minutes", "1 minute")
).agg(
    _sum("fare_amount").alias("total_fare")
)

# Extract window start and end times as separate columns
result_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Write the windowed results to CSV files in real time
query = result_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/task_3") \
    .option("checkpointLocation", "outputs/task_3/_checkpoint/") \
    .option("header", "true") \
    .start()

print("Task 3: Windowed analytics started. Writing to outputs/task_3/")
print("Press Ctrl+C to stop...")

query.awaitTermination()
