from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Task2_RealTimeAggregations") \
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

# Compute aggregations: total fare and average distance grouped by driver_id
agg_df = parsed_stream.groupBy("driver_id").agg(
    _sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Write the aggregation results to CSV files in real time
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("csv") \
    .option("path", "outputs/task_2") \
    .option("checkpointLocation", "outputs/task_2/_checkpoint/") \
    .option("header", "true") \
    .start()

print("Task 2: Real-time aggregations started. Writing to outputs/task_2/")
print("Press Ctrl+C to stop...")

query.awaitTermination()
