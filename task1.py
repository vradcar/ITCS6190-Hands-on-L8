from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Task1_StreamingIngestion") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce console output
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

# Print parsed data to the console (for testing)
# Uncomment this to see console output:
# console_query = parsed_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# Write parsed data to CSV files
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/task_1") \
    .option("checkpointLocation", "checkpoints/task_1") \
    .option("header", "true") \
    .start()

print("Task 1: Streaming ingestion started. Writing to outputs/task_1/")
print("Press Ctrl+C to stop...")

query.awaitTermination()
