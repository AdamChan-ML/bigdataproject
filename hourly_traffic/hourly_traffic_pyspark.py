from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count

# Create Spark session
spark = SparkSession.builder \
    .appName("HourlyTrafficAnalysis") \
    .getOrCreate()

# Path to your web server log file
log_file = "hdfs:///user/hadoop/access.log"

# Read the log file as a DataFrame
logs_df = spark.read.text(log_file)

# Regex pattern to extract the hour from timestamps like [10/Oct/2000:13:55:36 -0700]
hour_pattern = r"\[(\d{2}\/[A-Za-z]{3}\/\d{4}):(\d{2}):\d{2}:\d{2}"

# Extract the hour as a new column
logs_df = logs_df.withColumn("hour", regexp_extract(col("value"), hour_pattern, 2))

# Group by hour and count the number of requests
hourly_counts = logs_df.groupBy("hour").agg(count("*").alias("total_requests"))

# Sort by hour
hourly_counts = hourly_counts.orderBy("hour")

# Show the results
hourly_counts.show(24, truncate=False)

# Stop Spark session
spark.stop()
