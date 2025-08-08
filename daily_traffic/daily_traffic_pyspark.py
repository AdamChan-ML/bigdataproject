from pyspark.sql import SparkSession
import re

# Create Spark session
spark = SparkSession.builder \
    .appName("Daily Traffic Analysis") \
    .getOrCreate()

# Path to log file (change this to your HDFS/S3/local path)
log_file = "hdfs:///user/hadoop/access.log"

# Read log file as RDD
logs_rdd = spark.sparkContext.textFile(log_file)

# Regex to extract date (DD/Mon/YYYY)
date_pattern = re.compile(r'\[(\d{2}/[A-Za-z]+/\d{4}):')

# Extract dates and map to (date, 1)
dates_rdd = logs_rdd \
    .map(lambda line: date_pattern.search(line)) \
    .filter(lambda match: match is not None) \
    .map(lambda match: (match.group(1), 1))

# Reduce by date to count total requests per day
daily_counts = dates_rdd.reduceByKey(lambda a, b: a + b)

# Sort by date (optional)
sorted_counts = daily_counts.sortByKey()

# Collect and print results
for date, count in sorted_counts.collect():
    print(f"{date}\t{count}")

spark.stop()
