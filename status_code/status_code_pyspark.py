from pyspark.sql import SparkSession
import re

# Create Spark session
spark = SparkSession.builder \
    .appName("RequestsByStatusCode") \
    .getOrCreate()

# Read log file (update the path as needed)
log_file = "hdfs:///user/hadoop/access.log"
lines = spark.sparkContext.textFile(log_file)

# Regex to match status code
status_code_pattern = re.compile(r'"\w+\s[^\s]+\sHTTP/\d\.\d"\s(\d{3})')

# Extract and count status codes
status_codes = lines.map(lambda line: status_code_pattern.search(line)) \
                    .filter(lambda match: match is not None) \
                    .map(lambda match: (match.group(1), 1)) \
                    .reduceByKey(lambda a, b: a + b)

# Sort by status code numerically
sorted_status_codes = status_codes.sortBy(lambda x: int(x[0]))

# Collect and display results
for code, count in sorted_status_codes.collect():
    print(f"{code}\t{count}")

spark.stop()
