from pyspark.sql import SparkSession
import re

# Initialize Spark session
spark = SparkSession.builder.appName("ProductViewCount").getOrCreate()

# Read the raw log file
rdd = spark.sparkContext.textFile("hdfs:///user/hadoop/logdata/access.log")  # or local path

# Define the regex pattern
pattern = re.compile(r'GET\s+/(m/)?product/(\d+)')

# Extract product IDs and map them to (product_id, 1)
product_views = (
    rdd
    .map(lambda line: pattern.search(line))  # apply regex
    .filter(lambda match: match is not None)  # filter out non-matching lines
    .map(lambda match: (match.group(2), 1))  # get product_id and assign 1
)

# Reduce by key to count total views per product
product_counts = product_views.reduceByKey(lambda a, b: a + b)

# Optional: sort by view count descending
sorted_counts = product_counts.sortBy(lambda x: -x[1])

# Save or show result
sorted_counts.saveAsTextFile("hdfs:///user/hadoop/bigdata/spark_product_views")  # Save to HDFS
# Or print to console:
for product_id, count in sorted_counts.collect():
    print(f"{product_id}\t{count}")
