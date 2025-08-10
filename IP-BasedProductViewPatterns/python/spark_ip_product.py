from pyspark import SparkContext
import re

sc = SparkContext(appName="ProductIPCountSorted")

blacklist_ips = {
    "66.249.66.194",
    "91.99.72.15",
    "5.160.157.20"
}

pattern = re.compile(r'^(\d+\.\d+\.\d+\.\d+).+GET\s+/(m/)?product/(\d+)')

log_rdd = sc.textFile("hdfs:///user/hadoop/logdata/access.log")

result = (
    log_rdd
    .map(lambda line: pattern.search(line))
    .filter(lambda m: m is not None)
    .map(lambda m: (m.group(3), m.group(1)))
    .filter(lambda pair: pair[1] not in blacklist_ips)
    .map(lambda pair: ((pair[0], pair[1]), 1))
    .reduceByKey(lambda a, b: a + b)
    .map(lambda x: (x[1], x[0]))
    .sortByKey(ascending=False)
    .map(lambda x: (x[1], x[0]))
)

result.saveAsTextFile("hdfs:///user/hadoop/bigdata/spark_ip_product")
