from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
print("Spark version:", sc.version)
print("Hadoop version:", sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion())