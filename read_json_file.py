from pyspark.sql import SparkSession

# the Spark session should be instantiated as follows
spark = SparkSession \
    .builder \
    .appName("working with JSON") \
    .getOrCreate()

df_json = spark.read.option("multiline","true") \
      .json("iris.json")