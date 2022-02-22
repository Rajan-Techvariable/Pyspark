# import the required packages
from pyspark.sql import SparkSession

# the Spark session should be instantiated as follows
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

df=spark.read.format("csv").option("header","true").load("pokemon.csv")