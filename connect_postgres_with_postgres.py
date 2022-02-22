from pyspark.sql import SparkSession

# the Spark session should be instantiated as follows
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.14.jar") \
    .getOrCreate()
    

jdbcDF = spark.read.format("jdbc").\
options(
         url='jdbc:postgresql://localhost:5432/testdb', # jdbc:postgresql://<host>:<port>/<database>
         dbtable='usertable',
         user='sahu',
         password='sahu',
         driver='org.postgresql.Driver').\
load()