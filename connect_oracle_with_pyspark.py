from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "ojdbc7.jar") \
    .getOrCreate()


jdbcDF = spark.read.format("jdbc").\
options(
         url='jdbc:oracle:thin:admin/oracledbtest@//database-ahhesfe.amazonaws.com:1521/ORACLEDB',
         dbtable='employee',
         user='master',
         password='12345678',
         driver='oracle.jdbc.OracleDriver').\
load()
