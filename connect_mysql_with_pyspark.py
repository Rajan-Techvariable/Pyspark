from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.jars", "mysql-connector-java-8.0.22.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()


df = spark.read.format("jdbc").\
    option("url","jdbc:mysql://naacdrssf-1.rds.amazonaws.com/test").\
    option("driver","com.mysql.jdbc.Driver").option("dbtable","employee").option("user","master").\
    option("password","1234567890").load()