import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# JDBC connection 
jdbcDF = spark.read.format("jdbc").\
options(
         url=f'jdbc:oracle:thin:{user}/{password}@//{endpoint}:{port}/ORACLEDB',
         dbtable=f'{db}',# database table
         user=f'{user}', #username
         password=f'{password}', #password
         driver='oracle.jdbc.OracleDriver').\
load()

# count the number of row dataframe have
jdbcDF.count()

job.commit()