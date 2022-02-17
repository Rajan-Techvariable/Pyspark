import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.sql.functions import dayofweek, year, month

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Reading data from Crawlers as Dynamic Dataframe
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "test_ v", table_name = "input", transformation_ctx = "datasource0")
# print(datasource0.count())

## Convert dynamic dataframe into pyspark dataframe
df = datasource0.toDF()

## Checking
# df.printSchema()
# print(type(df))

# from date field we are extracting "day_of_week", "month" and year
df1=df.withColumn('day_of_week',dayofweek(df.date)).withColumn('month', month(df.date)).withColumn("year", year(df.date))

# Droping date coloum
df2=df1.drop('date')


 
# Remove the target column from the input feature set.
featuresCols = df2.columns
featuresCols.remove('sales')
 
# vectorAssembler combines all feature columns into a single feature vector column, "rawFeatures".
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")

v = vectorAssembler.transform(df2)

df3 = v.select("rawFeatures","sales")

df3=df3.withColumnRenamed("sales", "label")




data =df3
# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a GBT model.
gbt = GBTRegressor(featuresCol="features", maxIter=15)

# Chain indexer and GBT in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, gbt])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

gbtModel = model.stages[1]
print(gbtModel)



job.commit()