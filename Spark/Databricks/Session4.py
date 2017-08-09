# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com 

# See files in DBFS
dbutils.fs.ls('/') 
dbutils.fs.ls('/FileStore/tables')
dbutils.fs.ls('/FileStore/tables/1x1xr57q1502297004187/') 

# Also accesible as a table (if table declared during upload to CE)
%sql select * from kmeans_table 

# Applying KMeans (new ml lib - not mllib)
from pyspark.ml.clustering import KMeans

# Loading data: 
# This is the old way, load into RDD
# dataset = sc.textFile('/FileStore/tables/1x1xr57q1502297004187/kmeans_data.txt')

# Reading CSV to a df
kmeans_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter"," ").load("/FileStore/tables/1x1xr57q1502297004187/kmeans_data.txt")

# Check schema
kmeans_df.printSchema()

# Check data
display(kmeans_df) 

# Need to infer correctly the schema. Data are doubles, not string
kmeans_df = sqlContext.read.format("com.databricks.spark.csv") \
  .option("header", "false").option("delimiter"," ").option("inferschema", "true") \
  .load("/FileStore/tables/1x1xr57q1502297004187/kmeans_data.txt")
  
# Prepare data for training (see later the explanation about ML Pipelines)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

assembler = VectorAssembler(inputCols=["C0","C1","C2"], outputCol="features") 
assembler.transform(kmeans_df)

# Create the KMeans model
kmeans_estimator = KMeans().setFeaturesCol("features").setPredictionCol("prediction")
    
# Pipeline stages definition
pipeline = Pipeline(stages=[assembler, kmeans_estimator])

# Pipeline training
model = pipeline.fit(kmeans_df)

# Get the results: 
results = model.transform(kmeans_df)

# Check results:
display(results) 





  

  



