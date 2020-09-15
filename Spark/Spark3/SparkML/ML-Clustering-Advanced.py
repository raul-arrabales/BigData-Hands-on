# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com

# Clustering of restaurants and food related businesses in LA area

# Example adapted from: 
# Business Data Analysis by HiPIC of CalStateLA 
# http://web.calstatela.edu/centers/hipic/ 

# Data from Yelp available at: 
# https://raw.githubusercontent.com/hipic/biz_data_LA/master/Spark%20ML%20-%20Clustering/Business-Food.csv
# Also at: 
# https://raw.githubusercontent.com/hipic/biz_data_LA/master/Spark%20ML%20-%20Clustering/Business-Food.csv 

# Data is also stored in my DB Cloud Cluster at: 
# /FileStore/tables/bwddhyjz1502626538261/Business_Food-4345d.csv 
# Table foodbusinesses

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler

# File location and type
file_location = "/FileStore/tables/Business_Food.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# Create a view or table

temp_table_name = "Business_Food_csv"

df.createOrReplaceTempView(temp_table_name)

%sql

/* Query the created temp table in a SQL cell */

select * from `Business_Food_csv`

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "Business_Food_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)



# Adopt shcema to read csv data set in the schema. 
df.describe()
df.printSchema()

# If data types were wrong:
from pyspark.sql.types import DoubleType 
from pyspark.sql.types import IntegerType 
csv2 = csv.withColumn("review_count", csv["review_count"].cast(IntegerType()))
csv2 = csv2.withColumn("stars", csv2["stars"].cast(IntegerType()))

# Select the fields we're gonna use for clustering
data = csv2.select("review_count","Take-out", "GoodFor_lunch", "GoodFor_dinner", "GoodFor_breakfast","stars")
data.show(5)

# StringIndexer encodes a string column of labels to a column of label indices
def indexStringColumns(df, cols):
    #variable newdf will be updated several times
    newdata = df
    for c in cols:
        si = StringIndexer(inputCol=c, outputCol=c+"-x")
        sm = si.fit(newdata)
        newdata = sm.transform(newdata).drop(c)
        newdata = newdata.withColumnRenamed(c+"-x", c)
    return newdata

dfnumeric = indexStringColumns(data, ["Take-out","GoodFor_lunch", "GoodFor_dinner", "GoodFor_breakfast"])

# Check encoding
dfnumeric.show(5) 

# One-hot encoding maps a column of label indices to a column of binary vectors, with at most a single one-value.
def oneHotEncodeColumns(df, cols):
    from pyspark.ml.feature import OneHotEncoder
    newdf = df
    for c in cols:
        onehotenc = OneHotEncoder(inputCol=c, outputCol=c+"-onehot", dropLast=False)
        newdf = onehotenc.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-onehot", c)
    return newdf

dfhot = oneHotEncodeColumns(dfnumeric, ["Take-out","GoodFor_lunch", "GoodFor_dinner", "GoodFor_breakfast"])

dfhot.show(5)

# Taining set
assembler = VectorAssembler(inputCols = list(set(dfhot.columns) | set(['stars','review_count'])), outputCol="features")
train = assembler.transform(dfhot)

# Kmeans set for 5 clusters
knum = 5
kmeans = KMeans(featuresCol=assembler.getOutputCol(), predictionCol="cluster", k=knum, seed=0)
model = kmeans.fit(train)
print("Model Created!")

# See cluster centers:
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
    
# Apply the clustering model to our data:
prediction = model.transform(train)
prediction.groupBy("cluster").count().orderBy("cluster").show()

# Look at the features of each cluster
customerCluster = {}

for i in range(0,knum):
    tmp = prediction.select("stars","review_count","Take-out","GoodFor_lunch", "GoodFor_dinner", "GoodFor_breakfast").where("cluster =" +  str(i))
    customerCluster[str(i)] = tmp
    print("Cluster " + str(i))
    customerCluster[str(i)].show(3)

    
sqlContext.registerDataFrameAsTable(prediction, "PredTable")


%sql select avg(stars), avg(review_count) from PredTable where cluster == "1" 

