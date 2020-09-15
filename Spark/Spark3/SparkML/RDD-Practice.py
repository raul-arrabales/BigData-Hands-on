# PySpark Hands-On Training
# Databricks CE Cloud Practice
# Raul Arrabales

# Listing files (already uploaded to the distributed file system
dbutils.fs.ls("FileStore/tables/")

# Step by step (explain each step)

# **************************************
# ************* STEP 1 *****************
# **************************************

# rdd1 = sc.textFile("/FileStore/tables/quijote.txt")
# rdd1.take(15)

# Reading directly from GitHub: 

url = "https://raw.githubusercontent.com/raul-arrabales/BigData-Hands-on/master/Spark/Spark3/data/quijote_complete.txt"
from pyspark import SparkFiles
spark.sparkContext.addFile(url)

rdd1 = sc.textFile("file://"+SparkFiles.get("quijote_complete.txt"))

rdd1.take(15)

# **************************************
# ************* STEP 2 *****************
# **************************************


rdd2 = rdd1.map( lambda x: x.replace('.',' ').replace('-',' ').lower())
rdd2.take(10)

# **************************************
# ************* STEP 3 *****************
# **************************************

rdd3 = rdd2.flatMap(lambda x: x.split())
rdd3.count()

# **************************************
# ************* STEP 4 *****************
# **************************************

rdd4 = rdd3.map(lambda x: (x, 1))
rdd4.take(15)

# **************************************
# ************* STEP 5 *****************
# **************************************

rdd5 = rdd4.reduceByKey(lambda x,y:x+y)
rdd5.count()

# **************************************
# ************* STEP 6 *****************
# **************************************

rdd6 = rdd5.map(lambda x:(x[1],x[0]))
rdd6.take(10)

# **************************************
# ************* STEP 7 *****************
# **************************************

rdd7 = rdd6.sortByKey(False)
rdd7.take(100)
display(rdd7.take(4)) 


# **************************************
# ************* NESTED *****************
# **************************************

# Nested wordcount solution:

# Load text lines into an RDD
rdd1 = sc.textFile("[TXT URI") 

wordcounts = rdd1.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False)
        
# Check Results
wordcounts.take(5)
