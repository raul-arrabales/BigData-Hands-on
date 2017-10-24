# Create the Spark Context
import pyspark
sc = pyspark.SparkContext('local[*]')

# Check Spark Version
sc

sc.version

# Do something to prove it works
rdd = sc.parallelize(range(1000))
rdd.takeSample(False, 5)

###########################
# Quijote wc example

# Getting the file:
#
# Copy&Paste is disabled
# HTTPS as file system is disabled for TextFile
# wget not installed (yum install wget)
# wget https://raw.githubusercontent.com/raul-arrabales/BigData-Hands-on/master/data/quijote_complete.txt
# and finally get the file in the local file system

! ls -la

rdd1 = sc.textFile("quijote_complete.txt")  

rdd1.count()

# Nested wordcount:
wordcounts = rdd1.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False)
        
        
# Check Results
wordcounts.take(5)

# Step by step (explain each step)

rdd1 = sc.textFile("quijote_complete.txt")
rdd1.take(15)

rdd2 = rdd1.map( lambda x: x.replace('.',' ').replace('-',' ').replace(',',' ').lower())
rdd2.take(10)

rdd3 = rdd2.flatMap(lambda x: x.split())
rdd3.count()

rdd3.takeSample(False,6)

rdd4 = rdd3.map(lambda x: (x, 1))
rdd4.take(15)

rdd5 = rdd4.reduceByKey(lambda x,y:x+y)
rdd5.count()

rdd6 = rdd5.map(lambda x:(x[1],x[0]))
rdd6.take(10)

rdd7 = rdd6.sortByKey(False)
rdd7.take(10)





