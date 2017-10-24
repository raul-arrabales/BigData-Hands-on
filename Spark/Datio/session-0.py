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





