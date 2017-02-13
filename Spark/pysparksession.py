# Mostly from http://www.mccarroll.net/blog/pyspark2/ 

# coding: utf-8

# Configuring pyspark in Jupyter

import os
import sys

os.environ["JAVA_HOME"] = "/usr/lib/jvm/jre"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
os.environ["PYSPARK_PYTHON"] = "/home/cloudera/anaconda3/bin/python"

sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.9-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setMaster('yarn-client')
conf.setAppName('anaconda-pyspark-language')

sc = SparkContext(conf=conf)

# rdd = sc.textFile('/user/cloudera/quijotedata/quijote.txt')


        

