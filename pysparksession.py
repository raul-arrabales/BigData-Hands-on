# Mostly from http://www.mccarroll.net/blog/pyspark2/ 

# coding: utf-8

# In[1]:

import os


# In[2]:

import sys


# In[3]:

os.environ["JAVA_HOME"] = "/usr/lib/jvm/jre"


# In[4]:

os.environ["SPARK_HOME"] = "/usr/lib/spark"


# In[5]:

os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"


# In[6]:

os.environ["PYSPARK_PYTHON"] = "/home/cloudera/anaconda3/bin/python"


# In[7]:

sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.9-src.zip")


# In[8]:

sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[9]:

from pyspark import SparkConf


# In[10]:

from pyspark import SparkContext


# In[11]:

conf = SparkConf()


# In[12]:

conf.setMaster('yarn-client')


# In[13]:

conf.setAppName('anaconda-pyspark-language')


# In[14]:

sc = SparkContext(conf=conf)


# In[15]:

rdd = sc.textFile('/user/cloudera/quijotedata/quijote.txt')


        

