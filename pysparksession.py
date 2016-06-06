
# coding: utf-8

# In[1]:

import os


# In[2]:

import sys


# In[3]:

os.environ["JAVA_HOME"] = "/usr/lib/jvm/jre"


# In[5]:

os.environ["SPARK_HOME"] = "/usr/lib/spark"


# In[6]:

os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"


# In[8]:

os.environ["PYSPARK_PYTHON"] = "/home/cloudera/anaconda3/bin"


# In[9]:

sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.9-src.zip")


# In[10]:

sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[11]:

from pyspark import SparkConf


# In[12]:

from pyspark import SparkContext


# In[13]:

conf = SparkConf()


# In[14]:

conf.setMaster('yarn-client')


# In[15]:

conf.setAppName('anaconda-pyspark-language')


# In[16]:

sc = SparkContext(conf=conf)





