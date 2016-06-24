# Setting up the SQL context for Spark

# In CDH + Anaconda (after pyspark settings and sc):

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In Databricks CE Cloud: 

from pyspark.sql import Row
data = [('Alice', 1), ('Bob', 2), ('Bill', 4)]
df = sqlContext.createDataFrame(data, ['name', 'age'])
fil = df.filter(df.age >= 2).collect()
print fil
display(fil)

