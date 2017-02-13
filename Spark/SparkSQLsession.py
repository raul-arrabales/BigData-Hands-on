# Setting up the SQL context for Spark

# In CDH + Anaconda (after pyspark settings and sc):

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


help(sqlContext)

type(sqlContext)



# In Databricks CE Cloud: 

from pyspark.sql import Row

datosEdad = [('Raul', 22), ('Ana', 32), ('Juan', 46)]
df1 = sqlContext.createDataFrame(datosEdad, ['nombre', 'edad'])

filtroEdad = df1.filter(df1.edad >= 30).collect()
print filtroEdad

display(filtroEdad)

datosAltura = [('Raul', 176), ('Ana', 177), ('Juan', 182)]
df2 = sqlContext.createDataFrame(datosAltura, ['nombre', 'altura'])

print(df2.collect()) 

# Inner Join:
df3 = df1.join(df2, 'nombre')

display(df3)


df4 = df1.join(df2, 'nombre').select(df1.nombre, df2.altura)

print(df4.collect())

datosAltura = [('Raul', 176), ('Ana', 177), ('Juan', 182), ('Luisa', 168)]
df2 = sqlContext.createDataFrame(datosAltura, ['nombre', 'altura'])

df4 = df1.join(df2, 'nombre').select(df1.nombre, df2.altura)

df5 = df1.join(df2, 'nombre').select('nombre', 'altura') 

df6 = df1.join(df2, 'nombre', 'right_outer').select('nombre')
print(df6.collect())


# Generating Data:

from faker import Factory
fake = Factory.create()
fake.seed(1234)

# Each entry consists of last_name, first_name, ssn, job, and age (at least 1)
from pyspark.sql import Row
def fake_entry():
  name = fake.name().split()
  return Row(name[1], name[0], fake.ssn(), fake.job(), abs(2016 - fake.date_time().year) + 1)
  
# Create a helper function to call a function repeatedly
def repeat(times, func, *args, **kwargs):
    for _ in xrange(times):
        yield func(*args, **kwargs)

data = list(repeat(10000, fake_entry))

data[0][0], data[0][1], data[0][2], data[0][3], data[0][4]

len(data)

help(sqlContext.createDataFrame)

dataDF = sqlContext.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))

dataDF.printSchema()

sqlContext.registerDataFrameAsTable(dataDF, 'dataframe')

help(dataDF)

dataDF.rdd.getNumPartitions()

newDF = dataDF.distinct().select('*')
newDF.explain(True)

dataDF.show(n=30, truncate=False)

display(subDF)

filteredDF = dataDF.filter(dataDF.age < 10)
filteredDF.show(truncate=False)
filteredDF.count()

display(dataDF.orderBy('age').take(5))

print dataDF.count()
print dataDF.distinct().count()

dataDF.groupBy('occupation').count().show(truncate=False)

