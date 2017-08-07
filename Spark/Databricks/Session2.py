# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com 

# Getting the Spark SQL context and imports
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext.getOrCreate(sc.getOrCreate())

# Loading movies dataset as a set of rows
movies = (
  sc.textFile('/FileStore/tables/y81un4d91488113985408/movies.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (mId, t, gs) : Row(mId=int(mId), t=t)))

# Loading ratings dataset as a set of rows
ratings = (
  sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (uId, mId, r, ts) : Row(mId=int(mId), r=float(r))))

# Register both as SQL tables
sqlContext.createDataFrame(movies).registerTempTable("movies")
sqlContext.createDataFrame(ratings).registerTempTable("ratings") 

# Launch the SQL query
avgRs = sqlContext.sql("SELECT t, AVG(r) AS avgR " +
"FROM movies m " +
"JOIN ratings r ON (m.mId = r.mId) " +
"GROUP BY t ORDER BY avgR DESC")

# Inspect resutls
avgRs.take(10) 

# Another example: Counting ratings per user.

# Load ratings as rows with user id, movie id and rating (float):
ratings = (
  sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (uId, mId, r, ts) : Row(uId=int(uId), mId=int(mId), r=float(r))))

# Register the table for SQL
sqlContext.createDataFrame(ratings).registerTempTable("ratings")

# SQL query for counting ratings:
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) FROM ratings GROUP BY uId")

# Inspect results:
UsrRatingsCount.take(10)

# SQL query for counting ratings (ordered):
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) FROM ratings GROUP BY uId ORDER BY COUNT(*) DESC")

# Inspect results:
UsrRatingsCount.take(10)

# SQL query for counting ratings (ordered and good formatting):
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) AS UserCount " +
                                 "FROM ratings " +
                                 "GROUP BY uId " +
                                 "ORDER BY UserCount DESC")

# Inspect results:
UsrRatingsCount.take(10)




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
