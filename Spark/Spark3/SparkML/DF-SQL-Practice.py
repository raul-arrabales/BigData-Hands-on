# PySpark Hands-On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com 

# Getting the Spark SQL context and imports
from pyspark.sql import SQLContext, Row
# sqlContext already available in Databricks CE
# sqlContext = SQLContext.getOrCreate(sc.getOrCreate())

# Creating a simple DataFrame programatically
array = [Row(key="a", group="vowels", value=1),
         Row(key="b", group="consonants", value=2),
         Row(key="c", group="consonants", value=3),
         Row(key="d", group="consonants", value=4),
         Row(key="e", group="vowels", value=5)]

dataframe = sqlContext.createDataFrame(sc.parallelize(array))
dataframe.registerTempTable("PythonTestTable")

# Visualize (in Databricks cloud - Display() )
display(dataframe)

# Creating more sample DataFrames:

# Sample age data:
datosEdad = [('Raul', 22), ('Ana', 32), ('Juan', 46)]
df1 = sqlContext.createDataFrame(datosEdad, ['nombre', 'edad'])

# Apply filter to age data:
filtroEdad = df1.filter(df1.edad >= 30).collect()
display(filtroEdad)

# Sample height data
datosAltura = [('Raul', 176), ('Ana', 177), ('Juan', 182)]
df2 = sqlContext.createDataFrame(datosAltura, ['nombre', 'altura'])
print(df2.collect())

# Join height and age data:
df3 = df1.join(df2, 'nombre')
display(df3)

# Explore different queries
df4 = df1.join(df2, 'nombre').select(df1.nombre, df2.altura)
df4.collect()

df5 = df1.join(df2, 'nombre').select('nombre', 'altura') 
df5.collect()

df6 = df1.join(df2, 'nombre', 'right_outer').select('nombre')
df6.collect()




# MOVIE RATINGS EXAMPLE:

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


