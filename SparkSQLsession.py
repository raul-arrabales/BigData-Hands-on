# Setting up the SQL context for Spark

# In CDH + Anaconda (after pyspark settings and sc):

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


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




