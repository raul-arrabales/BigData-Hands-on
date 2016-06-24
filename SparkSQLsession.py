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

df3 = df1.join(df2, 'nombre')

display(df3)





