# Fake Data Generation
## Using faker and Spark SQL

! pip install faker

from faker import Faker
fake = Faker('es_ES')
fake.seed(6243)

# Each entry consists of last_name, first_name, ssn, job, and age (at least 1)
from pyspark.sql import Row
def fake_entry():
  name = fake.name().split()
  return Row(name[1], name[0], fake.ssn(), fake.job(), abs(2017 - fake.date_time().year) + 1)
  
  
# Create a helper function to call a function repeatedly
def repeat(times, func, *args, **kwargs):
    for _ in xrange(times):
        yield func(*args, **kwargs)
        
# Create the dataset with 1k entries
data = list(repeat(1000, fake_entry))

# Check:
data[0][0], data[0][1], data[0][2], data[0][3], data[0][4]


# Check:
len(data)

# Build the DataFrame:
dataDF = sqlContext.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))
dataDF.printSchema()

# Check DF
print dataDF.count()
print dataDF.distinct().count()

dataDF.explain()

# Check DataFrame (RDD) partitions
dataDF.rdd.getNumPartitions()

# See Spark planning: 
newDF = dataDF.distinct().select('*')
newDF.explain(True)

# See the data
dataDF.show(n=30, truncate=False)

# Applying filters
filteredDF = dataDF.filter(dataDF.age < 10)
filteredDF.show(truncate=False)
filteredDF.count()

# DF API Queries
temp = dataDF.orderBy('age')
temp.show(truncate=True)

# DF API Queries
dataDF.orderBy('age').take(5)

dataDF.groupBy('occupation').count().show(truncate=False)

dataDF.filter(dataDF.age < 18).groupBy('occupation').count().show(truncate=False)

from pyspark.sql.functions import col
occCount = dataDF.filter(dataDF.age < 80).groupBy('occupation').count()
occCount.sort(col('count').desc()).show(truncate=False)

occCount.registerTempTable("occTable")
res = sqlContext.sql("select * from occTable order by count desc")
res.show()

sqlContext.sql("select * from occTable order by count desc").explain()

# Other way to register the DataFrame as table for SQL usage:
sqlContext.registerDataFrameAsTable(dataDF, 'people') 

sqlContext.sql("select * from people where people.age > 46").show()

sqlContext.sql("select count(ssn) as number, occupation from people where age > 18 group by occupation").show()

