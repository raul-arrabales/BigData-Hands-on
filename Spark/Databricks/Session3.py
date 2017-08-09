# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com 

# Create a faker factory with a random seed
from faker import Factory
fake = Factory.create()
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

# Create the dataset with 10k entries
data = list(repeat(10000, fake_entry))

# Check:
data[0][0], data[0][1], data[0][2], data[0][3], data[0][4]
len(data)

# Build the DataFrame:
dataDF = sqlContext.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))
dataDF.printSchema()


