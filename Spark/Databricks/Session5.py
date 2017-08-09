# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com

# JDBC Connection
hostname = "yourDBserver.com"
dbname = "yourDB"
jdbcPort = 3306

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}" \
.format(hostname, jdbcPort, dbname, username, password)

# Or using a dictionary: 

jdbc_url = "jdbc:mysql://{0}:{1}/{2}".format(hostname, jdbcPort, dbname)
# For SQLServer, pass in the "driver" option
# driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# Add "driver" : driverClass
connectionProperties = {
  "user" : username,
  "password" : password
}

# Querying the database
pushdown_query = "(select * from yourDB where id < 53) tab_alias"
df = spark.read.jdbc(
  url=jdbc_url, 
  dbtable=pushdown_query, 
  properties=connectionProperties)

display(df)

# Multiple workers:
df = spark.read.\
      jdbc(url=jdbcUrl, \
              table='yourTable',\
              column='id',\
              lowerBound=1,\
              upperBound=100000, \
              numPartitions=100)
display(df)
