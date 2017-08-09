# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com

# JDBC Connection
hostname = "yourDBserver.com"
dbname = "yourDB"
jdbcPort = 3306

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(hostname, jdbcPort, dbname, username, password)

