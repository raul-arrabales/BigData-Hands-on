# Create the Dataproc cluster
gcloud dataproc clusters create birth_cluster

# Set the Big Query + Spark virtual env. 
virtualenv bq_plus_spark
source bq_plus_spark/bin/activate
