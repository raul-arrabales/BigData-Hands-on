# Settings for launching Jypyter on the Spark Cluster
# CDH 5.8 (updated parcel 5.8.0.1)
# To be added to .bashrc on cloudera user home

alias python=/home/cloudera/anaconda2/bin/python2.7 
export PYSPARK_PYTHON=/home/cloudera/anaconda2/bin/python2.7
export SPARK_HOME=/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/spark
export PYSPARK_DRIVER_PYTHON=/opt/cloudera/parcels/Anaconda/bin/jupyter 
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='*' --NotebookApp.port=8880"
export PYSPARK_PYTHON=/opt/cloudera/parcels/Anaconda/bin/python
export PATH=/opt/cloudera/parcels/Anaconda/bin:$PATH

# csv support: pyspark --packages com.databricks:spark-csv_2.10:1.2.0 
