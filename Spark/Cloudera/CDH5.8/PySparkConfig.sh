# Using VirtualBox in the host machine

# Download CDH 5.8. 
# From https://www.cloudera.com/downloads/quickstart_vms/5-8.html
# Virtualbox version

# Import VM in Virtualbox. 

# Before starting the VM configure to: 
# 2 vcores, 8 GB RAM (required for Cloudera Manager Express). 

# Start the VM. 

# Launch Cloudera Manager Express. 
# If you don't have 8 GB RAM in your VM, force it and wait:
sudo /home/cloudera/cloudera-manager --force --express   
# (remember less than 8GB is not supported, nor expected to work fine)

# Install Update Parcel from CDH 5.8 to CDH 5.8.0.1. 

# Install Anaconda Parcel (version 4.1.1): 
# https://docs.continuum.io/anaconda/cloudera  

# Force Python 2.7 also in local, apart from the cluster version. 
# Install Anaconda as pkg in home/cloudera

# Configure Python in local for v2.7:
alias python=/home/cloudera/anaconda2/bin/python2.7 
export PYSPARK_PYTHON=/home/cloudera/anaconda2/bin/python2.7
export SPARK_HOME=/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/spark

# Configure Jupyter for pyspark in the cluster:
export PYSPARK_DRIVER_PYTHON=/opt/cloudera/parcels/Anaconda/bin/jupyter 
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='*' --NotebookApp.port=8880"
export PYSPARK_PYTHON=/opt/cloudera/parcels/Anaconda/bin/python
export PATH=/opt/cloudera/parcels/Anaconda/bin:$PATH


# Launch pyspark.
# If you need CSV suppert: pyspark --packages com.databricks:spark-csv_2.10:1.2.0 

