
# Kafka install:
# sudo yum clean all
# sudo yum install kafka
# sudo yum install kafka-server

# Kafka setup
# Edit /etc/kafka/conf/server.properties to ensure that the broker.id is unique for each node and broker in Kafka cluster, 
# and zookeeper.connect points to same ZooKeeper for all nodes and brokers

# Kafka start
# sudo service kafka-server start

# Zookeeper check:
# zookeeper-client
# ls /brokers/ids
# get /brokers/ids/<ID>

# Kafka settings:
# kafka-topics --create --zookeeper quickstart.cloudera:2181 --topic wordcounttopic --partitions 1 --replication-factor 1


import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
