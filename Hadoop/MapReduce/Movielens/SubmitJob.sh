# Check pythons script permissions. 
# Check hadoop-streaming.jar location (this is hdp current version).
# File flags to make sure mapper and reducer are copied to all nodes. 
# cound directory shouldn't exist prior to job submission. 

hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-input /movielens/ratings.dat \
-output movielens/count \
-file /root/raul/map.py \
-file /root/raul/reduce.py \
-mapper /root/raul/map.py \
-reducer /root/raul/reduce.py    
