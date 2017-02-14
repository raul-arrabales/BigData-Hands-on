# Change data file field separator, from :: to tab

# Get the data files from HDFS to local: 
hadoop fs -copyToLocal /movielens/movies.dat movies.dat                                                        
hadoop fs -copyToLocal /movielens/ratings.dat ratings.dat 

# Replace all appearances of the separator
sed -i s/::/\\t/g movies.dat ratings.dat

# Upload new formatted files to HDFS
hadoop fs -copyFromLocal ratings.dat /movielens/ratings_tab.dat                                                
hadoop fs -copyFromLocal movies.dat /movielens/movies_tab.dat 

