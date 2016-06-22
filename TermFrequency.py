# In[15]:

rdd = sc.textFile('/user/cloudera/quijotedata/quijote.txt')


# In[16]:

rdd.count()


# In[17]:

rdd2 = sc.textFile('/user/cloudera/quijotedata/quijote_complete.txt')


# In[18]:

rdd2.count()


# In[19]:

rdd2.take(10)


# In[20]:

step1 = rdd2.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower())


# In[22]:

step2 = step1.filter( lambda x: len(x) > 0 )


# In[23]:

step2.take(10)


# In[24]:

step3 = step2.flatMap(lambda x: x.split())


# In[25]:

step3.take(10)


# In[26]:

step4 = step3.map(lambda x: (x, 1))


# In[27]:

step4.take(10)


# In[28]:

step5 = step4.reduceByKey(lambda x,y:x+y)


# In[29]:

step5.take(10)


# In[30]:

step5inverse = step5.map(lambda x:(x[1],x[0]))


# In[31]:

step5inverse.take(10)


# In[32]:

step5sorted = step5inverse.sortByKey(ascending=False)


# In[33]:

step5sorted.take(10)

# In one line:
wordcounts = rdd2.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False)
        
