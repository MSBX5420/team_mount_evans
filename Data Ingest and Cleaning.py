#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Install packages
# noinspection PyUnresolvedReferences
import matplotlib as mp
# noinspection PyUnresolvedReferences
from matplotlib import pyplot as plt
# noinspection PyUnresolvedReferences
from pyspark.sql import *
# noinspection PyUnresolvedReferences
from pyspark.sql import SparkSession
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import explode
# noinspection PyUnresolvedReferences
from pyspark.sql import functions as F
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import *
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import to_timestamp
# noinspection PyUnresolvedReferences
from pyspark.sql.types import IntegerType
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import hour, mean, count, minute, second


# In[3]:


# Initialize Spark Session & Context
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# In[4]:


# Create Spark DataFrame & display first 10 rows
tweet_raw = (spark.read.format("csv").options(header="true").load("20200312_Coronavirus_Tweets_Subset.CSV"))
display(tweet_raw)


# In[5]:


#### CLEAN DATA ####
# Initially filter tweets in English & create new filtered DataFrame
tweet_filter = tweet_raw.select("*", F.when(tweet_raw.lang == 'en', 'TRUE').alias('eng_true'))
tweet_filter = tweet_filter.filter("eng_true == 'TRUE'")
# Fix Date Structure
tweet_filter = tweet_filter.withColumn('created_at', regexp_replace('created_at', 'T', ' '))
tweet_filter = tweet_filter.withColumn('created_at', regexp_replace('created_at', 'Z', ''))
# Convert to Timestamp
tweet_filter = tweet_filter.withColumn('dt',to_timestamp(tweet_filter.created_at, 'yyyy-MM-dd HH:mm:ss'))
# Drop Unused Columns
tweet_filter = tweet_filter.drop('created_at','reply_to_status_id','reply_to_user_id','reply_to_screen_name','place_type','account_lang')
# Define Columns for Integer Transformation
cols = spark.createDataFrame([('status_id',1),('user_id',2),('favourites_count',3),('retweet_count',4),('followers_count',5),('friends_count',6)])
cols_col = cols.select("_1")
colnames = cols_col.rdd.map(lambda row : row[0]).collect()
# Transform Columns to Integer Type
for colname in colnames:
    tweet_filter = tweet_filter.withColumn(colname, tweet_filter[colname].cast(IntegerType()))
tweet_filter


# In[6]:


#### Simple KPIs ####
# Hourly Tweet Count
hourlyCount = (tweet_filter.groupBy(hour("dt").alias("hour")).agg(count("text").alias("count")).sort(desc("hour")).show())


# In[9]:


# Quote Count
QuoteCount = (tweet_filter.groupBy("is_quote").agg(count("text").alias("count")).sort(desc("is_quote")).show())


# In[10]:


# Retweet Count
RetweetCount = (tweet_filter.groupBy("is_retweet").agg(count("text").alias("count")).sort(desc("is_retweet")).show())


# In[13]:


# Mean Follower Count
FollowerCount = (tweet_filter.agg(mean("followers_count").alias("mean"))).show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# Isolate Datetime column for further analysis
date_col = tweet_filter.select("dt")
dates = date_col.rdd.map(lambda row : row[0]).collect()


# In[ ]:





# In[ ]:





# In[ ]:




