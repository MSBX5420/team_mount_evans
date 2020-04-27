#!/usr/bin/env python
# coding: utf-8

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
from pyspark.sql.types import IntegerType
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import hour, mean, count, minute, second, date_format, col, explode, to_timestamp
from pyspark.sql.functions import *
# noinspection PyUnresolvedReferences
from pyspark.sql.window import Window
# noinspection PyUnresolvedReferences
import seaborn as sns
# noinspection PyUnresolvedReferences
import io 
# noinspection PyUnresolvedReferences
import datetime as dt
# noinspection PyUnresolvedReferences
import matplotlib.dates as md
# noinspection PyUnresolvedReferences
import sys
# noinspection PyUnresolvedReferences
import pickle 
# noinspection PyUnresolvedReferences
import statsmodels.api as sm
# noinspection PyUnresolvedReferences
import statsmodels.formula.api as smf 
# noinspection PyUnresolvedReferences
import pandas as pd
# noinspection PyUnresolvedReferences
import numpy as np
#pip install textblob
import textblob as tb
#pip install wordcloud
from wordcloud import WordCloud, STOPWORDS

##### Initialize Spark Session & Context #####
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext



# Create Spark DataFrame & display first 10 rows
input_bucket = 's3://msbx5420-2020'
input_path = '/mountTeamEvans/*.CSV'
tweet_raw = (spark.read.format("csv").options(header="true").load(input_bucket + input_path))
display(tweet_raw)


#### CLEAN DATA ####
# Initially filter tweets in English & create new filtered DataFrame
tweet_filter = tweet_raw.select("*", F.when(tweet_raw.lang == 'en', 'TRUE').alias('eng_true'))
tweet_filter = tweet_filter.filter("eng_true == 'TRUE'")
# Fix Date Structure
tweet_filter = tweet_filter.withColumn('created_at', regexp_replace('created_at', 'T', ' '))
tweet_filter = tweet_filter.withColumn('created_at', regexp_replace('created_at', 'Z', ''))
# Convert to Timestamp
tweet_filter = tweet_filter.withColumn('created_at',to_timestamp(tweet_filter.created_at, 'yyyy-MM-dd HH:mm:ss'))
# Drop Unused Columns
tweet_filter = tweet_filter.drop('reply_to_status_id','reply_to_user_id','reply_to_screen_name','place_type','account_lang')
# Define Columns for Integer Transformation
cols = spark.createDataFrame([('status_id',1),('user_id',2),('favourites_count',3),('retweet_count',4),('followers_count',5),('friends_count',6)])
cols_col = cols.select("_1")
colnames = cols_col.rdd.map(lambda row : row[0]).collect()
# Transform Columns to Integer Type
for colname in colnames:
    tweet_filter = tweet_filter.withColumn(colname, tweet_filter[colname].cast(IntegerType()))
tweet_filter


##### Tweet Sentiment Analysis
# Create simple tweet sentiment analysis using TextBlob

positive = 0
negative = 0
neutral = 0
polarity = 0


def percent_calc(a, b):
    return 100 * float(a) / float(b)

# Isolate tweets column for sentiment analysis
tweets_col = tweet_filter.select("text")
tweets = tweets_col.rdd.map(lambda row : row[0]).collect()
n_tweets = len(tweets_col.rdd.map(lambda row : row[0]).collect())

for tweet in tweets:
    print(tweet)
    myAnalysis = tb.TextBlob(tweet)
    polarity += myAnalysis.sentiment.polarity
    if myAnalysis.sentiment.polarity == 0:
        neutral += 1
    elif myAnalysis.sentiment.polarity > 0.00:
        positive += 1
    elif myAnalysis.sentiment.polarity < 0.00:
        negative += 1
        
positive = percent_calc(positive, n_tweets)
negative = percent_calc(negative, n_tweets)
neutral = percent_calc(neutral, n_tweets)

positive = format(positive, '.2f')
negative = format(negative, '.2f')
neutral = format(neutral, '.2f')


if polarity > 0:
    print('----------------------------------------------------------------------------')
elif polarity < 0:
    print('Negative')
elif polarity == 0:
    print('Neutral')


# Plot results

labels = ['Positive [' + str(positive) + '%]', 'Neutral [' + str(neutral) + '%]','Negative [' + str(negative) + '%]']
sizes = [positive, neutral, negative]
colors = ['green', 'yellow', 'red']
patches, texts = plt.pie(sizes, colors = colors, startangle = 90)
plt.legend(patches, labels, loc = "best")
plt.title('How people are reacting on ' + 'Covid-19' + ' by analyzing ' + str(n_tweets) + ' Tweets.')
plt.axis('equal')
plt.tight_layout()
plt.savefig('s3://msbx5420-2020/mountTeamEvans/sentimentPie.png')

######## WordClouds ########
#CONVERTING TWEETS FOR WORDCLOUD
marchtext=pd.DataFrame(tweets)
text=str(marchtext)

#CREATE AND RUN WORDCLOUD
stopwords=set(STOPWORDS)
wordcloud=WordCloud(stopwords=stopwords).generate(text)

plt.imshow(wordcloud)
plt.figure(figsize = (10, 10), facecolor = None) 
plt.savefig('s3://msbx5420-2020/mountTeamEvans/wordcloud.png')

#### Simple KPIs ####
# Hourly Tweet Count
hourlyCount = (tweet_filter.groupBy(hour("created_at").alias("hour")).agg(count("text").alias("count")).sort(desc("hour")).show())

# Quote Count
QuoteCount = (tweet_filter.groupBy("is_quote").agg(count("text").alias("count")).sort(desc("is_quote")).show())

# Retweet Count
RetweetCount = (tweet_filter.groupBy("is_retweet").agg(count("text").alias("count")).sort(desc("is_retweet")).show())

# Mean Follower Count
FollowerCount = (tweet_filter.agg(mean("followers_count").alias("mean"))).show()


#Drop NA/Null values in country code column
tweet_raw2 = tweet_filter.na.drop(subset=["country_code"])

tweet_raw2.groupBy("country_code")     .count()     .orderBy(col("count").desc())     .show()

# Plot the histogram of langauge that related to Coronavirus
# tweet_raw2.groupby('Lang').count().select('count').rdd.flatMap(lambda x: x).histogram(10)


# Count number of verified accounts
tweet_raw2.groupBy("verified")     .count()     .orderBy(col("count").desc())     .show()



#Drop NA/Null values in langauge column
tweet_raw3 = tweet_filter.na.drop(subset=["source"])

tweet_raw3.groupBy("source")     .count()     .orderBy(col("count").desc())     .show()


# Plot the histogram of verified accounts
tweet_raw.groupby('source').count().select('count').rdd.flatMap(lambda x: x).histogram(10)



# Top10 Languages used by Tweets
spark = SparkSession.builder.appName("Twitter Data Analysis").getOrCreate()
df = tweet_filter
df.createOrReplaceTempView("BtsCovSpo")
sqlDF = spark.sql("select count(*) as Total_count, lang as Language from BtsCovSpo group by lang order by Total_count desc limit 11")
pd = sqlDF.toPandas()


def plot12():
    #plt.title('Top10 Languages used by Tweets')
    #sns.pointplot(y="Total_count", x="Language",data=pd)
    pd.plot.line(x="Language", y="Total_count", title="Top10 Languages used by Tweets")
    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image


# Time series analysis
tweets_pdf = tweet_filter.toPandas()
tweets_pdf.head(10)

tweet_filter = tweet_filter.withColumn('month',F.month(F.to_timestamp('created_at', 'dd/MM/yyyy')))



# Count number of tweets in each month

tweet_filter.groupBy('month').count().show()

tweet_filter2 = tweet_filter.select('created_at', date_format('created_at', 'u').alias('dow_number'), date_format('created_at', 'E').alias('dow_string'))
tweet_filter2.show()

# Count number of tweets each day of the week

tweet_filter2.groupBy('dow_string').count().show()


# Count number of retweeted in each months
tweet_filter.groupBy('month').sum('retweet_count').show()


# create a spark session
spark = SparkSession.builder.master("local").appName("Structured Streaming").getOrCreate()
pd.to_datetime(tweets_pdf['created_at'],errors='coerce')
idx = pd.DatetimeIndex(pd.to_datetime(tweets_pdf['created_at'],errors='coerce'))

idx



ones = np.ones(len(tweets_pdf['created_at']))
ITAvWAL = pd.Series(ones, index=idx)
per_minute = ITAvWAL.resample('1Min').sum().fillna(0)

per_minute


# Commented out IPython magic to ensure Python compatibility.
# The following code is to manipulate the data, and then visualize it into a time series line chart using Matplotlib. 

# Plotting the series
# %matplotlib inline
fig, ax = plt.subplots()
ax.grid(True)
ax.set_title("Tweet Numbers")
interval = md.MinuteLocator(interval=100)
date_formatter = md.DateFormatter('%H:%M')

#Change number according to the data
datemin = dt.datetime(2020, 3, 12, 00, 00) 
datemax = dt.datetime(2020, 3, 12, 17, 30)


ax.xaxis.set_major_locator(interval) 
ax.xaxis.set_major_formatter(date_formatter) 
ax.set_xlim(datemin, datemax)
max_freq = per_minute.max()
min_freq = per_minute.min()
ax.set_ylim(min_freq-100, max_freq+100) 
ax.plot(per_minute.index, per_minute)
display(fig)
plt.savefig('s3://msbx5420-2020/mountTeamEvans/timeSeries.png')

# Sum of confirmed cases around the world
coronadf = (spark.read.format("csv").options(header="true").load("train.csv"))
coronadf.show()

coronadf.groupBy("Country_Region").agg({'ConfirmedCases': 'sum'}).orderBy("sum(ConfirmedCases)", ascending = False).show()

coronadf.groupBy("Province_State").agg({'ConfirmedCases': 'sum'}).orderBy("sum(ConfirmedCases)", ascending = False).show()

coronadf.groupBy("Country_Region").agg({'Fatalities': 'sum'}).orderBy("sum(Fatalities)", ascending = False).show()



# Plot the histogram of Fatalities case in each countries
coronadf.groupby('Country_Region').count().select('count').rdd.flatMap(lambda x: x).histogram(10)



# Filtering to find only US and ordering by confirmed cases
coronadf2 = coronadf.filter("Country_Region == 'US'").groupBy("Province_State").agg({'ConfirmedCases': 'sum'}).orderBy("sum(ConfirmedCases)", ascending = False)
coronadf.filter("Country_Region == 'US'").groupBy("Province_State").agg({'ConfirmedCases': 'sum'}).orderBy("sum(ConfirmedCases)", ascending = False).show()


coronadf2 = coronadf2.withColumn('ConfirmedCases%', F.col('sum(ConfirmedCases)')/F.sum('sum(ConfirmedCases)').over(Window.partitionBy()))
coronadf2.orderBy('ConfirmedCases%', ascending=False).show()


# Filtering to find only Fatalities rate in the US and ordering by Fatalities
coronadf3 = coronadf.filter("Country_Region == 'US'").groupBy("Province_State").agg({'Fatalities': 'sum'}).orderBy("sum(Fatalities)", ascending = False)
coronadf.filter("Country_Region == 'US'").groupBy("Province_State").agg({'Fatalities': 'sum'}).orderBy("sum(Fatalities)", ascending = False)
coronadf3.withColumn('Fatalities%', F.col('sum(Fatalities)')/F.sum('sum(Fatalities)').over(Window.partitionBy())).show()

