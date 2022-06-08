# Databricks notebook source
# MAGIC %md 
# MAGIC # Setup the Environment 

# COMMAND ----------

# MAGIC %pip install delta-spark spark-nlp==3.3.3 wordcloud contractions gensim pyldavis==3.2.0

# COMMAND ----------


# Import your dependecies
from delta import *
import pyspark # run after findspark.init() if you need it
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# from pyspark.sql.functions import col, split
from pyspark.sql import functions as F

import re
from textblob import TextBlob

# COMMAND ----------

# MAGIC %md 
# MAGIC # Streaming Data Ingestion

# COMMAND ----------

confluentApiKey = "RSQKGKXY7364BGWJ"
confluentSecret = "O4HcP/YgoBJLDdD5F9i4OBQM27iRhP6NYRayqU6wlPPgEHFD4dU33trLLuOHqi4x"
host = "pkc-w12qj.ap-southeast-1.aws.confluent.cloud:9092"

# COMMAND ----------


streamingInputDF = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", host)  \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
  .option("kafka.ssl.endpoint.identification.algorithm", "https") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .option("subscribe", "product") \
  .load()



# COMMAND ----------

tweet_df_string = streamingInputDF.selectExpr("CAST (key AS STRING)", "CAST(value AS STRING)")

tweet_df_string.display

# COMMAND ----------

# MAGIC %md 
# MAGIC # Cleaning Tweet

# COMMAND ----------

myschema = StructType([StructField('value', StringType(), True)])


# COMMAND ----------

def cleanTweet(tweet:str):
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\S@[A-ZA-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove punctuation 
    my_punctuation = '!"$%&\()*+,-./:;<=>?[\\]^_`{|}~@'
    tweet = re.sub('[' + my_punctuation + ']', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))


    # remove hashtag 
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    
    # remove RT
    tweet = re.sub('RT', '', str(tweet))
    
    # remove enter
    tweet = re.sub('\n\n', ' ', str(tweet))
    
    return tweet 

# COMMAND ----------

clean_tweets = F.udf(cleanTweet, StringType())

# COMMAND ----------


raw_tweets = tweet_df_string.withColumn('processed_text', clean_tweets(col('value')))


# COMMAND ----------

# MAGIC %md 
# MAGIC # Sentiment Analysis 

# COMMAND ----------

def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


def getPolarity(tweet: str)-> float:
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0 :
        return 'Neutral'
    else:
        return 'Positive'

# COMMAND ----------

subjectivity = F.udf(getSubjectivity, FloatType())
polarity = F.udf(getPolarity, FloatType())
sentiment = F.udf(getSentiment, StringType())

# COMMAND ----------

subjectivity_tweets = raw_tweets.withColumn('subjectivity', subjectivity(col('processed_text')))
polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col('processed_text')))
sentiment_tweets = polarity_tweets.withColumn('sentiment', sentiment(col('polarity')))

# COMMAND ----------

# sentiment_tweets.writeStream.format("memory").queryName("tweetquery_sent").trigger(processingTime='2 seconds').start()

# COMMAND ----------

sentiment_tweet = sentiment_tweets \
  .writeStream.format("delta") \
  .outputMode("append") \
  .trigger(processingTime='10 seconds') \
  .option("checkpointLocation", "/tmp/checkpoint") \
  .start("/tmp/delta-tweet-table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Hashtag

# COMMAND ----------


tweets_tab_hashtag = sentiment_tweets \
    .withColumn('word', explode(split(col('value'), ' '))) \
    .groupBy('word') \
    .count() \
    .sort('count', ascending=False) \
    .filter(col('word').contains('#'))

writeTweet2 = tweets_tab_hashtag \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("tweetquery") \
    .trigger(processingTime='2 seconds') \
    .start()

# COMMAND ----------

spark.sql("select * from tweetquery").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Reading Data

# COMMAND ----------

spark.read \
  .format("delta") \
  .load("/tmp/delta-tweet-table") \
  .createOrReplaceTempView("table2")

# COMMAND ----------

spark.sql("SELECT * FROM table2 LIMIT 10").show()
