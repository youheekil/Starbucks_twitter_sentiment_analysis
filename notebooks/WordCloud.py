# Databricks notebook source
# MAGIC %pip install wordcloud

# COMMAND ----------

from wordcloud import WordCloud
from wordcloud import ImageColorGenerator
from wordcloud import STOPWORDS
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

# MAGIC %pip install sparknlp

# COMMAND ----------

import sparknlp
from sparknlp.base import *


# COMMAND ----------


from pyspark.ml import Pipeline

# spark-nlp-1.3.0.jar is attached to the cluster. This library was downloaded from the
# spark-packages repository https://spark-packages.org/package/JohnSnowLabs/spark-nlp
from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *


# COMMAND ----------

# MAGIC %md 
# MAGIC # Spark NLP

# COMMAND ----------

# Create pre-processing stages

# Stage 1: DocumentAssembler as entry point
documentAssembler = DocumentAssembler() \
                    .setInputCol("value") \
                    .setOutputCol("document")
 
# Stage 2: Tokenizer
tokenizer = Tokenizer() \
              .setInputCols(["document"]) \
              .setOutputCol("token")
 
# Stage 3: Normalizer to lower text and to remove html tags, hyperlinks, twitter handles, 
# alphanumeric characters (integers, floats), timestamps in the format hh:mm (e.g. 10:30) and punctuation
cleanUpPatterns = [r"RT", "<[^>]*>", r"www\S+", r"http\S+", "@[^\s]+", "[\d-]", "\d*\.\d+", "\d*\:\d+", "[^\w\d\s]"]
normalizer = Normalizer() \
                .setInputCols("token") \
                .setOutputCol("normalized") \
                .setCleanupPatterns(cleanUpPatterns) \
                .setLowercase(True)
 
# Stage 4: Remove stopwords
stopwords = StopWordsCleaner()\
              .setInputCols("normalized")\
              .setOutputCol("cleanTokens")\
              .setCaseSensitive(False)
 
# Stage 5: Lemmatizer
lemma = LemmatizerModel.pretrained() \
              .setInputCols(["cleanTokens"]) \
              .setOutputCol("lemma")
 
# Stage 6: Stemmer stems tokens to bring it to root form
#.setInputCols(["cleanTokens"]).setOutputCol("stem") \
stemmer = Stemmer() \
            .setInputCols(["lemma"]) \
            .setInputCols(["cleanTokens"]) \
            .setOutputCol("stem")
 
# Stage 7: Finisher to convert custom document structure to array of tokens
finisher = Finisher() \
            .setInputCols(["stem"]) \
            .setOutputCols(["token_features"]) \
            .setOutputAsArray(True) \
            .setCleanAnnotations(False)

# COMMAND ----------

# Create pre-processing stages
 
# Stage 1: DocumentAssembler as entry point
documentAssembler = DocumentAssembler() \
                    .setInputCol("value") \
                    .setOutputCol("document")
 
# Stage 2: Normalizer to lower text and to remove html tags, hyperlinks, twitter handles, alphanumeric characters (integers, floats) and timestamps in the format hh:mm (e.g. 10:30) 
cleanUpPatterns = [r"RT", r"\n\n", "<[^>]*>", r"www\S+", r"http\S+", "@[^\s]+", "[\d-]", "\d*\.\d+", "\d*\:\d+"]
documentNormalizer = DocumentNormalizer() \
                      .setInputCols("document") \
                      .setOutputCol("normalizedDocument") \
                      .setAction("clean") \
                      .setPatterns(cleanUpPatterns) \
                      .setReplacement("") \
                      .setPolicy("pretty_all") \
                      .setLowercase(True)


# COMMAND ----------

DF = (
   spark.read \
      .format("delta") \
      .load("/tmp/delta-tweet-table") \
      .createOrReplaceTempView("tweet_data")
)

tweet_df = spark.sql("SELECT * FROM tweet_data")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Normalized

# COMMAND ----------

# Check pre-processing pipeline
cleanup_pipeline = Pipeline(stages=[documentAssembler, documentNormalizer])
empty_df = spark.createDataFrame([['']]).toDF("value")
prep_pipeline_model = cleanup_pipeline.fit(empty_df)
 
result = prep_pipeline_model.transform(tweet_df)
result.select('normalizedDocument.result').show(truncate=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Token

# COMMAND ----------

# Check pre-processing pipeline
prep_pipeline = Pipeline(stages=[documentAssembler, tokenizer, normalizer, stopwords, lemma, stemmer, finisher])
 
empty_df = spark.createDataFrame([['']]).toDF("value")
prep_pipeline_model = prep_pipeline.fit(empty_df)
result = prep_pipeline_model.transform(tweet_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC # WordCloud 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Negative Sentiment

# COMMAND ----------

# Word cloud for negative tweets
neg = tweet_df.filter(tweet_df.sentiment == "Negative").toPandas()
text = " ".join(i for i in neg.processed_text)
text = text.lower()
stopwords = set(STOPWORDS)
wordcloud = WordCloud(stopwords=stopwords, background_color="white").generate(text)
plt.figure( figsize=(15,10))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Positive Sentiment

# COMMAND ----------

# Word cloud for negative tweets
pos = tweet_df.filter(tweet_df.sentiment == "Positive").toPandas()
text = " ".join(i for i in pos.processed_text)
text = text.lower()
stopwords = set(STOPWORDS)
wordcloud = WordCloud(stopwords=stopwords, background_color="white").generate(text)
plt.figure( figsize=(15,10))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()