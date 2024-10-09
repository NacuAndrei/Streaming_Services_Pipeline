# Databricks notebook source
import os
from pyyoutube import Api

# COMMAND ----------

os.environ['KAGGLE_USERNAME'] = dbutils.secrets.get(scope = 'abc2zsecrets', key = 'KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = dbutils.secrets.get(scope = 'abc2zsecrets', key = 'KAGGLE_KEY')

YOUTUBE_API_KEY = dbutils.secrets.get(scope = 'abc2zsecrets', key = 'YOUTUBE_API_KEY')

YOUTUBE_API_CLIENT = Api(api_key=YOUTUBE_API_KEY)

# COMMAND ----------

raw_incremental_path = "dbfs:/FileStore/kaggle_datasets/raw_incremental"
raw_netflix_path = "dbfs:/FileStore/kaggle_datasets/netflix_shows/netflix_titles.csv"
raw_amazon_path = "dbfs:/FileStore/kaggle_datasets/amazon-prime-movies-and-tv-shows/amazon_prime_titles.csv"
raw_disney_path = "dbfs:/FileStore/kaggle_datasets/disney-movies-and-tv-shows/disney_plus_titles.csv"


# COMMAND ----------

bronze_incremental_path = ""
silver_incremental_path = ""

# COMMAND ----------

bronze_streaming_path = ""
temp_bronze_streaming_path = ""

silver_streaming_path = ""
temp_silver_streaming_path = ""