# Databricks notebook source
# MAGIC %run "../shared/constants"

# COMMAND ----------

spark.sql(f'''
create database if not exists youtube_silver
location "{STREAMING_YT_SPOTIFY_SILVER_PATH}/youtube/";      
''').display()

# COMMAND ----------

spark.sql(f'''
create database if not exists streaming_services_silver
location "{STREAMING_YT_SPOTIFY_SILVER_PATH}/streaming_services/";      
''').display()

# COMMAND ----------

spark.sql(f'''
create database if not exists spotify_silver
location "{STREAMING_YT_SPOTIFY_SILVER_PATH}/spotify/";      
''').display()