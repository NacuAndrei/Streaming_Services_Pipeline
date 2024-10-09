# Databricks notebook source
# MAGIC %run "../shared/constants"

# COMMAND ----------

spark.sql(f'''
create database if not exists streaming_youtube_spotify_gold
location "{STREAMING_YT_SPOTIFY_GOLD_PATH}/";      
''').display()