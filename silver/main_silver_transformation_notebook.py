# Databricks notebook source
# MAGIC %run "./ensure_silver_database"

# COMMAND ----------

# %run "./youtube_silver_transformation_notebook"

# COMMAND ----------

# %run "./Spotify entities"

# COMMAND ----------

# MAGIC %run "./Streaming Services Scraping imdb_id"

# COMMAND ----------

# MAGIC %run "./Streaming Services Scraping additional data"

# COMMAND ----------

# MAGIC %run "./Streaming Services silver cleaning"

# COMMAND ----------

# MAGIC %run "./Streaming Services entities"