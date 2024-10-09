# Databricks notebook source
# MAGIC %run "../shared/common_functions"

# COMMAND ----------

# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/constants"

# COMMAND ----------

# MAGIC %run "../shared/kaggle_ingestion_functions"

# COMMAND ----------

# MAGIC %run "../shared/youtube_common_functions"

# COMMAND ----------

import time

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

import time

merged_temp_save_location = f'{STREAMING_YT_SPOTIFY_BRONZE_PATH}/temp_youtube_scraped/merged_youtube_scraped/'
merged_df = read_merged_scraped_youtube_df_with_checkpoints(merged_temp_save_location)
merged_df = scrape_youtube_dataset_batched(merged_df, merged_temp_save_location)

# COMMAND ----------

merged_temp_save_location = f'{STREAMING_YT_SPOTIFY_BRONZE_PATH}/temp_youtube_scraped/merged_youtube_scraped/'
merged_df = spark.read.format('delta').load(merged_temp_save_location)

# COMMAND ----------

merged_df \
    .write.format('delta')\
    .mode('overwrite')\
    .option('overwriteSchema', 'true') \
    .save(merged_temp_save_location)