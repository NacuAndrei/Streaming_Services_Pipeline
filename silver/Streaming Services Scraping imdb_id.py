# Databricks notebook source
# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/streaming_services_scraping_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "all_dates")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

cleaned_streaming_df = spark.read.format("delta").load(f"{bronze_incremental_path}/bronze_table/")

# COMMAND ----------

cleaned_streaming_df = cleaned_streaming_df.withColumnRenamed("release_year", "release_date")

# COMMAND ----------

if v_file_date != "all_dates":
    cleaned_streaming_df = cleaned_streaming_df.filter(F.col("last_updated_date") == v_file_date)

# COMMAND ----------

if (IMDB_ID not in cleaned_streaming_df.columns):
    cleaned_streaming_df = cleaned_streaming_df.withColumn(IMDB_ID, F.lit(None).cast("string"))

# COMMAND ----------

batch_df = cleaned_streaming_df.filter(F.col(IMDB_ID).isNull()).limit(500)

# COMMAND ----------

rdd = batch_df.rdd

rdd_with_imdb_ids = rdd.mapPartitions(fetch_imdb_ids)

# COMMAND ----------

schema = StructType([
    StructField(SHOW_ID, StringType(), True),
    StructField(CATEGORY, StringType(), True),
    StructField(AGE_REQUIRED, IntegerType(), True),
    StructField(TITLE, StringType(), True),
    StructField(DIRECTOR, StringType(), True),
    StructField(CAST, StringType(), True),
    StructField(COUNTRIES, StringType(), True),
    StructField(DATE_ADDED, DateType(), True),
    StructField(RELEASE_DATE, IntegerType(), True),
    StructField(DURATION, StringType(), True),
    StructField(GENRES, StringType(), True),
    StructField(IS_ANIMATION, StringType(), True),
    StructField(PLATFORM, StringType(), True),
    StructField(IS_SCRAPED, StringType(), True),
    StructField("last_updated_date", DateType(), True),
    StructField(IMDB_ID, StringType(), True)
])

# COMMAND ----------

df_with_imdb_ids = spark.createDataFrame(rdd_with_imdb_ids, schema)

# COMMAND ----------

if cleaned_streaming_df.schema == df_with_imdb_ids.schema:
    remaining_df = cleaned_streaming_df.join(df_with_imdb_ids, SHOW_ID, "left_anti")
    cleaned_streaming_df = remaining_df.unionByName(df_with_imdb_ids)

# COMMAND ----------

cleaned_streaming_df = cleaned_streaming_df.withColumn("last_updated_date", F.current_date())

# COMMAND ----------

spark.sql(f"""
   CREATE DATABASE IF NOT EXISTS silver_db
   LOCATION '{silver_path}'
   """)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.silver_table_imdbId (
# MAGIC   show_id STRING,
# MAGIC   category STRING,
# MAGIC   age_required INTEGER,
# MAGIC   title STRING,
# MAGIC   director STRING,
# MAGIC   cast STRING,
# MAGIC   countries STRING,
# MAGIC   date_added DATE,
# MAGIC   release_date INTEGER,
# MAGIC   duration STRING,
# MAGIC   genres STRING,
# MAGIC   is_animation STRING,
# MAGIC   platform STRING,
# MAGIC   is_scraped STRING,
# MAGIC   last_updated_date DATE,
# MAGIC   imdb_id STRING
# MAGIC )

# COMMAND ----------

cleaned_streaming_df.createOrReplaceTempView("cleaned_streaming_view")

# COMMAND ----------

merge_tables(spark, "silver_db", "silver_table_imdbId", "cleaned_streaming_view")