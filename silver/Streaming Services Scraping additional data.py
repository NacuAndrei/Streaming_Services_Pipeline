# Databricks notebook source
# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/streaming_services_scraping_functions"

# COMMAND ----------

cleaned_streaming_df = spark.read.table("silver_db.silver_table_imdbId")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "all_dates")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

if v_file_date != "all_dates":
    cleaned_streaming_df = cleaned_streaming_df.filter(F.col("last_updated_date") == v_file_date)

# COMMAND ----------

cleaned_streaming_df = cleaned_streaming_df.withColumn("release_date", F.col("release_date").cast("String"))

# COMMAND ----------

# most_recent_date = cleaned_streaming_df.agg(F.max(F.col("last_updated_date"))).collect()[0][0]
# cleaned_streaming_df = cleaned_streaming_df.filter(F.col("last_updated_date") == most_recent_date)

# COMMAND ----------

cleaned_streaming_df = cleaned_streaming_df.filter(cleaned_streaming_df.last_updated_date == F.current_date())

# COMMAND ----------

if RATING not in cleaned_streaming_df.columns:

    columns_to_add = [
        RATING,
        VOTES,
        LANGUAGES,
        COLOR,
        TOP_250,
        BOX_OFFICE,
        PRODUCTION_COMPANIES,
        WRITERS,
        SERIES_YEARS,
    ]

    for column_name in columns_to_add:
        if column_name not in cleaned_streaming_df.columns:
            cleaned_streaming_df = cleaned_streaming_df.withColumn(column_name, F.lit(None).cast("String"))

# COMMAND ----------

batch_df = cleaned_streaming_df.filter(F.col(IS_SCRAPED) == "No").limit(500)

# COMMAND ----------

rdd = batch_df.rdd

rdd_with_show_data = rdd.mapPartitions(fetch_show_data)

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
    StructField(RELEASE_DATE, StringType(), True),
    StructField(DURATION, StringType(), True),
    StructField(GENRES, StringType(), True),
    StructField(IS_ANIMATION, StringType(), True),
    StructField(PLATFORM, StringType(), True),
    StructField(IS_SCRAPED, StringType(), True),
    StructField("last_updated_date", DateType(), True),
    StructField(IMDB_ID, StringType(), True),
    StructField(RATING, StringType(), True),
    StructField(VOTES, StringType(), True),
    StructField(LANGUAGES, StringType(), True),
    StructField(COLOR, StringType(), True),
    StructField(TOP_250, StringType(), True),
    StructField(BOX_OFFICE, StringType(), True),
    StructField(PRODUCTION_COMPANIES, StringType(), True),
    StructField(WRITERS, StringType(), True),
    StructField(SERIES_YEARS, StringType(), True),
])

# COMMAND ----------

df_with_show_data = spark.createDataFrame(rdd_with_show_data, schema)

# COMMAND ----------

if cleaned_streaming_df.schema == df_with_show_data.schema:
    remaining_df = cleaned_streaming_df.join(df_with_show_data, SHOW_ID, "left_anti")
    cleaned_streaming_df = remaining_df.unionByName(df_with_show_data)

# COMMAND ----------

cleaned_streaming_df = cleaned_streaming_df.withColumn("last_updated_date", F.current_date())

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.silver_table_scraped (
# MAGIC   show_id STRING,
# MAGIC   category STRING,
# MAGIC   age_required INTEGER,
# MAGIC   title STRING,
# MAGIC   director STRING,
# MAGIC   cast STRING,
# MAGIC   countries STRING,
# MAGIC   date_added DATE,
# MAGIC   release_date STRING,
# MAGIC   duration STRING,
# MAGIC   genres STRING,
# MAGIC   is_animation STRING,
# MAGIC   platform STRING,
# MAGIC   is_scraped STRING,
# MAGIC   last_updated_date DATE,
# MAGIC   imdb_id STRING,
# MAGIC   rating STRING,
# MAGIC   votes STRING,
# MAGIC   languages STRING,
# MAGIC   color STRING,
# MAGIC   top_250 STRING,
# MAGIC   box_office STRING,
# MAGIC   production_companies STRING,
# MAGIC   writers STRING,
# MAGIC   series_years STRING
# MAGIC )

# COMMAND ----------

cleaned_streaming_df.createOrReplaceTempView("cleaned_streaming_view")

# COMMAND ----------

merge_tables(spark, "silver_db", "silver_table_scraped", "cleaned_streaming_view")