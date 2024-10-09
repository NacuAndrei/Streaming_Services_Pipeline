# Databricks notebook source
# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/streaming_services_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;

# COMMAND ----------

dbutils.widgets.text("p_file_date", "all_dates")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

raw_unified_streaming_df = spark.read.parquet(raw_incremental_path)

# COMMAND ----------

if "last_updated_date" not in raw_unified_streaming_df.columns:
    raw_unified_streaming_df = raw_unified_streaming_df.withColumn("last_updated_date", F.lit(None))
    
raw_unified_streaming_df = raw_unified_streaming_df.withColumn("last_updated_date", F.to_date("last_updated_date"))

# COMMAND ----------

if v_file_date != "all_dates":
    raw_unified_streaming_df = raw_unified_streaming_df.filter(F.col("last_updated_date") == v_file_date)

# COMMAND ----------

# raw_netflix_df = load_csv(raw_netflix_path)
# raw_amazon_df = load_csv(raw_amazon_path)
# raw_disney_df = load_csv(raw_disney_path)

# COMMAND ----------

# raw_unified_streaming_df = raw_unified_streaming_df \
#                             .withColumnRenamed("listed_in", GENRES) \
#                             .withColumnRenamed("country", COUNTRIES) \
#                             .withColumnRenamed("type", CATEGORY) \
#                             .withColumnRenamed("rating", AGE_REQUIRED) \
#                             .transform(remove_quotes_from_country) \
#                             .transform(lambda df: add_platform_column(df, dataframes_ids['raw_unified_streaming_df']))

# COMMAND ----------

# unwanted_strings = ["Disney ", "Disney's ", "Disneynature ", "Marvel's "]

# for unwanted_string in unwanted_strings:
#     raw_disney_df = replace_unwanted_string_in_column(raw_disney_df, unwanted_string, "")

# COMMAND ----------

# raw_unified_streaming_df = raw_netflix_df.union(raw_amazon_df).union(raw_disney_df)

# COMMAND ----------

# from pyspark.sql.functions import lit, col, when
# from pyspark.sql.window import Window
# import pyspark.sql.functions as F

# window_spec = Window.orderBy(F.monotonically_increasing_id())
# raw_unified_streaming_df = raw_unified_streaming_df.withColumn("row_num", F.row_number().over(window_spec))

# raw_unified_streaming_df = raw_unified_streaming_df.withColumn(
#     "last_updated_date",
#     when(col("row_num") <= 4, lit(v_file_date))
#     .when((col("row_num") > 4) & (col("row_num") <= 8), F.date_add(lit(v_file_date), 1))
#     .when((col("row_num") > 8) & (col("row_num") <= 10), F.date_add(lit(v_file_date), 2))
# )

# raw_unified_streaming_df = raw_unified_streaming_df.drop("row_num")

# COMMAND ----------

raw_unified_streaming_df = raw_unified_streaming_df.withColumn(COUNTRIES, F.regexp_replace(raw_unified_streaming_df[COUNTRIES], r"^,\s+", ""))

# COMMAND ----------

wrong_rated_df = raw_unified_streaming_df.filter(raw_unified_streaming_df[AGE_REQUIRED].contains("min"))

raw_unified_streaming_df = move_values_between_columns(raw_unified_streaming_df, wrong_rated_df, SHOW_ID, DURATION, AGE_REQUIRED)

# COMMAND ----------

list_regex = [JAPANESE_REGEX, ARABIC_REGEX, KOREAN_REGEX, CHINESE_REGEX, DEVANAGARI_REGEX, THAI_REGEX]

filtered_df = filter_non_english_values(raw_unified_streaming_df, list_regex)

# COMMAND ----------

translated_data_df = spark.createDataFrame(
    [
        ("nflx2639", "1000 Congratulations"),
        ("nflx2640", "Cairo Class"),
        ("nflx4915", "Son of the Sea"),
        ("nflx5023", "The Chase"),
        ("nflx5975", "Evil Plan of the Cunning Man"),
        ("nflx6178", "Ninja Hattori-kun"),
        ("nflx7102", "Witch Hunt"),
        ("nflx7109", "The Strongest Warrior Mini Force: The Birth of Heroes"),
        ("nflx8775", "Judgment Day"),
        ("nflx8795", "Clash"),
        ("amzn8712", "Broker")
    ],
    schema=StructType([
        StructField(SHOW_ID, StringType(), True),
        StructField("title_translation", StringType(), True)
    ])
)

# COMMAND ----------

raw_unified_streaming_df = raw_unified_streaming_df.join(translated_data_df, on=SHOW_ID, how="left") \
    .withColumn(
        TITLE,
        F.when(F.col("title_translation").isNotNull(), F.col("title_translation")).otherwise(F.col(TITLE))
    ) \
    .drop("title_translation")

# COMMAND ----------

age_required_mapping_df = spark.createDataFrame(list(rating_mapping.items()), [AGE_REQUIRED, "mapped_age_required"])

# COMMAND ----------

raw_unified_streaming_df = map_column(raw_unified_streaming_df, age_required_mapping_df, "mapped_age_required", AGE_REQUIRED)

# COMMAND ----------

raw_unified_streaming_df = raw_unified_streaming_df.withColumn(AGE_REQUIRED, F.col(AGE_REQUIRED).cast("int"))

# COMMAND ----------

remove_by_prefix_list = ["30-Minute", "30 Minutes", "16 Minute", "15-Minute", "10 Minute", "10 Day Yoga", "9-Minute", "9 Hour", "8 Minute", "8 Hours", "7 Minute", "6 Minutes", "5-Minute", "Yoga For"]

raw_unified_streaming_df = remove_values_by_prefix(raw_unified_streaming_df, TITLE, remove_by_prefix_list)

# COMMAND ----------

raw_unified_streaming_df = raw_unified_streaming_df.withColumn(
    IS_ANIMATION, 
    F.when(F.col(GENRES).contains("Animation") | F.col(GENRES).contains("Anime Series"), "yes").otherwise("no")
)

# COMMAND ----------

raw_unified_streaming_df = convert_date_format(raw_unified_streaming_df, DATE_ADDED)

# COMMAND ----------

raw_unified_streaming_df = raw_unified_streaming_df.withColumn("IS_SCRAPED", F.lit("No"))

# COMMAND ----------

cleaned_streaming_df = raw_unified_streaming_df.select(SHOW_ID, CATEGORY, AGE_REQUIRED, TITLE, DIRECTOR, CAST, COUNTRIES, DATE_ADDED, RELEASE_YEAR, DURATION, GENRES, IS_ANIMATION, PLATFORM, IS_SCRAPED, "last_updated_date")

# COMMAND ----------

cleaned_streaming_df = cleaned_streaming_df.dropDuplicates([TITLE])

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create the database using the variable for the location
# MAGIC spark.sql(f"""
# MAGIC    CREATE DATABASE IF NOT EXISTS bronze_db
# MAGIC    LOCATION '{bronze_path}'
# MAGIC    """)

# COMMAND ----------

# cleaned_streaming_df = cleaned_streaming_df.withColumn(
#     "last_updated_date",
#     F.when(cleaned_streaming_df.show_id == "nflx9", "2023-01-01").otherwise(cleaned_streaming_df.last_updated_date)
# )

# COMMAND ----------

# cleaned_streaming_df = cleaned_streaming_df.withColumn(
#     "title",
#     F.when(cleaned_streaming_df.show_id == "nflx10", "test").otherwise(cleaned_streaming_df.title)
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_db.bronze_table (
# MAGIC     show_id STRING,
# MAGIC     category STRING,
# MAGIC     age_required INT,
# MAGIC     title STRING,
# MAGIC     director STRING,
# MAGIC     cast STRING,
# MAGIC     countries STRING,
# MAGIC     date_added DATE,
# MAGIC     release_year INT,
# MAGIC     duration STRING,
# MAGIC     genres STRING,
# MAGIC     is_animation STRING,
# MAGIC     platform STRING,
# MAGIC     is_scraped STRING,
# MAGIC     last_updated_date DATE
# MAGIC )

# COMMAND ----------

cleaned_streaming_df.createOrReplaceTempView("cleaned_streaming_view")

# COMMAND ----------

merge_tables(spark, "bronze_db", "bronze_table", "cleaned_streaming_view")

# COMMAND ----------

bronze_table_df = spark.read.table("bronze_db.bronze_table")
display(bronze_table_df)