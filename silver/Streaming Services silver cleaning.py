# Databricks notebook source
# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/streaming_services_functions"

# COMMAND ----------

scraped_streaming_df = spark.read.table("silver_db.silver_table_scraped")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "all_dates")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

if v_file_date != "all_dates":
    scraped_streaming_df = scraped_streaming_df.filter(scraped_streaming_df.last_updated_date == F.current_date())

# COMMAND ----------

if RELEASE_YEAR in scraped_streaming_df.columns:
    scraped_streaming_df = scraped_streaming_df.withColumnRenamed(RELEASE_YEAR, RELEASE_DATE)

# COMMAND ----------

scraped_streaming_df = scraped_streaming_df.withColumn(RELEASE_DATE, F.regexp_replace(RELEASE_DATE, r"\s*\(.*\)", ""))

# COMMAND ----------

if dict(scraped_streaming_df.dtypes)[RELEASE_DATE] == 'string':
    scraped_streaming_df = scraped_streaming_df.withColumn(
        RELEASE_DATE,
        F.when(F.col(RELEASE_DATE).rlike("^\d{4}-\d{2}-\d{2}$"), F.to_date(F.col(RELEASE_DATE), "yyyy-MM-dd"))
    )

# COMMAND ----------

year_format = "yyyy"
month_year_format = "MMM yyyy"
full_date_format = "dd MMM yyyy"

if dict(scraped_streaming_df.dtypes)[RELEASE_DATE] != 'date':
    scraped_streaming_df = scraped_streaming_df.withColumn(
        RELEASE_DATE,
        F.when(F.col(RELEASE_DATE).rlike("^\d{4}$"), F.to_date(F.col(RELEASE_DATE), year_format))
        .when(F.col(RELEASE_DATE).rlike("^\d{2} [A-Za-z]{3} \d{4}$"), F.to_date(F.col(RELEASE_DATE), full_date_format))
        .when(F.col(RELEASE_DATE).rlike("^[A-Za-z]{3} \d{4}$"), F.to_date(F.col(RELEASE_DATE), month_year_format))
    )

# COMMAND ----------

if DURATION in scraped_streaming_df.columns:
    scraped_streaming_df = scraped_streaming_df.withColumn(DURATION, F.regexp_replace(DURATION, r"\D+", ""))
    scraped_streaming_df = scraped_streaming_df.withColumn(DURATION, F.col(DURATION).cast("int"))

    scraped_streaming_df = scraped_streaming_df.withColumn(
        DURATION_MINUTES, F.when(F.col(CATEGORY) == "Movie", F.col(DURATION)).otherwise(None)
    ).withColumn(
        DURATION_SEASONS, F.when(F.col(CATEGORY) == "TV Show", F.col(DURATION)).otherwise(None)
    ).drop(DURATION)

# COMMAND ----------

scraped_streaming_df = scraped_streaming_df.withColumn(RATING, F.col(RATING).cast("double"))
scraped_streaming_df = scraped_streaming_df.withColumn(RATING, F.round(F.col(RATING), 2))

# COMMAND ----------

if BOX_OFFICE in scraped_streaming_df.columns:
    scraped_streaming_df = scraped_streaming_df.withColumn(BOX_OFFICE, F.regexp_replace(BOX_OFFICE, r'^\{|\}$', ""))

    scraped_streaming_df = extract_box_office_data(scraped_streaming_df, BOX_OFFICE)

# COMMAND ----------

if SERIES_YEARS in scraped_streaming_df.columns:
    scraped_streaming_df = split_series_years(scraped_streaming_df, SERIES_YEARS, FIRST_SERIES_YEARS, LAST_SERIES_YEARS)

# COMMAND ----------

if BUDGET in scraped_streaming_df.columns:
    pattern = r'([A-Z$]+[0-9,]+)'
    scraped_streaming_df = scraped_streaming_df.withColumn(BUDGET, F.regexp_extract(F.col(BUDGET), r'([A-Z$]+[0-9,]+)', 1))

# COMMAND ----------

if OPENING_WEEKEND in scraped_streaming_df.columns:
    pattern = r"([A-Z]{2,3}\$?\d{1,3}(?:,\d{3})*|\$\d{1,3}(?:,\d{3})*)"
    scraped_streaming_df = scraped_streaming_df.withColumn(OPENING_WEEKEND, F.regexp_extract(F.col(OPENING_WEEKEND), pattern, 0))

# COMMAND ----------

if CUMULATIVE_WORLDWIDE_GROSS in scraped_streaming_df.columns:
    pattern = r"([A-Za-z]*\$?[0-9,]+)"
    scraped_streaming_df = scraped_streaming_df.withColumn(CUMULATIVE_WORLDWIDE_GROSS, F.regexp_extract(F.col(CUMULATIVE_WORLDWIDE_GROSS), pattern, 1))

# COMMAND ----------

scraped_streaming_df = scraped_streaming_df.drop(BOX_OFFICE)

# COMMAND ----------

columns_to_clean = [BUDGET, OPENING_WEEKEND, CUMULATIVE_WORLDWIDE_GROSS]

if all(column in scraped_streaming_df.columns for column in columns_to_clean):
    for column in columns_to_clean:
        scraped_streaming_df = scraped_streaming_df.withColumn(column, F.regexp_replace(column, ",", ""))

# COMMAND ----------

if all(column in scraped_streaming_df.columns for column in columns_to_clean):
    for col in columns_to_clean:
        currency_col, amount_col = extract_currency_and_amount(col)
        scraped_streaming_df = scraped_streaming_df.withColumn(f"{col}_currency", currency_col)
        scraped_streaming_df = scraped_streaming_df.withColumn(f"{col}_amount", amount_col)
        scraped_streaming_df = scraped_streaming_df.withColumn(f"{col}_eur", convert_to_eur_udf(F.col(f"{col}_currency"), F.col(f"{col}_amount")))


# COMMAND ----------

columns_to_drop = [f"{col}_currency" for col in columns_to_clean] + [f"{col}_amount" for col in columns_to_clean] + columns_to_clean
scraped_streaming_df = scraped_streaming_df.drop(*columns_to_drop)

# COMMAND ----------

columns_to_round = [BUDGET_EUR, OPENING_WEEKEND_EUR, CUMULATIVE_WORLDWIDE_GROSS_EUR]

for col in columns_to_round:
    scraped_streaming_df = scraped_streaming_df.withColumn(col, F.round(col, 0))

# COMMAND ----------

scraped_streaming_df = scraped_streaming_df.filter((F.col(BUDGET_EUR) >= 10) | F.col(BUDGET_EUR).isNull())

# COMMAND ----------

scraped_streaming_df = scraped_streaming_df.dropDuplicates([TITLE, PLATFORM])

# COMMAND ----------

for pattern, replacement in genre_streaming_services_mapping.items():
    case_insensitive_pattern = f"(?i){pattern}"  
    scraped_streaming_df = scraped_streaming_df.withColumn(GENRES, F.regexp_replace(F.col(GENRES), case_insensitive_pattern, replacement))

# COMMAND ----------

scraped_streaming_df = scraped_streaming_df.withColumn("last_updated_date", F.current_date())

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.silver_table_cleaned (
# MAGIC show_id string,
# MAGIC category string,
# MAGIC age_required integer,
# MAGIC title string,
# MAGIC director string,
# MAGIC cast string,
# MAGIC countries string,
# MAGIC date_added date,
# MAGIC release_date date,
# MAGIC genres string,
# MAGIC is_animation string,
# MAGIC platform string,
# MAGIC is_scraped string,
# MAGIC imdb_id string,
# MAGIC rating double,
# MAGIC votes string,
# MAGIC languages string,
# MAGIC color string,
# MAGIC top_250 string,
# MAGIC production_companies string,
# MAGIC writers string,
# MAGIC last_updated_date date,
# MAGIC duration_minutes integer,
# MAGIC duration_seasons integer,
# MAGIC first_series_years integer,
# MAGIC last_series_years integer,
# MAGIC budget_eur double,
# MAGIC opening_weekend_eur double,
# MAGIC cumulative_worldwide_gross_eur double
# MAGIC )

# COMMAND ----------

scraped_streaming_df.createOrReplaceTempView("scraped_streaming_view")

# COMMAND ----------

merge_tables(spark, "silver_db", "silver_table_cleaned", "scraped_streaming_view")