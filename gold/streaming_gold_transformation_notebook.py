# Databricks notebook source
# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/common_functions"

# COMMAND ----------

# MAGIC %run "../shared/youtube_common_functions"

# COMMAND ----------

# MAGIC %run "../shared/streaming_services_functions"

# COMMAND ----------

shows_and_movies_df = spark.table("streaming_services_silver.shows_and_movies")
shows_and_movies_directors_df = spark.table("streaming_services_silver.shows_and_movies_directors")
shows_and_movies_actors_df = spark.table("streaming_services_silver.shows_and_movies_actors")
shows_and_movies_countries_df = spark.table("streaming_services_silver.shows_and_movies_countries")
shows_and_movies_genres_df=spark.table("streaming_services_silver.shows_and_movies_genres")
shows_and_movies_prod_companies_df = spark.table("streaming_services_silver.shows_and_movies_prod_companies")
shows_and_movies_writers_df = spark.table("streaming_services_silver.shows_and_movies_writers")
shows_and_movies_languages_df = spark.table("streaming_services_silver.shows_and_movies_languages")
shows_and_movies_platforms_df = spark.table("streaming_services_silver.shows_and_movies_platforms")
genres_df = spark.table("streaming_services_silver.genres")
platforms_df = spark.table("streaming_services_silver.platforms")
countries_df = spark.table("streaming_services_silver.countries")

# COMMAND ----------

shows_with_genres_df = shows_and_movies_df.join(
    shows_and_movies_genres_df, 
    on="show_id", 
    how="inner"
)

shows_with_genre_name_df = shows_with_genres_df.join(
    genres_df, 
    on="genre_id", 
    how="inner"
)

shows_and_movies_by_genre_df = shows_with_genre_name_df.groupBy("genre_name").agg(
    F.count("*").alias("count_of_shows_movies"),
    F.sum("cumulative_worldwide_gross_eur").alias("total_gross_eur"),
    F.round(F.avg("rating"), 2).alias("average_rating"),
    F.round(F.avg("votes"), 2).alias("average_votes")
)

#display(shows_and_movies_by_genre_df)

# COMMAND ----------

shows_with_platform_df = shows_and_movies_df.join(
    shows_and_movies_platforms_df, 
    on="show_id", 
    how="inner"
)

shows_with_platform_name_df = shows_with_platform_df.join(
    platforms_df, 
    on="platform_id", 
    how="inner"
)

shows_and_movies_by_platform_df = shows_with_platform_name_df.groupBy("platform_name").agg(
    F.count("*").alias("count_of_shows_movies"),
    F.sum("cumulative_worldwide_gross_eur").alias("total_gross_eur"),
    F.round(F.avg("rating"), 2).alias("average_rating"),
    F.round(F.avg("votes"), 2).alias("average_votes")
)

#display(shows_and_movies_by_platform_df)

# COMMAND ----------

shows_with_countries_df = shows_and_movies_df.join(
    shows_and_movies_countries_df,
    on="show_id",
    how="inner"
)

shows_with_country_name_df = shows_with_countries_df.join(
    countries_df,
    on="country_id",
    how="inner"
)

shows_and_movies_by_country_df = shows_with_country_name_df.groupBy("country_name", "category").agg(
    F.count("*").alias("count_of_shows_or_movies")
)

shows_and_movies_by_country_pivot_df = shows_and_movies_by_country_df.groupBy("country_name").pivot("category").agg(
    F.sum("count_of_shows_or_movies")
)

shows_and_movies_by_country_pivot_df = shows_and_movies_by_country_pivot_df.withColumnRenamed("Movie", "count_of_movies").withColumnRenamed("TV Show", "count_of_tv_shows")

shows_and_movies_by_country_pivot_df = shows_and_movies_by_country_pivot_df.withColumn(
    "count_of_both",
    F.coalesce(F.col("count_of_movies"), F.lit(0)) + F.coalesce(F.col("count_of_tv_shows"), F.lit(0))
)

#display(shows_and_movies_by_country_pivot_df)

# COMMAND ----------

save_to_db(shows_and_movies_by_genre_df, "streaming_youtube_spotify_gold","shows_and_movies_by_genre_stats", overwrite_schema=True)
save_to_db(shows_and_movies_by_platform_df, "streaming_youtube_spotify_gold","shows_and_movies_by_platform_stats", overwrite_schema=True)
save_to_db(shows_and_movies_by_country_pivot_df, "streaming_youtube_spotify_gold","shows_and_movies_by_country_stats", overwrite_schema=True)