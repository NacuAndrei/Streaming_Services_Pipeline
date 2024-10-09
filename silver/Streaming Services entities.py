# Databricks notebook source
# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/streaming_services_functions"

# COMMAND ----------

silver_streaming_df = spark.read.table("silver_db.silver_table_cleaned")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "all_dates")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

if v_file_date != "all_dates":
    silver_streaming_df = silver_streaming_df.filter(silver_streaming_df.last_updated_date == F.current_date())

# COMMAND ----------

directors_df = create_unique_df(silver_streaming_df, DIRECTOR, DIRECTOR_NAME, DIRECTOR_ID)
actors_df = create_unique_df(silver_streaming_df, CAST, ACTOR_NAME, ACTOR_ID)
countries_df = create_unique_df(silver_streaming_df, COUNTRIES, COUNTRY_NAME, COUNTRY_ID)
platforms_df = create_unique_df(silver_streaming_df, PLATFORM, PLATFORM_NAME, PLATFORM_ID)
genres_df = create_unique_df(silver_streaming_df, GENRES, GENRE_NAME, GENRE_ID)
prod_companies_df = create_unique_df(silver_streaming_df, PRODUCTION_COMPANIES, PROD_COMPANY_NAME, PROD_COMPANY_ID)
writers_df = create_unique_df(silver_streaming_df, WRITERS, WRITER_NAME, WRITER_ID)
languages_df = create_unique_df(silver_streaming_df, LANGUAGES, LANGUAGE_NAME, LANGUAGE_ID)

# COMMAND ----------

shows_and_movies_directors_df = create_association_table(silver_streaming_df, directors_df, DIRECTOR, DIRECTOR_NAME, SHOW_ID, DIRECTOR_ID)
shows_and_movies_actors_df = create_association_table(silver_streaming_df, actors_df, CAST, ACTOR_NAME, SHOW_ID, ACTOR_ID)
shows_and_movies_countries_df = create_association_table(silver_streaming_df, countries_df, COUNTRIES, COUNTRY_NAME, SHOW_ID, COUNTRY_ID)
shows_and_movies_genres_df = create_association_table(silver_streaming_df, genres_df, GENRES, GENRE_NAME, SHOW_ID, GENRE_ID)
shows_and_movies_prod_companies_df = create_association_table(silver_streaming_df, prod_companies_df, PRODUCTION_COMPANIES, PROD_COMPANY_NAME, SHOW_ID, PROD_COMPANY_ID)
shows_and_movies_writers_df = create_association_table(silver_streaming_df, writers_df, WRITERS, WRITER_NAME, SHOW_ID, WRITER_ID)
shows_and_movies_languages_df = create_association_table(silver_streaming_df, languages_df, LANGUAGES, LANGUAGE_NAME, SHOW_ID, LANGUAGE_ID)
shows_and_movies_platforms_df = create_association_table(silver_streaming_df, platforms_df, PLATFORM, PLATFORM_NAME, SHOW_ID, PLATFORM_ID)

# COMMAND ----------

shows_and_movies_df = silver_streaming_df.select(
    SHOW_ID, CATEGORY, AGE_REQUIRED, TITLE, DATE_ADDED, RELEASE_DATE, RATING, VOTES,
    COLOR, TOP_250, IS_ANIMATION, IMDB_ID, DURATION_MINUTES, DURATION_SEASONS,
    FIRST_SERIES_YEARS, LAST_SERIES_YEARS, BUDGET_EUR, OPENING_WEEKEND_EUR, 
    CUMULATIVE_WORLDWIDE_GROSS_EUR
)

# COMMAND ----------

save_to_db2(directors_df, db_name, DIRECTORS)
save_to_db2(actors_df, db_name, ACTORS)
save_to_db2(countries_df, db_name, COUNTRIES)
save_to_db2(genres_df, db_name, GENRES)
save_to_db2(prod_companies_df, db_name, PROD_COMPANIES)
save_to_db2(writers_df, db_name, WRITERS)
save_to_db2(languages_df, db_name, LANGUAGES)
save_to_db2(platforms_df, db_name, PLATFORMS)
save_to_db2(shows_and_movies_df, db_name, "shows_and_movies")

save_to_db2(shows_and_movies_directors_df, db_name, "shows_and_movies_directors")
save_to_db2(shows_and_movies_actors_df, db_name, "shows_and_movies_actors")
save_to_db2(shows_and_movies_countries_df, db_name, "shows_and_movies_countries")
save_to_db2(shows_and_movies_genres_df, db_name, "shows_and_movies_genres")
save_to_db2(shows_and_movies_prod_companies_df, db_name, "shows_and_movies_prod_companies")
save_to_db2(shows_and_movies_writers_df, db_name, "shows_and_movies_writers")
save_to_db2(shows_and_movies_languages_df, db_name, "shows_and_movies_languages")
save_to_db2(shows_and_movies_platforms_df, db_name, "shows_and_movies_platforms")