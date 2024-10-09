# Databricks notebook source
# MAGIC %run "../shared/streaming_services_functions"

# COMMAND ----------

# MAGIC %run "../shared/constants"

# COMMAND ----------

# MAGIC %run "../shared/spotify_common_functions"

# COMMAND ----------

genres_df = spark.read.table("silver_spotify_bbq.genres")
artists_df = spark.read.table("silver_spotify_bbq.artists")
artists_genres_df = spark.read.table("silver_spotify_bbq.artists_genres")

tracks_stats_df = spark.read.table("silver_spotify_bbq.tracks_stats")
tracks_artists_df = spark.read.table("silver_spotify_bbq.tracks_artists")
tracks_df = spark.read.table("silver_spotify_bbq.tracks") 

# COMMAND ----------

updated_genres_df = genres_df.withColumn(MAPPED_GENRE, map_genre_udf(F.col(NAME_GENRE)))

distinct_genres_df = updated_genres_df.select(MAPPED_GENRE).distinct()
distinct_genres_df = distinct_genres_df.withColumn(ID_GENERAL_GENRE, F.monotonically_increasing_id())

# COMMAND ----------

final_genres_df = updated_genres_df.join(distinct_genres_df, on=MAPPED_GENRE).select(ID_GENRE, NAME_GENRE, ID_GENERAL_GENRE, MAPPED_GENRE)

# COMMAND ----------

artist_general_genre_df = artists_genres_df.join(
    final_genres_df,
    artists_genres_df.id_genre == final_genres_df.id_genre,
    how="inner"
).select(ID_ARTIST, ID_GENERAL_GENRE, MAPPED_GENRE).distinct()


# COMMAND ----------

artist_with_general_genre_df = artist_general_genre_df.join(
    artists_df,
    artist_general_genre_df.id_artist == artists_df.id_artist,
    how="inner"
).select(
    artist_general_genre_df.id_artist.alias(ID_ARTIST),
    artists_df.name_artist.alias(NAME_ARTIST),
    artist_general_genre_df.id_general_genre.alias(ID_GENERAL_GENRE),
    artist_general_genre_df.mapped_genre.alias(MAPPED_GENRE)
)

# COMMAND ----------

tracks_stats_df = tracks_stats_df.select(ID_TRACK, STREAMS, PLAYLIST_COUNT, POPULARITY_TRACK, SPOTIFY_SOURCE_DATASET)
tracks_df = tracks_df.select(ID_TRACK, NAME_TRACK, ISRC, BPM, KEY, MODE, RELEASE_DATE, DANCEABILITY, ENERGY, ACOUSTICNESS, INSTRUMENTALNESS, LIVENESS, SPEECHINESS)
tracks_artists_df = tracks_artists_df.select(ID_TRACK, ID_ARTIST, IS_MAIN_ARTIST)
artists_df = artists_df.select(ID_ARTIST, NAME_ARTIST, COUNTRY, BIRTH_DATE, TYPE)
artists_genres_df = artist_with_general_genre_df.select(ID_ARTIST, ID_GENERAL_GENRE).distinct()
genres_df = artist_with_general_genre_df.select(ID_GENERAL_GENRE, MAPPED_GENRE).distinct()

# COMMAND ----------

artists_genres_df = artists_genres_df.withColumnRenamed(ID_GENERAL_GENRE, ID_GENRE)
genres_df = genres_df.withColumnRenamed(ID_GENERAL_GENRE, ID_GENRE) \
    .withColumnRenamed(MAPPED_GENRE, NAME_GENRE)

# COMMAND ----------

db_name = "spotify_silver"

# COMMAND ----------

save_to_db(tracks_stats_df, db_name, "tracks_stats", True)
save_to_db(tracks_df, db_name, "tracks", True)
save_to_db(tracks_artists_df, db_name, "tracks_artists", True)
save_to_db(artists_df, db_name, "artists", True)
save_to_db(artists_genres_df, db_name, "artists_genre", True)
save_to_db(genres_df, db_name, "genres", True)

# COMMAND ----------

