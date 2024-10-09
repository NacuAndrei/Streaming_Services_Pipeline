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

shows_and_movies_path = ""
youtube_videos_path = ""
youtube_videos_stats_path = ""

# COMMAND ----------

shows_and_movies_df = spark.read.format("delta").load(shows_and_movies_path)
yt_videos_df = spark.read.format("delta").load(youtube_videos_path)
yt_videos_stats_df = spark.read.format("delta").load(youtube_videos_stats_path)

# COMMAND ----------

shows_and_movies_df = shows_and_movies_df.withColumnRenamed("title", "streaming_title")
yt_videos_df = yt_videos_df.withColumnRenamed("title", "video_title")

# COMMAND ----------

from pyspark.sql.functions import col, lower, regexp_replace, length, udf, max

def join_trailers(streaming_df, youtube_df):

    terms_to_remove = [
        "gameplay", "xbox", "valorant", "reaction", "call of duty",
        "league of legends", "mortal kombat", "resident evil", "diablo", "sims", "street fighter", "mario kart",
        "apex legends", "the legend of zelda", "tekken", "nintendo", "music video", "mv trailer", "RÉACTION", "overwatch", "super bowl", "game", "react", "legendado", "explicacion", "announcement", "Anúncio", "launch", "global trailer", "cities skylines", "announce", "mobile legends", "comeback trailer", "genshin impact", "reaccion", "among us", "ps5", "games", "breakdown", "justin bieber"
    ]

    categories_to_exclude = ["Gaming"]
    
    pattern = "(?i)\\b(" + "|".join(terms_to_remove) + ")\\b"
    
    youtube_filtered_df = youtube_df.filter(
        (lower(col("video_title")).like("%trailer%")) &
        (~col("video_title").rlike(pattern)) &
        (~lower(col("category_name")).isin([category.lower() for category in categories_to_exclude]))
    )


    streaming_df_clean = streaming_df.select("show_id", "streaming_title","votes", "cumulative_worldwide_gross_eur")

    cross_joined_df = streaming_df_clean.crossJoin(youtube_filtered_df)
    
    joined_df = cross_joined_df.filter(
        lower(col("video_title")).contains(lower(col("streaming_title")))
    )
    
    filtered_df = joined_df.filter(
        length(col("streaming_title")) > 5
    )

    final_df = filtered_df.select("show_id", "streaming_title", "video_id", "video_title","votes", "cumulative_worldwide_gross_eur")
    
    return final_df


# COMMAND ----------

result_df = join_trailers(shows_and_movies_df, yt_videos_df)

# COMMAND ----------

from pyspark.sql.types import BooleanType
import re

def exact_match(video_title, streaming_title):
    pattern = r'\b' + re.escape(streaming_title.lower()) + r'\b'
    return bool(re.search(pattern, video_title.lower()))

exact_match_udf = udf(exact_match, BooleanType())

def filter_exact_matches(joined_df, yt_videos_stats_df):
    latest_stats_df = yt_videos_stats_df.groupBy("video_id").agg(
        max("trending_date").alias("latest_trending_date"),
        max("view_count").alias("view_count"),
        max("likes").alias("likes"),
        max("dislikes").alias("dislikes"),
        max("comment_count").alias("comment_count")
    )

    exact_match_df = joined_df.filter(
        exact_match_udf(col("video_title"), col("streaming_title"))
    )

    final_df = exact_match_df.select("show_id", "streaming_title", "video_id", "video_title","votes", "cumulative_worldwide_gross_eur")

    final_with_view_count_df = final_df.join(latest_stats_df, on="video_id", how="left")

    final_with_view_count_df = final_with_view_count_df.select(
        "show_id", "streaming_title", "video_id", "video_title", "view_count", "likes", "dislikes", "comment_count", "votes", "cumulative_worldwide_gross_eur"
    )
    
    final_count = final_with_view_count_df.count()

    print(f"Filtered Final Exact Match Record Count: {final_count}")

    return final_with_view_count_df

joined_df = join_trailers(shows_and_movies_df, yt_videos_df)

final_with_view_count_df = filter_exact_matches(joined_df, yt_videos_stats_df)

#display(final_with_view_count_df)

# COMMAND ----------

def filter_unique_show_id(final_with_view_count_df):
    windowed_df = final_with_view_count_df.groupBy("show_id").agg(
        max("view_count").alias("max_view_count")
    )

    filtered_df = final_with_view_count_df.join(windowed_df, on="show_id") \
        .filter(col("view_count") == col("max_view_count")) \
        .drop("max_view_count") 

    final_count = filtered_df.count()
    print(f"Final Unique Show ID Record Count: {final_count}")

    return filtered_df

final_unique_df = filter_unique_show_id(final_with_view_count_df)

#display(final_unique_df)

# COMMAND ----------

save_to_db(final_unique_df, 'streaming_youtube_spotify_gold', 'youtube_streaming_trailers', overwrite_schema=True)