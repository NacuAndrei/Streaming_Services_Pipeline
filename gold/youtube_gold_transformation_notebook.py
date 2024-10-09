# Databricks notebook source
# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/common_functions"

# COMMAND ----------

# MAGIC %run "../shared/youtube_common_functions"

# COMMAND ----------

# MAGIC %run "../shared/spotify_common_functions"

# COMMAND ----------

# MAGIC %run "../shared/streaming_services_functions"

# COMMAND ----------

yt_videos_df = spark.table('youtube_silver.yt_videos')
yt_trending_df = spark.table('youtube_silver.yt_video_trending')
yt_channels_df = spark.table('youtube_silver.yt_channels')
yt_tags_df = spark.table('youtube_silver.yt_tags')
yt_videos_tags_df = spark.table('youtube_silver.yt_videos_tags')


# COMMAND ----------

joined_tags_df = yt_videos_tags_df \
    .join(yt_tags_df, YT_TAG_ID, 'left') \
    .select(
        YT_VIDEO_ID,
        YT_TAG
    )

joined_stats_df = yt_trending_df \
    .join(yt_videos_df, YT_VIDEO_ID, 'left') \
    .join(yt_channels_df, YT_CHANNEL_ID, 'left') \
    .select(
        YT_VIDEO_ID,
        YT_VIDEO_TITLE,
        YT_CHANNEL_TITLE,
        YT_TRENDING_DATE,
        YT_VIDEO_PUBLISH_TIME,
        YT_COUNTRY_DATASET,
        YT_VIEW_COUNT,
        YT_LIKES,
        YT_DISLIKES,
        YT_COMMENT_COUNT,
        YT_CATEGORY_NAME,
        YT_COMMENTS_DISABLED,
        YT_RATINGS_DISABLED,
        YT_DEFAULT_LANGUAGE,
        YT_DURATION,
        YT_CAPTION,
    )

joined_stats_df = joined_stats_df.withColumnRenamed(YT_VIDEO_PUBLISH_TIME, 'UTC_publish_date')
joined_stats_df = joined_stats_df.withColumn('local_publish_date', convert_to_local_time_udf(F.col('UTC_publish_date'), F.col(YT_COUNTRY_DATASET)))

# COMMAND ----------

joined_videos_tags_df = yt_trending_df \
    .join(yt_videos_df, YT_VIDEO_ID, 'left') \
    .join(joined_tags_df, YT_VIDEO_ID, 'left') \
    .select(
        YT_VIDEO_ID,
        YT_TAG,
        YT_VIEW_COUNT,
        YT_COMMENT_COUNT,
        YT_LIKES,
        YT_DISLIKES,
        YT_TRENDING_DATE,
        YT_COUNTRY_DATASET,
    )

# COMMAND ----------

tags_statistics_df = drop_duplicates_by_ordered_partition(joined_videos_tags_df, partition_by=F.col(YT_VIDEO_ID), order_by=F.col(YT_TRENDING_DATE))

tags_statistics_df = tags_statistics_df \
    .where(F.col(YT_VIEW_COUNT) > 1_000_000) \
    .withColumn('likes_per_view', F.col(YT_LIKES) / F.col(YT_VIEW_COUNT)) \
    .where((F.col(YT_TAG).isNotNull()) & (F.col(YT_TAG) != 'other')) \
    .groupBy(YT_TAG) \
    .agg(
        F.count('*').alias('videos_count'),
        F.mean(YT_VIEW_COUNT).alias('avg_view_count'),
        F.mean(YT_LIKES).alias('avg_likes_count'),
        F.mean('likes_per_view').alias('avg_likes_per_view'),
    ) \
    .where((F.col('videos_count') > 100))


# COMMAND ----------

window_spec = Window.partitionBy(YT_COUNTRY_DATASET).orderBy(F.col('videos_count').desc())

tags_statistics_per_country_df = joined_videos_tags_df \
    .where(F.col(YT_VIEW_COUNT) > 1_000_000) \
    .withColumn('likes_per_view', F.col(YT_LIKES) / F.col(YT_VIEW_COUNT)) \
    .where((F.col(YT_TAG).isNotNull()) & (F.col(YT_TAG) != 'other')) \
    .groupBy(YT_TAG, YT_COUNTRY_DATASET) \
    .agg(
        F.count('*').alias('videos_count'),
        F.mean(YT_VIEW_COUNT).alias('avg_view_count'),
        F.mean(YT_LIKES).alias('avg_likes_count'),
        F.mean('likes_per_view').alias('avg_likes_per_view'),
    ) \
    .withColumn('tag_country_rank', F.row_number().over(window_spec)) \
    .where(F.col('tag_country_rank') <= 25)


# COMMAND ----------

categories_distribution_df = yt_trending_df \
    .join(yt_videos_df, YT_VIDEO_ID, 'left') \
    .select(YT_COUNTRY_DATASET, YT_VIDEO_ID, YT_CATEGORY_NAME, YT_TRENDING_DATE)


videos_per_country = yt_trending_df.groupBy(YT_COUNTRY_DATASET).agg(F.count('*').alias('videos_per_country'))

categories_distribution_df = categories_distribution_df \
    .groupBy(YT_COUNTRY_DATASET, YT_CATEGORY_NAME) \
    .agg(F.count('*').alias('videos_in_category_per_country'))

categories_distribution_df = categories_distribution_df \
    .join(videos_per_country, YT_COUNTRY_DATASET, 'left') \
    .select(
        YT_COUNTRY_DATASET,
        YT_CATEGORY_NAME,
        (F.col('videos_in_category_per_country') / F.col('videos_per_country')).alias('percent_of_country_trending_videos'),
        'videos_in_category_per_country',
    ) \
    .orderBy(YT_COUNTRY_DATASET, YT_CATEGORY_NAME)

global_categories_distribution_df = categories_distribution_df \
    .groupBy(YT_CATEGORY_NAME) \
    .agg(F.sum('videos_in_category_per_country').alias('videos_in_category_per_country')) \
    .withColumn('percent_of_country_trending_videos', F.col('videos_in_category_per_country') / F.sum('videos_in_category_per_country').over(Window.partitionBy())) \
    .withColumn(YT_COUNTRY_DATASET, F.lit('ALL'))

categories_distribution_df = categories_distribution_df.unionByName(global_categories_distribution_df)

# COMMAND ----------



categories_monthly_distribution_df = yt_trending_df \
    .join(yt_videos_df, YT_VIDEO_ID, 'left') \
    .select(YT_VIDEO_ID, YT_TRENDING_DATE, YT_CATEGORY_NAME, YT_COUNTRY_DATASET)

categories_monthly_distribution_df = aggregate_category_distribution_over_time_periods(categories_monthly_distribution_df, [YT_CATEGORY_NAME, YT_COUNTRY_DATASET], YT_TRENDING_DATE, 'month') \
    .withColumnRenamed('count_per_month', 'videos_in_category_per_month')

# COMMAND ----------

save_to_db(joined_stats_df, 'streaming_youtube_spotify_gold', 'joined_youtube_stats', overwrite_schema=True)
save_to_db(joined_tags_df, 'streaming_youtube_spotify_gold', 'joined_youtube_tags', overwrite_schema=True)
save_to_db(tags_statistics_df, 'streaming_youtube_spotify_gold', 'top_tags_stats_merged', overwrite_schema=True)
save_to_db(tags_statistics_per_country_df, 'streaming_youtube_spotify_gold', 'top_tags_stats_per_country', overwrite_schema=True)
save_to_db(categories_distribution_df, 'streaming_youtube_spotify_gold', 'youtube_categories_distribution', overwrite_schema=True)
save_to_db(categories_monthly_distribution_df, 'streaming_youtube_spotify_gold', 'youtube_monthly_categories_distribution', overwrite_schema=True)