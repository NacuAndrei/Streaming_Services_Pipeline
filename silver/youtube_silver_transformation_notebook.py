# Databricks notebook source
# MAGIC %run "../shared/common_functions"

# COMMAND ----------

# MAGIC %run "../shared/youtube_common_functions"

# COMMAND ----------

# MAGIC %run "../shared/kaggle_ingestion_functions"

# COMMAND ----------

# MAGIC %run "../shared/constants"

# COMMAND ----------

# MAGIC %run "../shared/config"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

stats_df = read_raw_merged_youtube_csvs()

# COMMAND ----------

videos_df = youtube_initial_cleaning(stats_df)
videos_df = drop_duplicates_by_ordered_partition(videos_df, partition_by = F.col(YT_VIDEO_ID), order_by = F.col(YT_TRENDING_DATE).desc())

scraped_data_path = f'{STREAMING_YT_SPOTIFY_BRONZE_PATH}/temp_youtube_scraped/merged_youtube_scraped/'
scraped_videos_df = read_merged_scraped_youtube_df_with_checkpoints(scraped_data_path)

videos_df = videos_df.join(
    scraped_videos_df,
    videos_df[YT_VIDEO_ID] == scraped_videos_df[YT_VIDEO_ID],
    'inner'
) \
    .select(
        videos_df[YT_VIDEO_ID],
        videos_df[YT_CHANNEL_ID],
        videos_df[YT_VIDEO_TITLE],
        videos_df[YT_CHANNEL_TITLE],
        videos_df[YT_VIDEO_PUBLISH_TIME],
        videos_df[YT_CATEGORY_NAME],
        videos_df[YT_THUMBNAIL_LINK],
        videos_df[YT_COMMENTS_DISABLED],
        videos_df[YT_RATINGS_DISABLED],
        scraped_videos_df[YT_TAGS],
        scraped_videos_df[YT_IS_SCRAPED],
        scraped_videos_df[YT_DEFAULT_LANGUAGE],
        scraped_videos_df[YT_DURATION],
        scraped_videos_df[YT_CAPTION],
        scraped_videos_df[YT_LICENSED_CONTENT],
        scraped_videos_df[YT_REGION_RESTRICTION],
        scraped_videos_df[YT_CONTENT_RATING],
    )

videos_df = videos_df.withColumn(YT_DURATION, iso8601_to_time_udf(YT_DURATION))
videos_df = convert_string_boolean_to_int(videos_df, 'caption')
videos_df = convert_bool_columns_to_int(videos_df)


# COMMAND ----------

yt_region_restrictions = videos_df \
    .select(
        YT_VIDEO_ID,
        F.explode(YT_REGION_RESTRICTION).alias('restriction_type', 'regions_list'),
        F.explode('regions_list').alias('country_code')) \
    .drop('regions_list') \
    .dropDuplicates()

save_to_db(yt_region_restrictions, 'youtube_silver', 'yt_region_restrictions', overwrite_schema = True)
yt_region_restrictions = spark.table('youtube_silver.yt_region_restrictions')

# COMMAND ----------

stats_df = stats_df.select(
    YT_VIDEO_ID,
    YT_TRENDING_DATE,
    YT_COUNTRY_DATASET,
    YT_VIEW_COUNT,
    YT_LIKES,
    YT_DISLIKES,
    YT_COMMENT_COUNT,
)

save_to_db(stats_df, 'youtube_silver', 'yt_video_trending', overwrite_schema = True)
stats_df = spark.table('youtube_silver.yt_video_trending')

# COMMAND ----------

channels_df = videos_df \
    .select(
    YT_CHANNEL_ID,
    YT_CHANNEL_TITLE
    ) \
    .dropDuplicates([YT_CHANNEL_ID])
 
save_to_db(channels_df, 'youtube_silver', 'yt_channels', overwrite_schema = True)
channels_df = spark.table('youtube_silver.yt_channels')

# COMMAND ----------

videos_tags_df = videos_df.select(
    YT_VIDEO_ID,
    YT_TAGS,
)

# COMMAND ----------

videos_df = videos_df.select(
    YT_VIDEO_ID,
    YT_CHANNEL_ID,
    YT_VIDEO_TITLE,
    YT_VIDEO_PUBLISH_TIME,
    YT_CATEGORY_NAME,
    YT_THUMBNAIL_LINK,
    YT_COMMENTS_DISABLED,
    YT_RATINGS_DISABLED,
    YT_IS_SCRAPED,
    YT_DEFAULT_LANGUAGE,
    YT_DURATION,
    YT_CAPTION,
    YT_LICENSED_CONTENT,
    YT_CONTENT_RATING,
)


# COMMAND ----------

save_to_db(videos_df, 'youtube_silver', 'yt_videos', overwrite_schema = True)
videos_df = spark.table('youtube_silver.yt_videos')

# COMMAND ----------

df_translations = spark.read \
    .csv("/FileStore/tables/translated_tags.csv", header=True, inferSchema=True) \
    .select(
        F.col('normalized_tag').alias('original_tag'),
        F.col('english_translation').alias('translated_tag'),
    )

# COMMAND ----------

videos_tags_df = videos_tags_df.withColumn(YT_TAG, F.explode(YT_TAGS)).drop(YT_TAGS)
videos_tags_df = videos_tags_df.withColumn(YT_TAG, F.lower(YT_TAG)).dropDuplicates()
videos_tags_df = videos_tags_df \
    .join(df_translations, videos_tags_df[YT_TAG] == df_translations['original_tag'], 'left') \
    .select(
        YT_VIDEO_ID,
        F.col('translated_tag').alias(YT_TAG),
        )
    
videos_tags_df = videos_tags_df.na.fill(YOUTUBE_OTHER_TAG_PLACEHOLDER)
videos_tags_df = videos_tags_df.dropDuplicates([YT_VIDEO_ID, YT_TAG])
videos_tags_df = videos_tags_df.cache()

# COMMAND ----------

tags_df = videos_tags_df.select(YT_TAG).distinct()
tags_df = tags_df.withColumn('main_tag', F.col(YT_TAG))
for main_tag in CONDENSE_YOUTUBE_TAGS_MAP.keys():
    tags_df = tags_df.withColumn('main_tag', F.when(F.col(YT_TAG).rlike(main_tag), F.lit(CONDENSE_YOUTUBE_TAGS_MAP[main_tag])).otherwise(F.col('main_tag')))

# COMMAND ----------

window_spec = Window.orderBy('main_tag')
tags_df = tags_df \
    .withColumn(YT_TAG_ID, F.dense_rank().over(window_spec)).cache()

# COMMAND ----------

videos_tags_df = videos_tags_df \
    .join(tags_df, videos_tags_df[YT_TAG] == tags_df[YT_TAG], "left") \
    .select(
        videos_tags_df[YT_VIDEO_ID],
        tags_df[YT_TAG_ID],
        ) \
    .dropDuplicates()

# COMMAND ----------

save_to_db(videos_tags_df, 'youtube_silver', 'yt_videos_tags', overwrite_schema = True)
videos_tags_df = spark.table('youtube_silver.yt_videos_tags')

# COMMAND ----------

tags_df = tags_df \
    .select(
        YT_TAG_ID,
        F.col('main_tag').alias(YT_TAG)) \
    .dropDuplicates()

# COMMAND ----------

save_to_db(tags_df, 'youtube_silver', 'yt_tags', overwrite_schema = True)
tags_df = spark.table('youtube_silver.yt_tags')