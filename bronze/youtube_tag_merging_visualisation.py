# Databricks notebook source
# MAGIC %run "../shared/common_functions"

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

youtube_df = read_kaggle_csv(
    f'{STREAMING_YT_SPOTIFY_BRONZE_PATH}/youtube_raw/US_youtube_trending_data/',
     schema = YOUTUBE_RAW_SCHEMA)

youtube_df = convert_bool_columns_to_int(youtube_df)

youtube_df = youtube_df.dropDuplicates([YT_VIDEO_ID])

# COMMAND ----------

youtube_df = youtube_df.withColumnsRenamed({
    'video_id': YT_VIDEO_ID,
    'title': YT_VIDEO_TITLE,
    'publishedAt': YT_VIDEO_PUBLISH_TIME,
    'channelId': YT_CHANNEL_ID,
    'channelTitle': YT_CHANNEL_TITLE,
    'categoryId': YT_CATEGORY_ID,
    'trending_date': YT_TRENDING_DATE,
    'view_count': YT_VIEW_COUNT,
    'likes': YT_LIKES,
    'dislikes': YT_DISLIKES,
    'comment_count': YT_COMMENT_COUNT,
    'thumbnail_link': YT_THUMBNAIL_LINK,
    'comments_disabled': YT_COMMENTS_DISABLED,
    'ratings_disabled': YT_RATINGS_DISABLED,
    'category_name': YT_CATEGORY_NAME,
    'country_dataset': YT_COUNTRY_DATASET
})

# COMMAND ----------

youtube_df.display()
print(youtube_df.count())

# COMMAND ----------

youtube_df = replace_value_in_column(youtube_df, 'tags', '[None]', None)

# COMMAND ----------

tags_df = explode_string_column(youtube_df, 'tags', '\|', YT_TAG, keep_old_column = True) \
    .select(YT_TAG, YT_VIDEO_ID)

tags_df = tags_df \
    .withColumn(YT_TAG, F.lower(col(YT_TAG))) \
    .withColumn('main_tag', col(YT_TAG)) \
    .where(col(YT_TAG) != '')

# COMMAND ----------

main_tags_map = {
    r'gaming|gameplay|gamer': 'gaming',
    r'video gam[e|ing]': 'gaming',
    r'\bmine ?craft\b': 'minecraft',
    r'\b(comedy|funny|skit|laugh|humou?r|prank)\b': 'comedy',
    r'\bvlogs?\b': 'vlog',
    r'\bnews?\b': 'news',
    'trailer': 'trailer',
    'movie': 'movie',
    'music': 'music',
    'cooking': 'food',
    'food': 'food',
    'challenge': 'challenge',
    'animation': 'animation',
    r'\bsports?\b': 'sports',
    r'\be[ -]?sports'
    'pewdiepie': 'pewdiepie',
    r'you ?tube': 'youtube',
    'fashion': 'fashion',
    r'\bpolitic(s|al)?\b': 'politics',
    'basketball': 'basketball',
    r'\bnba\b': 'nba',
    r'\bnfl\b': 'nfl',
    r'\bfort[ -]?nite\b': 'fortnite',
    r'\bmarvel\b': 'marvel',
    'soccer': 'soccer',
    r'foot[ -]?ball': 'football',#TODO: DISCUSS AMERICAN FOOTBALL / SOCCER NAMING CONFLICT
    r'\bcar\b': 'car',
    r'\brap\b': 'rap',
    r'\brapper?s\b': 'rap',
    r'\bscience?s\b': 'sciencce',
    r'\beducation\b': 'education',
    r'\b(streaming|twitch)\b': 'streaming',
    r'\bmma\b': 'mma',  #TODO DISCUSS IF MMA AND WRESTLING SHOULD BE MERGED
    r'\bwrestling\b':'wrestling',
    r'\bpodcast?s\b': 'podcast',
    r'\bsmosh\b': 'smosh',
    r'\bdocumentary\b': 'documentary',
    r'\binterview?s\b': 'interview',
    r'\bcommentary\b': 'commentary',
    r'\breaction?s\b': 'reaction',
    r'\btutorial?s\b': 'tutorial',
    r'\bbts\b': 'bts',
    r'\btennis\b': 'tennis',
    r'\bgrand ?slam\b': 'tennis',
    r'\bgame dev\b': 'gamedev',
    r'\bviral\b': 'viral',
    r'\bdisney\b': 'disney',
    r'\bmeme\b': 'meme', #TODO maybe merge with comedy
    r'\bmr\s?beast\b': 'mrbeast',
    r'\bchess\b': 'chess',
    r'\banime\b': 'anime',
    r'\bhorror\b': 'horror',
    r'\bacting\b': 'acting',
    r'\bdanc(e|ing)\b': 'dancing', #TODO: dance
    r'\b(family|child|kid)[ -]?friendly\b': 'familyfriendly',
    r'lgbt|\bgay\b|\btrans\b|\bqueer\b': 'lgbt',
    r'\bamon?g ?us': 'among us',
    r'skibidi': 'skibidi',
    r'\bhistory\b': 'history',
    r'\bcelebrity\b': 'celebrity',
    }

for main_tag in main_tags_map.keys():
    tags_df = tags_df.withColumn('main_tag', F.when(col(YT_TAG).rlike(main_tag), F.lit(main_tags_map[main_tag])).otherwise(col('main_tag')))

# COMMAND ----------

tags_count_df = group_by_count(tags_df, 'main_tag') \
    .orderBy('main_tag_count', ascending = False)

tags_count_df.display()
tags_count_df.count()

# COMMAND ----------

category_candidate = r'\bnature\b'
tags_count_df.where(col('main_tag').rlike(category_candidate)).display()

# COMMAND ----------

top_tags_to_select = 1000

most_common_tags_df = tags_count_df.limit(top_tags_to_select)
most_common_tags = [tag['main_tag'] for tag in most_common_tags_df.collect()]

most_common_tags_df.display()

# COMMAND ----------

tags_videos_df = tags_df.withColumn('top_x_tag', F.when(F.col('main_tag').isin(most_common_tags), F.lit(1)).otherwise(F.lit(0)))

# COMMAND ----------

tags_videos_aggregated_df = tags_videos_df \
    .groupBy(YT_VIDEO_ID) \
    .agg(
        count('*').alias('tags_count'),
        F.sum('top_x_tag').alias('top_x_tag_count'),
        when(F.col('top_x_tag_count') > 0, 1).otherwise(0).alias('has_top_x_tag'))

tags_videos_aggregated_df.display()


total_videos = youtube_df.count()
original_videos_with_no_tags_percent = null_tags_count = youtube_df.where(F.col('tags').isNull()).count() / total_videos
average_tags_per_video = tags_videos_aggregated_df.select(F.sum('tags_count')).collect()[0][0] / total_videos
average_top_x_tags_per_video = tags_videos_aggregated_df.select(F.sum('top_x_tag_count')).collect()[0][0] / total_videos
video_with_top_x_tag_count = tags_videos_aggregated_df.select(F.sum('has_top_x_tag')).collect()[0][0]

print(f'Original average tags per video: {average_tags_per_video}')
print(f'Original percentage of videos with tags: {1 - original_videos_with_no_tags_percent}')
print(f'Condensed average tags per video: {average_top_x_tags_per_video}')
print(f'Percentage of videos with condensed tags: {video_with_top_x_tag_count/total_videos}')

# COMMAND ----------

tags_videos_aggregated_df.groupBy('tags_count').agg(count('*')).orderBy('tags_count', descending=True).display()

# COMMAND ----------

count_na(youtube_df).display()