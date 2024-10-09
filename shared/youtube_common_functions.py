# Databricks notebook source
# MAGIC %run "../shared/constants"

# COMMAND ----------

# MAGIC %run "../shared/common_functions"

# COMMAND ----------

# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "./kaggle_ingestion_functions"

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import math
from time import time
from tqdm import tqdm
from pyspark.sql.window import Window

# COMMAND ----------

def get_youtube_columns_from_video_object(item: dict) -> dict:
    '''Extracts column fields from YouTube API response item'''
    return {
        'video_id': item.get('id', None),
        'tags': item['snippet'].get('tags', None),
        'default_language': item['snippet'].get('defaultLanguage', None),
        'duration': item['contentDetails'].get('duration', None),
        'caption': item['contentDetails'].get('caption', None),
        'licensed_content': item['contentDetails'].get('licensedContent', None),
        'region_restriction': item['contentDetails'].get('regionRestriction', None),
        'content_rating': item['contentDetails'].get('contentRating', None)
    }

# COMMAND ----------

def get_youtube_columns_from_api_response_list(videos) -> list[dict]:
    '''Extracts column fields from YouTube API response list.'''
    return [get_youtube_columns_from_video_object(item.to_dict()) for item in videos.items]

# COMMAND ----------

def scrape_youtube_videos_into_dataframe(video_ids: list[str]) -> DataFrame:
    '''Search YouTube API for video_ids and return a dataframe with the results.'''
    api_response = YOUTUBE_API_CLIENT.get_video_by_id(video_id=video_ids, parts = YOUTUBE_PARTS)
    response_dict_list = get_youtube_columns_from_api_response_list(api_response)
    return spark.createDataFrame(response_dict_list, schema=SCRAPED_NEW_COLUMNS_SCHEMA) \
        .withColumn(YT_IS_SCRAPED, F.lit(1))

# COMMAND ----------

def add_not_found_rows(dataframe: DataFrame, searched_ids: list[str]) -> DataFrame:
    '''Find rows that were not found in the API response and add them to the dataframe. Return modified DataFrame.'''
    found_ids = set(dataframe.select(YT_VIDEO_ID).rdd.flatMap(lambda x: x).collect())
    not_found_ids = list(set(searched_ids) - set(found_ids))
    not_found_df = spark.createDataFrame(
        [(id, -1) for id in not_found_ids],
        schema = T.StructType([
            T.StructField(YT_VIDEO_ID, T.StringType()),
            T.StructField(YT_IS_SCRAPED, T.IntegerType())]))
    
    return dataframe.unionByName(not_found_df, allowMissingColumns = True)

# COMMAND ----------

def scrape_youtube_data(dataframe: DataFrame, limit = None, cache_period = None) -> DataFrame:
    '''
    Scrape YouTube API to fill in dataframe missing columns.
    
    Parameters:
        dataframe: DataFrame to be modified.
        limit: Maximum number of rows to scrape.
        cache_period: Cache dataframe after every 'cache_period' batches.

    Returns:
        modified DataFrame with new columns.
    '''
    dataframe = ensure_column_existence(dataframe, YT_IS_SCRAPED, T.IntegerType(), 0)
    dataframe = merge_schema_into_dataframe(dataframe, SCRAPED_NEW_COLUMNS_SCHEMA)
    
    not_scraped_df = dataframe.where(F.col(YT_IS_SCRAPED) == 0).select(YT_VIDEO_ID)
    not_scraped_ids = not_scraped_df.rdd.flatMap(lambda x: x).collect()
    not_scraped_count = not_scraped_df.count() if limit is None else limit

    for i in tqdm(range(0, not_scraped_count, 50)):
        batch_ids = not_scraped_ids[i:i+50]

        batch_scraped_df = scrape_youtube_videos_into_dataframe(batch_ids)
        abtch_scraped_df = add_not_found_rows(batch_scraped_df, batch_ids)
        batch_scraped_df = add_prefix_to_column_names(batch_scraped_df, 'scraped_')

        dataframe = dataframe.join(
            batch_scraped_df,
            dataframe[YT_VIDEO_ID] == batch_scraped_df[f'scraped_{YT_VIDEO_ID}'],
            'left'
            )

        dataframe = coalesce_prefixed_columns(dataframe, 'scraped_', SCRAPED_NEW_COLUMNS_SCHEMA.fieldNames())

        batch_number = i // 50
        if cache_period and batch_number % cache_period == 0:
            dataframe = dataframe.cache()
    return dataframe

# COMMAND ----------

def youtube_initial_cleaning(dataframe: DataFrame) -> DataFrame:
    '''Perform initial cleaning operations on YouTube dataset. Return resulting DataFrame.
    
    - rename columns according to schema
    - ensure tags column is formatted as a list of strings for future transformations
    - truncate date columns to contain only year, month and day information
    - drop duplicate trending entries of videos in the same dataset and same date, keeping the one with the highest view count
    - ensure boolean columns are converted to integers for future transformations
    '''
    dataframe = dataframe.withColumnsRenamed(YT_RENAME_COLUMNS_DICT)
    dataframe = ensure_list_column(dataframe, YT_TAGS, '\|', '[None]')    
    # dataframe = truncate_string_columns(dataframe, [YT_VIDEO_PUBLISH_TIME, YT_TRENDING_DATE], 1, 10)
    dataframe = drop_duplicates_by_ordered_partition(dataframe, partition_by=[YT_VIDEO_ID, YT_TRENDING_DATE, YT_COUNTRY_DATASET], order_by=YT_VIEW_COUNT)
    dataframe = convert_bool_columns_to_int(dataframe)
    return dataframe


# COMMAND ----------

def scrape_youtube_dataset_batched(dataframe: DataFrame, save_location: str, batch_size: int = 500, vacuum_period: int = 5) -> DataFrame:
    '''
    Scrape YouTube API for each row in dataframe and save the results to the given save location.

    Parameters: 
        dataframe: the DataFrame to scrape.
        save_location: the location to save the scraped dataframe checkpoints after each batch.
        batch_size: the number of rows to scrape for each batch.
        vacuum_period: the number of batches to wait before vacuuming the save location.
    '''
    
    dataframe = ensure_column_existence(dataframe, YT_IS_SCRAPED, T.IntegerType(), 0)
    not_scraped_rows = dataframe.where(F.col(YT_IS_SCRAPED) == 0).count()
    batches = math.ceil(not_scraped_rows / batch_size)

    if batches == 0:
        print(f'No rows to scrape')
    else:
        print(f'Scraping merged dataset for {not_scraped_rows} rows:')

    for batch in range(batches):
        start = time.time()
        print(f'Scraping batch {batch+1}/{batches} of size {batch_size}')
        dataframe = scrape_youtube_data(dataframe, save_location, limit = batch_size, cache_period = 1)
        
        print('Saving checkpoint...')
        dataframe \
            .write.format('delta')\
            .mode('overwrite')\
            .option('overwriteSchema', 'true') \
            .save(save_location)
        dataframe = spark.read.format('delta').load(save_location)
        end = time.time()

        batch_processing_time_minutes_seconds = format_seconds_and_minutes(end-start)
        print(f'Batch finished in {batch_processing_time_minutes_seconds} at {get_current_localized_timestamp()}\n')

        if (batch + 1) % vacuum_period == 0:
            spark.sql(f"VACUUM delta.`{save_location}` RETAIN 0 HOURS")
    return dataframe

# COMMAND ----------

def read_merged_scraped_youtube_df_with_checkpoints(merged_temp_save_location: str) -> DataFrame:
    '''Check if there is a checkpoint for the merged dataset. If so, load it. Otherwise, read the individual datasets and merge them. Return the merged DataFrame.'''
    if check_path_exists(merged_temp_save_location):
        merged_df =  spark.read.format('delta').load(merged_temp_save_location)
    else:
        merged_df = read_youtube_datasets_checkpoints_and_merge(merged_temp_save_location)
    return merged_df

# COMMAND ----------

def read_country_youtube_df_with_checkpoints(country_code:str) -> DataFrame:
    '''Check if there is a checkpoint for the country's dataset. If so, load it. Otherwise, read the raw dataset and perform preprocessing on it. Return the resulting DataFrame.'''
    temp_save_location = f'{STREAMING_YT_SPOTIFY_BRONZE_PATH}/temp_youtube_scraped/{country_code}_youtube_scraped/'
    if check_path_exists(temp_save_location):
        youtube_df =  spark.read.format('delta').load(temp_save_location)
    else:
        youtube_df = read_kaggle_csv(
            f'{STREAMING_YT_SPOTIFY_BRONZE_PATH}/youtube_raw/{country_code}_youtube_trending_data/',
            schema = YOUTUBE_RAW_SCHEMA
            )
        youtube_df = ensure_column_existence(youtube_df, YT_IS_SCRAPED, T.IntegerType(), 0)
        youtube_df = youtube_initial_cleaning(youtube_df)
        youtube_df = drop_duplicates_by_ordered_partition(youtube_df, YT_VIDEO_ID, youtube_df[YT_TRENDING_DATE].desc())
    return youtube_df

# COMMAND ----------

def read_youtube_datasets_checkpoints_and_merge(merged_temp_save_location, verbose = False):
    '''
    Read the YouTube raw dataset for each country and merge them into a single DataFrame. Return the Resulting DataFrame.
    
    If verbose is True, print the total number of videos and the number of unscraped videos for each dataset, and for the merged dataset. 
    '''
    individual_df_unique_count = 0
    merged_df = spark.createDataFrame([], YOUTUBE_SCRAPED_SCHEMA)
    for country_code in COUNTRY_CODES:
        youtube_df = read_country_youtube_df_with_checkpoints(country_code)

        if verbose:
            print(f'{country_code} unique videos: {youtube_df.count()}, unscraped: {youtube_df.where(F.col(YT_IS_SCRAPED) == 0).count()}')
            individual_df_unique_count += youtube_df.count()

        merged_df = merged_df.unionByName(youtube_df, allowMissingColumns=True)

    merged_df = merged_df.dropDuplicates(subset = [YT_VIDEO_ID])

    if verbose:
        print(f"Total rows of merged dataset: {individual_df_unique_count}")
        print(f"Unique videos in merged dataset: {merged_df.count()}")
        print(f'Videos already scraped from merged dataset: {merged_df.where(F.col(YT_IS_SCRAPED) != 0).count()}') 
    pass

    return merged_df.cache()

# COMMAND ----------

def read_raw_merged_youtube_csvs() -> DataFrame:
    '''Read raw YouTube datasets for each country and merge them into a single DataFrame. Return the resulting DataFrame.'''
    dataframes = []
    for country in COUNTRY_CODES:
        file_path = f'{STREAMING_YT_SPOTIFY_BRONZE_PATH}/youtube_raw/{country}_youtube_trending_data/'
        dataframe = read_kaggle_csv(file_path, YOUTUBE_RAW_SCHEMA)
        dataframes.append(dataframe)
    combined_df = dataframes[0]
    for dataframe in dataframes[1:]:
        combined_df = combined_df.unionByName(dataframe, allowMissingColumns=True)
    return combined_df