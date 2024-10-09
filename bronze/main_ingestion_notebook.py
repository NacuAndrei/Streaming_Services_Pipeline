# Databricks notebook source
# MAGIC %run "../shared/config"

# COMMAND ----------

# MAGIC %run "../shared/constants"

# COMMAND ----------

# MAGIC %run "../shared/common_functions"

# COMMAND ----------

# MAGIC %run "../shared/kaggle_ingestion_functions"

# COMMAND ----------

kaggle_datasets_identifiers = {
    'amazon': KAGGLE_AMAZON_PRIME_IDENTIFIER,
    'netflix': KAGGLE_NETFLIX_IDENTIFIER,
    'disney': KAGGLE_DISNEY_PLUS_IDENTIFIER,
}

# COMMAND ----------

for dataset, identifier in kaggle_datasets_identifiers.items():
    download_kaggle_streaming_dataset(
    dataset_identifier = identifier,
    blob_storage_path = BRONZE_PATH,
    dataset_directory = STREAMING_SERVICES_DIRECTORY)

# COMMAND ----------

try:
    download_kaggle_youtube_dataset(
        KAGGLE_YOUTUBE_IDENTIFIER,
        BRONZE_PATH,
        'youtube_raw')
except Exception as e:
    print(f"Youtube dataset unavailable, loading from DBFS backup")
    
    category_json_path = f'/dbfs{DBFS_YOUTUBE_RAW_PATH}/US_category_id.json'
    with open(category_json_path, 'r') as f:
        category_json = json.load(f)

    category_id_map = {
        category_data['id']: category_data['snippet']['title']
        for category_data in category_json['items']
        }

    for file_path in dbutils.fs.ls(f"dbfs:{DBFS_YOUTUBE_RAW_PATH}"):
        file_path = file_path.path
        if not file_path.endswith('.csv'):
            continue

        file_name = file_path.split('/')[-1].split('.')[0]
        country_code = file_name[:2]

        df = read_kaggle_csv(file_path)
        df = map_column_values(df, category_id_map, YT_RAW_CATEGORY_ID, YT_RAW_CATEGORY_NAME)
        df = df.drop('description')
        df = df.withColumn(YT_RAW_COUNTRY_DATASET, F.lit(country_code))

        df.write \
            .options(
                header = True,
                quote='"', 
                escape='"') \
            .mode('overwrite') \
            .csv(f'{BRONZE_PATH}/youtube_raw/{file_name}') 
            