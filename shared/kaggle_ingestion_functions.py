# Databricks notebook source
import subprocess
import zipfile
import os
import json
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

# COMMAND ----------

def read_kaggle_csv(path: str, schema: StructType = None) -> DataFrame:
    """Read a kaggle youtube/streaming dataset csv and return a DataFrame."""
    dataframe = spark.read \
        .options(header=True,quote='"', escape='"', multiline=True) \
        .schema(schema) \
        .csv(path)
    dataframe = dataframe.withColumnsRenamed(YT_RENAME_COLUMNS_DICT)
    return dataframe

# COMMAND ----------

def download_kaggle_streaming_dataset(dataset_identifier: str,  blob_storage_path: str, temp_dir: str = 'tmp',dataset_directory: str = None) -> None:
    '''Download from Kaggle the streaming service dataset identified by 'dataset_identifier' and save it to blob storage.'''
    dataset_name = dataset_identifier.split('/')[-1]
    dataset_directory = dataset_directory if dataset_directory is not None else dataset_name

    subprocess.run(['kaggle', 'datasets', 'download', '-d', dataset_identifier, '-p', temp_dir], check = True)

    with zipfile.ZipFile(f'{temp_dir}/{dataset_name}.zip', 'r') as zip_ref:
        zip_ref.extractall(f'{temp_dir}/{dataset_directory}')
    dbutils.fs.rm(f'file:{os.getcwd()}/{temp_dir}/{dataset_name}.zip')


    file_name = os.listdir(f'{os.getcwd()}/{temp_dir}/{dataset_directory}')[0].split('.')[0]
    df = spark.read \
        .options(header=True,quote='"', escape='"', multiline=True) \
        .csv(f'file:{os.getcwd()}/{temp_dir}/{dataset_directory}/{file_name}.csv', header = True)

    df.write.csv(
        f'{blob_storage_path}/{dataset_directory}/{file_name}',
        mode = 'overwrite',
        header = True
    )

    dbutils.fs.rm(f"file:{os.getcwd()}/{temp_dir}/{dataset_directory}", recurse = True)


# COMMAND ----------

def download_kaggle_youtube_dataset(dataset_identifier: str,  blob_storage_path: str, temp_dir: str = 'tmp',dataset_directory: str = None) -> None:
    '''
    Download from Kaggle the YouTube dataset identified by dataset_identifier, perform preprocessing transformations and save it to blob storage.
    
    Remove the 'description' column
    Read category mapping json from dataset and add a category column.
    Add country code column according to the name of each file.
    '''
    dataset_name = dataset_identifier.split('/')[-1]
    dataset_directory = dataset_directory if dataset_directory is not None else dataset_name

    subprocess.run(['kaggle', 'datasets', 'download', '-d', dataset_identifier, '-p', temp_dir], check = True)

    with zipfile.ZipFile(f'{temp_dir}/{dataset_name}.zip', 'r') as zip_ref:
        zip_ref.extractall(f'{temp_dir}/{dataset_directory}')
    dbutils.fs.rm(f'file:{os.getcwd()}/{temp_dir}/{dataset_name}.zip')


    for file_name in os.listdir(f'{os.getcwd()}/{temp_dir}/{dataset_directory}'):
        if file_name.endswith('.csv'):
            file_name = file_name.split('.')[0]
            country_code = file_name[:2]

            df = spark.read \
                .options(header=True,quote='"', escape='"', multiline=True) \
                .csv(f'file:{os.getcwd()}/{temp_dir}/{dataset_directory}/{file_name}.csv', header = True)

            category_json_path = f'{os.getcwd()}/{temp_dir}/{dataset_directory}/{country_code}_category_id.json'
            with open(category_json_path, 'r') as f:
                category_json = json.load(f)
            
            category_id_map = {
                category_data['id']: category_data['snippet']['title']
                for category_data in category_json['items']
                }
            df = map_column_values(df, category_id_map, YT_RAW_CATEGORY_ID, YT_RAW_CATEGORY_NAME)
            df = df.drop('description')
            df = df.withColumn(YT_RAW_COUNTRY_DATASET, F.lit(country_code))

            df.write \
                .options(
                    header = True,
                    quote='"', 
                    escape='"') \
                .mode('overwrite') \
                .csv(f'{blob_storage_path}/{dataset_directory}/{file_name}')

            dbutils.fs.rm(f'file:{os.getcwd()}/{temp_dir}/{dataset_directory}/{file_name}.csv')
            dbutils.fs.rm(category_json_path)

    dbutils.fs.rm(f"file:{os.getcwd()}/{temp_dir}/{dataset_directory}", recurse = True)

