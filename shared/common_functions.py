# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column
from pyspark.sql import types as T
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
from enum import Enum
from py4j.java_gateway import java_import
import math
from typing import Dict
from pyspark.sql import Window
import time
from datetime import datetime
import pytz
from itertools import chain
import re
import os



# COMMAND ----------

def merge_tables(spark, db_name, table_name, new_data_view):
    target_table_schema = spark.table(f"{db_name}.{table_name}").schema
    column_names = [field.name for field in target_table_schema if field.name != 'show_id']
  
    update_set_clause = ",\n".join([f"target.{col} = updates.{col}" for col in column_names if col != 'last_updated_date'])

    insert_columns = ", ".join(['show_id'] + column_names)
    insert_values = ", ".join([f"updates.{col}" for col in ['show_id'] + column_names])

    if 'last_updated_date' in column_names:
        update_set_clause += ",\n target.last_updated_date = current_date()"
        insert_values = insert_values.replace("updates.last_updated_date", "current_date()")

    merge_sql = f"""
    MERGE INTO {db_name}.{table_name} target 
    USING {new_data_view} updates 
    ON target.show_id = updates.show_id 
    WHEN MATCHED THEN 
      UPDATE SET  
        {update_set_clause}
    WHEN NOT MATCHED 
      THEN INSERT ({insert_columns}) 
      VALUES ({insert_values})
    """

    spark.sql(merge_sql)

# COMMAND ----------

def count_na(dataframe: DataFrame, columns: list[str] = None) -> DataFrame:
    '''Return a dataframe with one row, containing the number of nulls for each column in columns list.'''
    columns = dataframe.columns if columns == None else columns
    return dataframe.select([F.count(F.when(F.col(c).isNull(), c)).alias(f'{c}_na') for c in columns])

# COMMAND ----------

def get_value_list_from_dicts(dicts: list[dict], key: str) -> list[str]:
    '''Return a list of values from a list of dicts'''
    return [item[key] for item in dicts]

# COMMAND ----------

def select_rows_with_missing_values(dataframe: DataFrame, columns: list[str] = None) -> DataFrame:
    '''
    Returns a dataframe containig all rows with any missing values in the specified columns list.

    If no columns are provided, all columns are considered.
    '''
    columns = columns if columns else dataframe.columns
    condition = None

    for column in columns:
        if condition is None:
            condition = F.col(column).isNull()
        else:
            condition = condition | F.col(column).isNull()
            
    return dataframe.where(condition)

# COMMAND ----------

def drop_missing_character(dataframe: DataFrame, columns: list[str], full_string: bool = True) -> DataFrame:
    '''
    Return dataframe with rows containing � filtered out.

    dataframe: dataframe to filter
    columns: list of columns to filter on
    full_string: wether to match the whole string or at least one character
    '''
    rexpression = '^[�\W]+$' if full_string else '�+'

    for column in columns:
        dataframe = dataframe.where(~(F.col(column).rlike(rexpression)))
    
    return dataframe

# COMMAND ----------

def regexp_match_count(df: DataFrame, columns: list[str], regex_expression: str) -> DataFrame:
    '''Count the number of matches of regex_expression in columns, the rows that match on all columns and rows that match on at least one column. Return a dataframe.'''
    return df.agg(
        *[F.sum(F.expr(f"rlike({c}, '{regex_expression}')").cast('int')).alias(f'{c}_count') for c in columns],
        F.sum(F.expr(' AND '.join([f"rlike({c}, '{regex_expression}')" for c in columns])).cast('int')).alias('all_columns_match_count'),
        F.sum(F.expr(' OR '.join([f"rlike({c}, '{regex_expression}')" for c in columns])).cast('int')).alias('any_column_match_count')
    )

# COMMAND ----------

def remove_outer_quotes(column: str):
    '''Return column with outer quotes removed.'''
    return F.regexp_replace(F.regexp_replace(F.col(column), '^"*', ''), '"*$', '')

# COMMAND ----------

def map_column_values(dataframe: DataFrame, map_dict: Dict, column: str, new_column: str = "", default_value: str = None) -> DataFrame:
    """Handy method for mapping column values from one value to another

    Args:
        dataframe (DataFrame): Dataframe to operate on 
        map_dict (Dict): Dictionary containing the values to map from and to
        column (str): The column containing the values to be mapped
        new_column (str, optional): The name of the column to store the mapped values in. 
                                    If not specified the values will be stored in the original column

    Returns:
        DataFrame
    """
    new_column = column if not new_column else new_column

    broadcast_map_dict = sc.broadcast(map_dict)
    map_dict = broadcast_map_dict.value

    spark_mapping_expression = F.create_map([F.lit(x) for x in chain(*map_dict.items())])
    dataframe = dataframe.withColumn(new_column, spark_mapping_expression.getItem(F.col(column)))
    dataframe = replace_value_in_column(dataframe, new_column, None, default_value)
    return dataframe

# COMMAND ----------

def convert_bool_columns_to_int(dataframe: DataFrame) -> DataFrame:
    '''Cast all boolean columns to IntegerType and return the resulting DataFrame.'''
    for field in dataframe.schema.fields:
        if isinstance(field.dataType, T.BooleanType):
            dataframe = dataframe.withColumn(field.name, F.col(field.name).cast(T.IntegerType()))
    return dataframe

# COMMAND ----------

def replace_value_in_column(dataframe: DataFrame, column: str, value_to_replace, value_to_fill) -> DataFrame:
    '''Replace all instances of value_to_replace in column from dataframe with value_to_fill. Return modified DataFrame.'''
    return dataframe.withColumn(column, F.when(F.col(column) == value_to_replace, value_to_fill).otherwise(F.col(column)))

# COMMAND ----------

def explode_string_column(dataframe: DataFrame, column_to_split: str, separator: str = ',', new_column: str = None, keep_old_column = False) -> DataFrame:
    '''
    Explode string column column_to_split into multiple rows, splitting by separator. Write resulting column into new_column. Return modified DataFrame.
    
    If no new_column is provided, default to replacing column_to_split.
    If new_column is provided and keep_old_column is True, drop column_to_split!
    '''
    new_column = new_column if new_column else column_to_split

    dataframe = dataframe \
        .withColumn(column_to_split, F.split(F.col(column_to_split), separator)) \
        .withColumn(new_column, F.explode(F.col(column_to_split)))

    if column_to_split != new_column and not keep_old_column:
        dataframe = dataframe.drop(column_to_split)

    return dataframe

# COMMAND ----------

def group_by_count(dataframe: DataFrame, group_by: str | list[str] | Column | list[Column]) -> DataFrame:
    '''Apply groupBy and count on group_by columns and count entries. Return modified DataFrame.'''
    if isinstance(group_by, str):
        group_by = [group_by]
    
    return dataframe \
        .groupBy(*group_by) \
        .agg(*[F.count(column).alias(f'{column}_count') for column in group_by])

# COMMAND ----------

def drop_duplicates_by_ordered_partition(dataframe: DataFrame, partition_by: str | list[str] | Column | list[Column], order_by: str | list[str] | Column | list[Column]):
    '''Drop duplicates by partitioning by partition_by columns, ordering by order_by columns and keeping only the first row. Return modified DataFrame'''
    window_spec = Window.partitionBy(partition_by).orderBy(order_by)
    return dataframe \
        .withColumn('row_number', F.row_number().over(window_spec)) \
        .where(F.col('row_number') == 1) \
        .drop('row_number')

# COMMAND ----------

def check_path_exists(path: str) -> bool:
    '''Check if ABFS path exists. Return boolean value'''
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        return False

# COMMAND ----------

def get_current_localized_timestamp() -> str:
    '''Return current timestamp string for Bucharest time, in the format: MM-DD HH:MM:SS.'''
    return datetime.now(pytz.timezone('Europe/Bucharest')).strftime("%m-%d %H:%M:%S")


# COMMAND ----------

def format_seconds_and_minutes(seconds: int | float) -> str:
    '''Return a string representation of the given number of seconds, with the format: MM:SS.1f.'''
    minutes, seconds = divmod(seconds, 60)
    return f"{int(minutes):02}:{seconds:04.1f}"

# COMMAND ----------

def df_to_dict(dataframe: DataFrame, col_key: str, col_value: str) -> dict:
    '''Construct and return dictionary from given DataFrame, with the given column names as keys and values.'''
    df_unique = dataframe.where((F.col(col_key).isNotNull()) & (F.col(col_value).isNotNull())).select(col_key, col_value).dropDuplicates([col_key])
    return dict(df_unique.rdd.map(lambda row: (row[col_key], row[col_value])).collect())

# COMMAND ----------

def rename_table(database: str, old_table_name: str, new_table_name: str) -> None:
    '''Rename database.old_table_name to database.new_table_name'''
    spark.sql(f"ALTER TABLE {database}.{old_table_name} RENAME TO {database}.{new_table_name}")

# COMMAND ----------

def create_unique_df(input_df: DataFrame, column_name: str, alias_name: str, id_name: str) -> DataFrame:
    '''Construct dataframe from string column column_name split by "," and assign unique id to each value. Return resulting DataFrame.'''
    unique_values_df = input_df.select(F.explode(F.split(F.col(column_name), ",")).alias(alias_name)).distinct()
    unique_values_df = unique_values_df.withColumn(id_name, F.monotonically_increasing_id())

    return unique_values_df

# COMMAND ----------

def create_association_table(left_df: DataFrame, right_df: DataFrame, column_name: str, alias_name: str, left_id_name: str, right_id_name: str) -> DataFrame:
    '''Construct association table from left_df and right_df. Join on alias_name and return resulting DataFrame with left_id_name and right_id_name columns.'''
    association_df = left_df.select(left_id_name, F.explode(F.split(F.col(column_name), ",")).alias(alias_name))
    association_df = association_df.join(right_df, alias_name, "left").select(left_id_name, right_id_name)
    return association_df


# COMMAND ----------

def save_to_db(dataframe: DataFrame, db_name: str, table_name: str, overwrite_schema: bool = False) -> None:
    '''Save dataframe to database as delta table with name table_name. If overwrite_schema is True, overwrite existing table schema (default False).'''
    write_mode = 'overwrite' if overwrite_schema else 'append'
    dataframe.write.format("delta").mode(write_mode).option("mergeSchema", 'true').saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def save_to_db2(input_df, db_name, table_name):
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    input_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    input_df.write.mode("overwrite").format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def list_to_comma_separated_string(items: list[str]) -> str:
    '''Join a list of strings into a comma separated string and return it.'''
    if items:
        return ", ".join(items)
    return None

# COMMAND ----------

def convert_date_format(input_df: DataFrame, column_name: str) -> DataFrame:
    '''Convert a date column from a string to a date format. Returns modified DataFrame.'''
    updated_df = input_df.withColumn(
        column_name,
        F.to_date(F.col(column_name), 'MMMM d, yyyy')
    )
    return updated_df

# COMMAND ----------

def load_csv(path: str) -> DataFrame:
    '''Read a CSV file into a DataFrame.'''
    return spark.read \
        .format("csv") \
        .options(
            header=True,
            multiline=True,
            quote='"',
            escape='"',
            delimiter=',',
            inferSchema=True,
            encoding='utf-8'
        ) \
        .load(path)

# COMMAND ----------

def ensure_column_existence(dataframe: DataFrame, column_name: str, column_type: str, default_value: None) -> DataFrame:
    '''Check if a column exists in the dataframe, if not, add it with a default value. Return modified DataFrame.'''
    if column_name not in dataframe.schema.fieldNames():
        dataframe = dataframe.withColumn(column_name, F.lit(default_value).cast(column_type))
    return dataframe

# COMMAND ----------

def merge_schema_into_dataframe(dataframe: DataFrame, schema_to_merge: StructType) -> DataFrame:
    '''Merges schema_to_merge into dataframe using union. Return modified DataFrame.'''
    empty_df = spark.createDataFrame([], schema_to_merge)
    return dataframe.unionByName(empty_df, allowMissingColumns=True)

# COMMAND ----------

def add_prefix_to_column_names(dataframe: DataFrame, prefix: str) -> DataFrame:
    '''Rename all column names in a dataframe in the format '{prefix}{column}'. Return modified DataFrame.'''
    return dataframe.withColumnsRenamed({
        column: f'{prefix}{column}' for column in dataframe.schema.fieldNames()
    }) 

# COMMAND ----------

def coalesce_prefixed_columns(dataframe: DataFrame, prefix: str, columns: list[str]) -> DataFrame:
    '''Coalesce columns starting with prefix to a single column with no prefix. Return modified DataFrame.'''
    for column in columns:
        dataframe = dataframe.withColumn(column, F.coalesce(dataframe[f'{prefix}{column}'], dataframe[column])) \
            .drop(f'{prefix}{column}')

# COMMAND ----------

def truncate_string_columns(dataframe: DataFrame, column_names: str | list[str], start: int = 1, size: int = None) -> DataFrame:
    '''Truncate string columns. Return modified DataFrame.'''
    if isinstance(column_names, str):
        column_names = [column_names]
    for column in column_names:
        dataframe = dataframe.withColumn(column, F.substring(F.col(column), start, size))
    return dataframe

# COMMAND ----------

def ensure_list_column(dataframe: DataFrame, column_name: str, separator = ', ', null_value = None) -> DataFrame:
    '''
    Ensure that column column_name is a list of strings. If not, separate into lists and return modified DataFrame.
    
    Parameters:
        dataframe: DataFrame to be modified.
        column_name: Name of column to be converted to list.
        separator: Separator to use when converting to list.
        null_value: Value to replace nulls with.
    '''
    if(dict(dataframe.dtypes)[column_name] == 'string'):
        if null_value:
            dataframe = replace_value_in_column(dataframe, column_name, null_value, None)
        dataframe = dataframe.withColumn(column_name, F.split(F.col(column_name), separator))
    return dataframe

# COMMAND ----------

def filter_with_regex_list(dataframe: DataFrame, column: str, regex_list: list = None, reverse_match = False) -> DataFrame:
    '''Select rows where title column matches either regex in regex_list. Return filtered DataFrame. If reverse_match is True, select rows with no matches.'''
    if not regex_list:
        return dataframe
    
    condition = F.col(column).rlike(regex_list[0])
    for regex in regex_list[1:]:
        condition |= F.col(column).rlike(regex)

    if reverse_match:
        condition = ~condition
    
    return dataframe.filter(condition)

# COMMAND ----------

def iso8601_to_time(duration: str) -> str:
    if not duration:
        return None
    match = re.match(r'P(\d+D)?T(\d+H)?(\d+M)?(\d+S)?', duration)
    if not match:
        return None
    days = int(match.group(1)[:-1]) if match.group(1) else 0
    hours = int(match.group(2)[:-1]) if match.group(2) else 0
    minutes = int(match.group(3)[:-1]) if match.group(3) else 0
    seconds = int(match.group(4)[:-1]) if match.group(4) else 0
    return f'{days}.{hours:02}:{minutes:02}:{seconds:02}'

iso8601_to_time_udf = F.udf(iso8601_to_time, T.StringType())

# COMMAND ----------

def convert_string_boolean_to_int(dataframe: DataFrame, column: str) -> DataFrame:
    '''Convert string column with true and false values (capitalized or not) into integer. Return resulting DataFrame.'''
    return dataframe.withColumn(column, F.when(F.lower(F.col(column)) == 'true',1).otherwise(F.when(F.lower(F.col(column)) == 'false',0).otherwise(None)))

# COMMAND ----------

def filter_date_range(dataframe: DataFrame, date_column: str, start_date: str, end_date: str) -> DataFrame:
    '''Filter a dataframe datetype column by date range. Return filtered DataFrame'''
    return dataframe.where((F.col(date_column) >= start_date) & (F.col(date_column) <= end_date))
    
def filter_date_year(dataframe: DataFrame, column:str, year: int) -> DataFrame:
    return filter_date_range(dataframe, column, f'{year}-01-01', f'{year}-12-31')

# COMMAND ----------

def display_duplicates(dataframe: DataFrame, column: str) -> None:
    '''Displays the count of duplicates for each value in the specified column.'''
    dataframe.groupBy(column).agg(F.count('*').alias('count')).where(F.col('count') > 1).display()

# COMMAND ----------

def convert_to_local_time(utc_time: T.TimestampType, country_code: str) -> T.TimestampType:
    if country_code in COUNTRY_CODES_TIMEZONES_MAP_BROADCAST.value:
        local_tz = pytz.timezone(COUNTRY_CODES_TIMEZONES_MAP_BROADCAST.value[country_code])
        utc_dt = utc_time.replace(tzinfo=pytz.utc)
        local_dt = utc_dt.astimezone(local_tz)
        return local_dt.replace(tzinfo=None)
    else:
        return utc_time  # If country code is not found, return the original UTC time

# COMMAND ----------

convert_to_local_time_udf = F.udf(convert_to_local_time, T.TimestampType())

# COMMAND ----------

def aggregate_category_distribution_over_time_periods(dataframe: DataFrame, group_by_columns: str | list[str], date_column: str, period: str='month'):
    '''Group by group_by_columns and time period (year/month/day) then count entries. Return aggregated DataFrame.'''
    if isinstance(group_by_columns, str):
        group_by_columns = [group_by_columns]
    return dataframe \
        .withColumn(period, F.date_trunc(period, F.col(date_column))) \
        .groupBy(*group_by_columns, period) \
        .agg(
            F.count('*').alias(f'count_per_{period}'),
        )