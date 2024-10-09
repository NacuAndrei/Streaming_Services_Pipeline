# Databricks notebook source
import re
import time
import random

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, DateType, StructField, StructType, FloatType, DoubleType
from imdb import Cinemagoer
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %run "../shared/constants"

# COMMAND ----------

# MAGIC %run "../shared/common_functions"

# COMMAND ----------

def change_id_prefix(input_df: DataFrame, prefix: str) -> DataFrame:
    '''Change prefix of show_id column from s to prefix. Return resulting DataFrame.'''
    updated_df = input_df.withColumn(SHOW_ID, F.expr(f"regexp_replace(SHOW_ID, '^s', '{prefix}')"))
    return updated_df

# COMMAND ----------

def remove_quotes_from_country(input_df: DataFrame) -> DataFrame:
    '''Remove trailing quotes from countries column. Return resulting DataFrame'''
    updated_df = input_df.withColumn(COUNTRIES, F.regexp_replace(COUNTRIES, r'^\s*"', '')) \
                   .withColumn(COUNTRIES, F.regexp_replace(COUNTRIES, r'"\s*$', ''))
    return updated_df

# COMMAND ----------

def replace_unwanted_string_in_column(input_df: DataFrame, replaced_string: str, new_string: str) -> DataFrame:
    '''Replace replaced_string in title column with new_string. Return resulting DataFrame.'''
    return input_df.withColumn(TITLE, F.regexp_replace(TITLE, replaced_string, new_string))

# COMMAND ----------

def move_values_between_columns(input_df: DataFrame, filtered_wrong_values_df: DataFrame, common_column: str, column_a: str, column_b: str) -> DataFrame:
    corrected_rated_df = filtered_wrong_values_df.withColumn(column_a, F.col(column_b)).withColumn(column_b, F.lit(None))
    remove_condition = (input_df[common_column].isin([row[common_column] for row in filtered_wrong_values_df.collect()]))
    filtered_combined_df = input_df.filter(~remove_condition)
    input_df = filtered_combined_df.union(corrected_rated_df)
    return input_df

# COMMAND ----------

def filter_non_english_values(input_df: DataFrame, regex_list: list = None, exclude_non_english = False) -> DataFrame:
    '''Select rows where title column contains non-latin characters, identified by regex_list. Return filtered DataFrame.'''
    return filter_with_regex_list(input_df, column = TITLE, regex_list=regex_list, reverse_match=exclude_non_english)

# COMMAND ----------

def add_platform_column(input_df: DataFrame, prefix: str) -> DataFrame:
    if prefix == "nflx":
        return input_df.withColumn(PLATFORM, F.lit("Netflix"))
    elif prefix == "amzn":
        return input_df.withColumn(PLATFORM, F.lit("Amazon"))
    elif prefix == "dsny":
        return input_df.withColumn(PLATFORM, F.lit("Disney"))

# COMMAND ----------

def remove_values_by_prefix(input_df: DataFrame, target_col: str, prefix_list: list) -> DataFrame:
    '''Filter out rows where target_col contains a prefix in prefix_list. Return filtered DataFrame.'''
    regex_pattern = f"^({'|'.join([re.escape(prefix) for prefix in prefix_list])})"
    
    removal_condition = ~F.col(target_col).rlike(regex_pattern)
    
    return input_df.filter(removal_condition)

# COMMAND ----------

def map_column(input_df: DataFrame, mapping_df: DataFrame, mapped_column: str, join_column: str) -> DataFrame:
    '''Join input_df and mapping_df on join_column. Overwrite join_column with mapped_column. Return resulting DataFrame.'''
    input_df = input_df.join(mapping_df, on=join_column, how="left")
    input_df = input_df.withColumn(join_column, F.col(mapped_column)).drop(mapped_column)
    return input_df

# COMMAND ----------

def get_names(entity_list):
        return [entity[NAME] for entity in entity_list if NAME in entity]

# COMMAND ----------

def extract_box_office_data(input_df: DataFrame, column: str) -> DataFrame:
    budget_pattern = r"Budget=([^\d]+[0-9,]+) \(estimated\)"
    opening_weekend_pattern = r"Opening Weekend [A-Za-z ]*=([^\d]+[0-9,]+), [0-9]{2} [A-Za-z]{3} [0-9]{4}"
    cumulative_gross_pattern = r"Cumulative Worldwide Gross=([^\d]+[0-9,]+)"
    
    input_df = input_df.withColumn(BUDGET, F.when(F.regexp_extract(F.col(column), budget_pattern, 0) == "", None)
                                .otherwise(F.regexp_extract(F.col(column), budget_pattern, 0)))
    
    input_df = input_df.withColumn(OPENING_WEEKEND, F.when(F.regexp_extract(F.col(column), opening_weekend_pattern, 0) == "", None)
                                       .otherwise(F.regexp_extract(F.col(column), opening_weekend_pattern, 0)))
    
    input_df = input_df.withColumn(CUMULATIVE_WORLDWIDE_GROSS, F.when(F.regexp_extract(F.col(column), cumulative_gross_pattern, 0) == "", None)
                                           .otherwise(F.regexp_extract(F.col(column), cumulative_gross_pattern, 0)))
    
    return input_df

# COMMAND ----------

def convert_to_eur(currency: str, amount: float) -> float:
    if currency in conversion_rates_to_eur and conversion_rates_to_eur[currency] is not None:
        return amount / conversion_rates_to_eur[currency]
    
convert_to_eur_udf = udf(convert_to_eur, DoubleType())

# COMMAND ----------

def extract_currency_and_amount(column):
    currency_code_col = F.regexp_extract(column, r'^(\D+)', 1).alias("currency")
    amount_col = F.regexp_extract(column, r'^(\D+)(\d+)', 2).cast("double").alias("amount")
    return currency_code_col, amount_col

# COMMAND ----------

def split_series_years(input_df: DataFrame, series_years_col: str, first_series_years_col: str, last_series_years_col: str) -> DataFrame:
    split_col = F.split(F.col(series_years_col), "-")
    
    input_df = input_df.withColumn(first_series_years_col, split_col.getItem(0))
    
    input_df = input_df.withColumn(
        last_series_years_col,
        F.when(F.size(split_col) > 1, split_col.getItem(1)).otherwise(F.lit(None))
    )
    
    input_df = input_df.drop(series_years_col)
    
    input_df = input_df.withColumn(first_series_years_col, F.col(first_series_years_col).cast("int")) \
           .withColumn(last_series_years_col, F.col(last_series_years_col).cast("int"))
    
    return input_df