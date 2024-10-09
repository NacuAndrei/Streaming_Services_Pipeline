# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

# MAGIC %run "./common_functions"

# COMMAND ----------

import time
import math
from pyspark.sql.functions import count, when, col, sum as _sum, expr, udf, regexp_replace
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import MapType, StringType, Row
from enum import Enum


# COMMAND ----------

class spotifyEntityType(Enum):
    ALBUM = 'album'
    ARTIST = 'artist'
    AUDIO_FEATURES = 'audio_features'

# COMMAND ----------

key_dict = {
    0: 'C',
    1: 'C#',
    2: 'D',
    3: 'D#',
    4: 'E',
    5: 'F',
    6: 'F#',
    7: 'G',
    8: 'G#',
    9: 'A',
    10: 'A#',
    11: 'B'
}

mode_dict = {
    0: 'Minor',
    1: 'Major'
}

# COMMAND ----------

def search_track(search_filters: dict) -> dict:
    '''
    Search the Spotify API for a song by [name, [artist, album]] or [ISRC]. Return a dict with the top result's data as returned by the Spotify API.

    search_filters: dict with keys:
        'name',
        'artist' (optional but recommended),
        'album' (optional)
        'isrc' (optional, should be used alone)
    '''

    for search_filter, value in search_filters.items():
        if type(value) == str:
            search_filters[search_filter] = search_filters[search_filter].replace('�', ' ')
    
    query = ''.join([f'{search_filter}: {value} ' for search_filter, value in search_filters.items()]).strip()

    max_retries = 3
    attempts = 0
    retry_after = 2
    while attempts < max_retries:
        try:
            cache_token = AUTH_MANAGER.get_access_token(as_dict=False)
            sp = spotipy.Spotify(cache_token, retries = 0)
    
            results = sp.search(q = query, type = 'track', limit = 1)
            if results['tracks']['items']:
                return results['tracks']['items'][0]
            else:
                return None
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", retry_after))
                print(f"Rate limit hit for track data. Sleeping for {retry_after} seconds")
                time.sleep(retry_after)
                attempts+=1
            else:
                raise e

    return None

def get_track_data(search_filters: dict) -> dict:
    '''
    Search for a song using search_filters dict
    
    search_filters: dict with keys:
        'name',
        'artist' (optional but recommended),
        'album' (optional)
        'isrc' (optional, should be used alone)
    
    Return dictionary containing features:
        (name, artists list, album, isrc, id, explcit, popularity, album id, artist id) or None for each key if no results are found
    '''
    
    track_features = ['name','artists','album','isrc','id','explicit','popularity']

    result = search_track(search_filters)

    if result:
        out_feature_dict = {
            'name_track': result['name'],
            'name_artist_list': ", ".join(get_value_list_from_dicts(result["artists"], 'name')),
            'id_artist_list': ", ".join(get_value_list_from_dicts(result['artists'], 'id')),
            'name_album': result["album"]["name"],
            'isrc': result["external_ids"]["isrc"],
            'id_track': result["id"],
            'is_explicit': int(result["explicit"]),
            'popularity_track': result["popularity"] / 100,
            'id_album': result['album']['id']
            }
        
        return out_feature_dict
    
    else:
        return { key: None for key in track_features }


def get_track_data_row_by_name_and_artist(row: Row) -> Row:
    '''
    Apply get_track_data function to a dataframe row. Dataframe must have string columns name_track and name_artist_list
    
    Return a Row with the original columns and the track data as a dictionary in a "temp" column
    '''
    track_data = get_track_data({'name': row.name_track, 'artists': row.name_artist_list})
    row_dict = row.asDict()
    row_dict.update({"temp" : track_data})
    return Row(**row_dict)

def process_track_partition_by_name_and_artist(rows) -> Row:
    '''
    Apply the get_track_data_row_by_name_and_artist function to a dataframe partition. Dataframe must have string columns name_track and name_artist_list.
    
    Used for applying the function to a RDD in a parallel context.
    '''
    for row in rows:
        yield get_track_data_row_by_name_and_artist(row)


def get_track_data_row_by_isrc(row: Row) -> Row:
    '''
    Apply the get_track_data function to a dataframe row. Dataframe must have isrc string column

    Return a Row with the original columns and the track data as a dictionary in a "temp" column
    '''
    track_data = get_track_data({'isrc': row.isrc})
    row_dict = row.asDict()
    row_dict.update({"temp" : track_data})
    return Row(**row_dict)
 
def process_track_partition_by_isrc(rows) -> Row:
    '''
    Apply the get_track_data_row_by_name_and_artist function to a dataframe partition. Dataframe must have isrc column.
    
    Used for applying the function to a RDD in a parallel context
    '''
    for row in rows:
        yield get_track_data_row_by_isrc(row)

# COMMAND ----------

def search_artist(artist_name: str) -> dict:
    '''Search the Spotify API for an artist by name. Return a dict with the top result's raw data returned by the Spotify API.'''
    max_retries = 3
    attempts = 0
    retry_after = 2
    while attempts < max_retries:
        try:
            cache_token = AUTH_MANAGER.get_access_token(as_dict=False)
            sp = spotipy.Spotify(cache_token, retries=0)

            results = sp.search(q=f"{artist_name}", type="artist", limit=1)
            if results["artists"]["items"]:
                return results["artists"]["items"][0]
            else:
                return None
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", retry_after))
                print(
                    f"Rate limit hit for artist data. Sleeping for {retry_after} seconds"
                )
                time.sleep(retry_after)
                attempts += 1
            else:
                raise e

    return None

def get_artist_data(artist_name: str) -> dict:
    '''Search the Spotify API for an artist by name. Return a dict with the top result's extracted data in a dict (artist id, name, genres list, popularity, followers).'''
    artist_features = ["id", "name", "genres", "popularity", "followers"]
    
    result = search_artist(artist_name)

    if result:
        out_feature_dict = {
            "id": result["id"],
            "name": result["name"],
            "genres": result["genres"],
            "popularity": result["popularity"] / 100,
            "followers": result["followers"]["total"],
        }
    else:
        out_feature_dict = {key: None for key in artist_features}
    return out_feature_dict


def get_multiple_artists_by_id(artist_ids: list[str]) -> list[dict]:
    """
    Search the Spotify API for a list of artist id's and return a list of dictionaries containing (name, genres list, popularity, followers, id).

    If an artist is not found, dict values will be returned as "N/A".
    If the Spotify API endpoint is timed out and max retry limit is reached, a list of dicts with None values will be returned.
    """
    _artist_features = ["name_artist", "genres", "popularity", "followers", "id_artist"]
    max_retries = 5
    attempts = 0
    retry_after = 2
    while attempts < max_retries:
        try:
            cache_token = AUTH_MANAGER.get_access_token(as_dict=False)
            sp = spotipy.Spotify(cache_token, retries=0)

            result_artist_features_list = sp.artists(artist_ids)["artists"]
            out_list = []
            for result_artist_features in result_artist_features_list:
                if result_artist_features:
                    out_feature_dict = {
                        "name_artist": result_artist_features["name"],
                        "genres": result_artist_features["genres"],
                        "popularity_artist": result_artist_features["popularity"] / 100,
                        "id_artist": result_artist_features["id"],
                        "followers": result_artist_features["followers"]["total"],
                    }
                    out_list.append(out_feature_dict)
                else:
                    out_list.append({feature: "N/A" for feature in _artist_features})
            return out_list
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", retry_after * 2))
                time.sleep(retry_after + 0.5)
                attempts += 1
            else:
                raise e
    return [{feature: None for feature in _artist_features} * len(artist_ids)]

# COMMAND ----------

def get_multiple_albums_by_id(album_ids: list[str]) -> list[dict]:
    """
    Search the Spotify API for a list of album id's and return list of dictionaries containing (id, name, type, release date, label, popularity, number of tracks, isrc).

    If an album is not found, dict values will be returned as "N/A".
    If the Spotify API endpoint is timed out and max retry limit is reached, a list of dicts with None values will be returned.
    """
    _audio_features = ['id_album', 'name_album', 'is_single', 'release_date', 'label', 'popularity_album', 'total_tracks', 'isrc']
    max_retries = 3
    attempts = 0
    retry_after = 2
    while attempts < max_retries:
        try:
            cache_token = AUTH_MANAGER.get_access_token(as_dict=False)
            sp = spotipy.Spotify(cache_token, retries = 0)
            
            result_album_list = sp.albums(album_ids)['albums']
            out_list = []
            for result_album in result_album_list:
                if result_album:
                    out_feature_dict = {
                        'id_album': result_album['id'],
                        'name_album': result_album['name'],
                        'album_type': result_album['album_type'],
                        'release_date': result_album['release_date'],
                        'label': result_album['label'],
                        'popularity_album': result_album['popularity'] / 100,
                        'no_of_tracks': result_album['total_tracks'],
                    }
                    out_list.append(out_feature_dict)
                else:
                    out_list.append({feature: 'N/A' for feature in _audio_features})
            return out_list
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", retry_after))
                print(f"Rate limit hit for audio features. Sleeping for {retry_after} seconds")
                time.sleep(retry_after)
                attempts+=1
            else:
                raise e
    return [{feature: None for feature in _audio_features} * len(album_ids)]


# COMMAND ----------

def get_multiple_track_audio_features_by_id(track_ids: list[str]) -> list[dict]:
    """
    Search the Spotify API for a list of track id's and return list of dictionaries containing their audio features(key, mode, danceability, energy, speechiness, acousticness, instrumentalness, liveness, bpm, valence).

    If a track is not found, dict values will be returned as "N/A".
    If the Spotify API endpoint is timed out and max retry limit is reached, a list of dicts with None values will be returned.
    """

    _audio_features = ['key', 'mode', 'danceability', 'energy', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'bpm', 'valence']
    max_retries = 3
    attempts = 0
    retry_after = 2
    while attempts < max_retries:
        try:
            cache_token = AUTH_MANAGER.get_access_token(as_dict=False)
            sp = spotipy.Spotify(cache_token, retries = 0)
            
            result_audio_features_list = sp.audio_features(track_ids)
            out_list = []
            for result_audio_features in result_audio_features_list:
                if result_audio_features:
                    out_feature_dict = {
                        "id_track": result_audio_features["id"],
                        "key": key_dict[result_audio_features["key"]],
                        "mode": mode_dict[result_audio_features["mode"]],
                        "danceability": str(result_audio_features["danceability"]),
                        "energy": str(result_audio_features["energy"]),
                        "speechiness": str(result_audio_features["speechiness"]),
                        "acousticness": str(result_audio_features["acousticness"]),
                        "instrumentalness": str(result_audio_features["instrumentalness"]),
                        "liveness": str(result_audio_features["liveness"]),
                        "bpm": str(int(result_audio_features["tempo"])),
                        "valence": str(result_audio_features["valence"]),
                    }
                    out_list.append(out_feature_dict)
                else:
                    out_list.append({feature: 'N/A' for feature in _audio_features})
            return out_list
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", retry_after))
                print(f"Rate limit hit for audio features. Sleeping for {retry_after} seconds")
                time.sleep(retry_after)
                attempts+=1
            else:
                raise e
    return [{feature: None for feature in _audio_features} * len(track_ids)]


# COMMAND ----------

def spotify_fill_missing_data_batches(dataframe: DataFrame, columns_to_fill: list[str], id_column: str, table_type: spotifyEntityType) -> DataFrame:
    '''
    Fill missing values in columns_to_fill columns of dataframe. Searche Spotify API for data determined by table_type using id_column.

    Parameters:
    dataframe: dataframe to fill missing values in.
    columns_to_fill: list of columns to fill missing values in. If a column does not exist in dataframe, a new column with None values will be added before filling.
    id_column: column to use as id for searching Spotify API.
    table_type: spotifyEntityType(Enum) ["artist", "album", "audio_features"]

    Returns: dataframe with missing values filled in. If a column in columns_to_fill does not exist in dataframe, a new column with None values will be added before filling.
    '''
    for column in columns_to_fill:
        if column not in dataframe.columns:
            dataframe = dataframe.withColumn(column, F.lit(None))
    
    missing_values_df = select_rows_with_missing_values(dataframe, columns_to_fill)
    row_count = missing_values_df.count()

    if table_type == spotifyEntityType.AUDIO_FEATURES:
        batch_size = 100
        spotify_search_function = get_multiple_track_audio_features_by_id
    if table_type == spotifyEntityType.ALBUM:
        batch_size = 20
        spotify_search_function = get_multiple_albums_by_id
    if table_type == spotifyEntityType.ARTIST:
        batch_size = 50
        spotify_search_function = get_multiple_artists_by_id


    for batch in range(math.ceil(row_count/batch_size)):

        batch_ids = missing_values_df.select([id_column]).offset(batch*batch_size).limit(batch_size).rdd.flatMap(lambda x:x).collect()

        batch_data_to_fill = spotify_search_function(batch_ids)

        batch_data_to_fill_df = spark.createDataFrame([
            {f'new_{column}': value for column, value in row.items()}
            for row in batch_data_to_fill])

        dataframe = dataframe\
            .join(batch_data_to_fill_df,
                batch_data_to_fill_df[f'new_{id_column}'] == dataframe[id_column],
                'left')
        
        for column in columns_to_fill:
            dataframe = dataframe\
                .withColumn(
                    column,
                    F.coalesce(batch_data_to_fill_df[f'new_{column}'], dataframe[column])
                )\
                .drop(f'new_{column}')\
                .drop(f'new_{id_column}')

    return dataframe
    

    

# COMMAND ----------

def map_genre(genre):
    for pattern, mapped_genre in genre_spotify_mapping.items():
        if re.search(pattern, genre, re.IGNORECASE):
            return mapped_genre
    return "others"

map_genre_udf = udf(map_genre, StringType())