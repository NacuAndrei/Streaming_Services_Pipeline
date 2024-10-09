# Databricks notebook source
#Raw bronze streaming services columns
SHOW_ID = "show_id"
CATEGORY = "category"
AGE_REQUIRED = "age_required"
TITLE = "title"
DIRECTOR = "director"
CAST = "cast"
COUNTRIES = "countries"
DATE_ADDED = "date_added"
RELEASE_YEAR = "release_year"
DURATION = "duration"
GENRES = "genres"
DESCRIPTION = "description"
IS_ANIMATION = "is_animation"
PLATFORM = "platform"
IS_SCRAPED = "is_scraped"

# COMMAND ----------

raw_netflix_folder = "raw_netflix_df.parquet"
raw_amazon_folder = "raw_amazon_df.parquet"
raw_disney_folder = "raw_disney_df.parquet"

# COMMAND ----------

#Scraped bronze streaming services columns
IMDB_ID = "imdb_id"
DIRECTORS = "directors"
RATING = "rating"
VOTES = "votes"
LANGUAGES = "languages"
COLOR = "color"
TOP_250 = "top_250"
BOX_OFFICE = "box_office"
PRODUCTION_COMPANIES = "production_companies"
PROD_COMPANIES = "prod_companies"
ACTORS = "actors"
WRITERS = "writers"
PLATFORMS = "platforms"
SERIES_YEARS = "series_years"
RELEASE_DATE = "release_date"

#Cleaned bronze streaming services columns
DURATION_MINUTES = "duration_minutes"
DURATION_SEASONS = "duration_seasons"
BUDGET = "budget"
OPENING_WEEKEND = "opening_weekend"
CUMULATIVE_WORLDWIDE_GROSS = "cumulative_worldwide_gross"
FIRST_SERIES_YEARS = "first_series_years"
LAST_SERIES_YEARS = "last_series_years"
NAME = "name"
BUDGET_EUR = "budget_eur"
OPENING_WEEKEND_EUR = "opening_weekend_eur"
CUMULATIVE_WORLDWIDE_GROSS_EUR = "cumulative_worldwide_gross_eur"

#Silver entities streaming services columns
DIRECTOR_NAME = "director_name"
DIRECTOR_ID = "director_id"
ACTOR_NAME = "actor_name"
ACTOR_ID = "actor_id"
COUNTRY_NAME = "country_name"
COUNTRY_ID = "country_id"
PLATFORM_NAME = "platform_name"
PLATFORM_ID = "platform_id"
GENRE_NAME = "genre_name"
GENRE_ID = "genre_id"
PROD_COMPANY_NAME = "prod_company_name"
PROD_COMPANY_ID = "prod_company_id"
WRITER_NAME = "writer_name"
WRITER_ID = "writer_id"
LANGUAGE_NAME = "language_name"
LANGUAGE_ID = "language_id"

# COMMAND ----------

#Silver entities spotify columns
NAME_GENRE = "name_genre"
ID_GENERAL_GENRE = "id_general_genre"
MAPPED_GENRE = "mapped_genre"
ID_GENRE = "id_genre"
ID_ARTIST = "id_artist"
NAME_ARTIST = "name_artist"

ID_TRACK = "id_track"
STREAMS = "streams"
PLAYLIST_COUNT = "playlist_count"
POPULARITY_TRACK = "popularity_track"
INGESTION_DATE = "ingestion_date"

NAME_TRACK = "name_track"
ISRC = "isrc"
BPM = "bpm"
KEY = "key"
MODE = "mode"
RELEASE_DATE = "release_date"
DANCEABILITY = "danceability"
ENERGY = "energy"
ACOUSTICNESS = "acousticness"
INSTRUMENTALNESS = "instrumentalness"
LIVENESS = "liveness"
SPEECHINESS = "speechiness"
TRACK_RELEASE_DATE = "release_date"
SPOTIFY_SOURCE_DATASET = "source_dataset"

ID_ARTIST = "id_artist"
IS_MAIN_ARTIST = "is_main_artist"

NAME_ARTIST = "name_artist"
COUNTRY = "country"
BIRTH_DATE = "birth_date"
TYPE = "type"

# COMMAND ----------

from pyspark.sql import types as T

# COMMAND ----------

KAGGLE_YOUTUBE_IDENTIFIER = 'rsrishav/youtube-trending-video-dataset'
KAGGLE_AMAZON_PRIME_IDENTIFIER = 'shivamb/amazon-prime-movies-and-tv-shows'
KAGGLE_NETFLIX_IDENTIFIER = 'shivamb/netflix-shows'
KAGGLE_DISNEY_PLUS_IDENTIFIER = 'shivamb/disney-movies-and-tv-shows'

STREAMING_SERVICES_DIRECTORY = 'streaming_services_raw'

STREAMING_YT_SPOTIFY_BRONZE_PATH = ''
STREAMING_YT_SPOTIFY_SILVER_PATH = ''
STREAMING_YT_SPOTIFY_GOLD_PATH = ''
DBFS_YOUTUBE_RAW_PATH = '/FileStore/kaggle_datasets/youtube-trending-video-dataset'

# COMMAND ----------

# YouTube raw bronze columns
YT_RAW_VIDEO_ID = 'video_id'
YT_RAW_VIDEO_TITLE = 'title'
YT_RAW_VIDEO_PUBLISH_TIME = 'publishedAt'
YT_RAW_CHANNEL_ID = 'channelId'
YT_RAW_CHANNEL_TITLE = 'channelTitle'
YT_RAW_CATEGORY_ID = 'categoryId'
YT_RAW_TRENDING_DATE = 'trending_date'
YT_RAW_TAGS = 'tags'
YT_RAW_VIEW_COUNT = 'view_count'
YT_RAW_LIKES = 'likes'
YT_RAW_DISLIKES = 'dislikes'
YT_RAW_COMMENT_COUNT = 'comment_count'
YT_RAW_THUMBNAIL_LINK = 'thumbnail_link'
YT_RAW_COMMENTS_DISABLED = 'comments_disabled'
YT_RAW_RATINGS_DISABLED = 'ratings_disabled'
YT_RAW_CATEGORY_NAME = 'category_name'
YT_RAW_COUNTRY_DATASET = 'country_dataset'

# COMMAND ----------

# YouTube cleaned bronze columns
YT_VIDEO_ID = 'video_id'
YT_VIDEO_TITLE = 'title' 
YT_VIDEO_PUBLISH_TIME = 'published_at'
YT_CHANNEL_ID = 'channel_id'
YT_CHANNEL_TITLE = 'channel_title'
YT_CATEGORY_ID = 'category_id'
YT_CATEGORY_NAME = 'category_name'
YT_TRENDING_DATE = 'trending_date'
YT_TAG = 'tag'
YT_VIEW_COUNT = 'view_count'
YT_LIKES = 'likes'#
YT_DISLIKES = 'dislikes'#
YT_COMMENT_COUNT = 'comment_count'
YT_THUMBNAIL_LINK = 'thumbnail_link'
YT_COMMENTS_DISABLED = 'comments_disabled'
YT_RATINGS_DISABLED = 'ratings_disabled'
YT_COUNTRY_DATASET = 'country_dataset'#

# YouTube additional scraped and auxiliary columns
YT_IS_SCRAPED = 'is_scraped'
YT_TAG_ID = 'tag_id'
YT_TAGS = 'tags'
YT_DEFAULT_LANGUAGE = 'default_language'#X
YT_DURATION = 'duration'
YT_CAPTION = 'caption'
YT_LICENSED_CONTENT = 'licensed_content'
YT_REGION_RESTRICTION = 'region_restriction'#X
YT_CONTENT_RATING = 'content_rating' #X

# COMMAND ----------

YOUTUBE_OTHER_TAG_PLACEHOLDER = '[OTHERS]'

# COMMAND ----------

YT_RENAME_COLUMNS_DICT = {
    YT_RAW_VIDEO_ID: YT_VIDEO_ID,
    YT_RAW_VIDEO_TITLE: YT_VIDEO_TITLE,
    YT_RAW_VIDEO_PUBLISH_TIME: YT_VIDEO_PUBLISH_TIME,
    YT_RAW_CHANNEL_ID: YT_CHANNEL_ID,
    YT_RAW_CHANNEL_TITLE: YT_CHANNEL_TITLE,
    YT_RAW_CATEGORY_ID: YT_CATEGORY_ID,
    YT_RAW_TRENDING_DATE: YT_TRENDING_DATE,
    YT_RAW_TAGS: YT_TAGS,
    YT_RAW_VIEW_COUNT: YT_VIEW_COUNT,
    YT_RAW_LIKES: YT_LIKES,
    YT_RAW_DISLIKES: YT_DISLIKES,
    YT_RAW_COMMENT_COUNT: YT_COMMENT_COUNT,
    YT_RAW_THUMBNAIL_LINK: YT_THUMBNAIL_LINK,
    YT_RAW_COMMENTS_DISABLED: YT_COMMENTS_DISABLED,
    YT_RAW_RATINGS_DISABLED: YT_RATINGS_DISABLED,
    YT_RAW_CATEGORY_NAME: YT_CATEGORY_NAME,
    YT_RAW_COUNTRY_DATASET: YT_COUNTRY_DATASET,
}

# COMMAND ----------

YOUTUBE_RAW_SCHEMA = T.StructType([
    T.StructField(YT_RAW_VIDEO_ID, T.StringType()),
    T.StructField(YT_RAW_VIDEO_TITLE, T.StringType()),
    T.StructField(YT_RAW_VIDEO_PUBLISH_TIME, T.TimestampType()),
    T.StructField(YT_RAW_CHANNEL_ID, T.StringType()),
    T.StructField(YT_RAW_CHANNEL_TITLE, T.StringType()),
    T.StructField(YT_RAW_CATEGORY_ID, T.IntegerType()),
    T.StructField(YT_RAW_TRENDING_DATE, T.TimestampType()),
    T.StructField(YT_RAW_TAGS, T.StringType()),
    T.StructField(YT_RAW_VIEW_COUNT, T.LongType()),
    T.StructField(YT_RAW_LIKES, T.LongType()),
    T.StructField(YT_RAW_DISLIKES, T.LongType()),
    T.StructField(YT_RAW_COMMENT_COUNT, T.LongType()),
    T.StructField(YT_RAW_THUMBNAIL_LINK, T.StringType()),
    T.StructField(YT_RAW_COMMENTS_DISABLED, T.BooleanType()),
    T.StructField(YT_RAW_RATINGS_DISABLED, T.BooleanType()),
    T.StructField(YT_RAW_CATEGORY_NAME, T.StringType()),
    T.StructField(YT_RAW_COUNTRY_DATASET, T.StringType()),
])


# COMMAND ----------

YOUTUBE_SCRAPED_SCHEMA = T.StructType([
    T.StructField(YT_VIDEO_ID, T.StringType(), True),
    T.StructField(YT_VIDEO_TITLE, T.StringType(), True),
    T.StructField(YT_VIDEO_PUBLISH_TIME, T.TimestampType(), True),
    T.StructField(YT_CHANNEL_ID, T.StringType(), True),
    T.StructField(YT_CHANNEL_TITLE, T.StringType(), True),
    T.StructField(YT_CATEGORY_ID, T.IntegerType(), True),
    T.StructField(YT_TRENDING_DATE, T.TimestampType(), True),
    T.StructField(YT_TAGS, T.ArrayType(T.StringType(), True), True),
    T.StructField(YT_VIEW_COUNT, T.LongType(), True),
    T.StructField(YT_LIKES, T.LongType(), True),
    T.StructField(YT_DISLIKES, T.LongType(), True),
    T.StructField(YT_COMMENT_COUNT, T.LongType(), True),
    T.StructField(YT_THUMBNAIL_LINK, T.StringType(), True),
    T.StructField(YT_COMMENTS_DISABLED, T.BooleanType(), True),
    T.StructField(YT_RATINGS_DISABLED, T.BooleanType(), True),
    T.StructField(YT_CATEGORY_NAME, T.StringType(), True),
    T.StructField(YT_COUNTRY_DATASET, T.StringType(), True),
    T.StructField(YT_IS_SCRAPED, T.IntegerType(), True),
    T.StructField(YT_DEFAULT_LANGUAGE, T.StringType(), True),
    T.StructField(YT_DURATION, T.StringType(), True),
    T.StructField(YT_CAPTION, T.StringType(), True),
    T.StructField(YT_LICENSED_CONTENT, T.BooleanType(), True),
    T.StructField(YT_REGION_RESTRICTION, T.MapType(T.StringType(), T.ArrayType(T.StringType(), True), True), True),
    T.StructField(YT_CONTENT_RATING, T.MapType(T.StringType(), T.StringType(), True), True)
])

# COMMAND ----------


SCRAPED_NEW_COLUMNS_SCHEMA = T.StructType([
    T.StructField(YT_VIDEO_ID, T.StringType()),
    T.StructField(YT_TAGS, T.ArrayType(T.StringType())),
    T.StructField(YT_DEFAULT_LANGUAGE, T.StringType()),
    T.StructField(YT_DURATION, T.StringType()),
    T.StructField(YT_CAPTION, T.StringType()),
    T.StructField(YT_LICENSED_CONTENT, T.BooleanType()),
    T.StructField(YT_REGION_RESTRICTION, T.MapType(T.StringType(), T.ArrayType(T.StringType()))),
    T.StructField(YT_CONTENT_RATING, T.MapType(T.StringType(), T.StringType())),
    T.StructField(YT_IS_SCRAPED, T.IntegerType()),
])

# COMMAND ----------

YOUTUBE_PARTS = "snippet,contentDetails"

# COMMAND ----------

COUNTRY_CODES = ['BR', 'CA', 'DE', 'FR', 'GB', 'IN', 'JP', 'KR', 'MX', 'RU', 'US']

# COMMAND ----------

dataframes_ids = {
    "raw_netflix_df": "nflx",
    "raw_amazon_df": "amzn",
    "raw_disney_df": "dsny"
}

# COMMAND ----------

rating_mapping = {
    "TV-Y": 0,
    "TV-Y7": 7,
    "TV-Y7-FV": 7,
    "G": 0,
    "PG": 7,
    "PG-13": 13,
    "R": 16,
    "NC-17": 18,
    "TV-G": 0,
    "TV-PG": 7,
    "TV-14": 13,
    "TV-MA": 18,
    "ALL": 0,
    "ALL_AGES": 0,
    "16": 16,
    "18+": 18,
    "13+": 13,
    "7+": 7,
    "AGES_16_": 16,
    "AGES_18_": 18,
    "16+": 16,
    "NR": None,
    "UR": None,
    "UNRATED": None,
    "NOT_RATE": None,
    "TV-NR": None,
    None: None,
}

# COMMAND ----------

genre_spotify_mapping = {
    ".*metal.*": "metal",
    ".*trap.*": "trap",
    ".*hip hop.*": "rap",
    ".*rap.*": "rap",
    ".*pop.*": "pop",
    ".*indie.*": "indie",
    ".*rock.*": "rock",
    ".*r&b.*": "r&b",
    ".*edm.*": "edm",
    ".*house.*": "house",
    ".*instrumental.*": "instrumental",
    ".*afro.*": "afro",
    ".*jazz.*": "jazz",
    ".*blues.*": "blues",
    ".*country.*": "country",
    ".*classical.*": "classical",
    ".*reggae.*": "reggae",
    ".*folk.*": "folk",
    ".*soul.*": "soul",
    ".*funk.*": "funk",
    ".*punk.*": "punk",
    ".*disco.*": "disco",
    ".*techno.*": "techno",
    ".*dance.*": "dance",
    ".*drill.*": "drill",
    ".*hardstyle.*": "hardstyle",
    ".*electronic.*": "electronic",
    ".*core.*": "electronic",
    ".*lo-fi.*": "lo-fi",
    ".*alternative.*": "alternative",
    ".*bass.*": "bass",
    ".*urbano.*": "urbano",
    ".*phonk.*": "phonk",
    ".*trance.*": "trance",
    ".*brasileiro.*": "brazilian",
    ".*songwriter.*": "songwriter",
    ".*orchestral.*": "orchestral"
}

# COMMAND ----------

genre_streaming_services_mapping = {
    ".*adventure.*": "Adventure",
    ".*animals.*|.*nature.*": "Animals & Nature",
    ".*animation.*": "Animation",
    ".*anime.*": "Anime",
    ".*anthology.*": "Anthology",
    ".*arthouse.*": "Arthouse",
    ".*arts.*": "Arts",
    ".*biographical.*": "Biographical",
    ".*buddy.*": "Buddy",
    ".*children.*|.*family.*": "Children & Family",
    ".*classic.*": "Classic",
    ".*comedies.*|.*comedy.*": "Comedy",
    ".*coming of age.*": "Coming of Age",
    ".*crime.*": "Crime",
    ".*cult.*": "Cult",
    ".*dance.*": "Dance",
    ".*disaster.*": "Disaster",
    ".*documentaries.*|.*documentary.*|.*docuseries.*": "Documentary",
    ".*drama.*|.*dramas.*": "Drama",
    ".*entertainment.*": "Entertainment",
    ".*faith.*|.*spirituality.*": "Faith & Spirituality",
    ".*fantasy.*": "Fantasy",
    ".*game show.*|.*competition.*": "Game Show / Competition",
    ".*historical.*": "Historical",
    ".*horror.*": "Horror",
    ".*independent.*": "Independent",
    ".*international.*": "International",
    ".*kids.*": "Kids",
    ".*korean.*": "Korean",
    ".*lgbtq.*": "LGBTQ",
    ".*lifestyle.*": "Lifestyle",
    ".*medical.*": "Medical",
    ".*military.*|.*war.*": "Military and War",
    ".*music.*|.*musical.*|.*concert.*|.*videos.*": "Music",
    ".*mystery.*": "Mystery",
    ".*parody.*": "Parody",
    ".*police.*|.*cop.*": "Police/Cop",
    ".*reality.*": "Reality",
    ".*romance.*|.*romantic.*": "Romance",
    ".*sci-fi.*|.*science.*|.*fantasy.*": "Sci-Fi & Fantasy",
    ".*soap opera.*|.*melodrama.*": "Soap Opera / Melodrama",
    ".*spanish-language.*": "Spanish-Language",
    ".*special interest.*": "Special Interest",
    ".*sports.*": "Sports",
    ".*spy.*|.*espionage.*": "Spy/Espionage",
    ".*stand-up.*|.*talk show.*|.*variety.*": "Stand-Up Comedy & Talk Shows",
    ".*superhero.*": "Superhero",
    ".*survival.*": "Survival",
    ".*suspense.*": "Suspense",
    ".*teen.*": "Teen",
    ".*thriller.*|.*thrillers.*": "Thriller",
    ".*travel.*": "Travel",
    ".*unscripted.*": "Unscripted",
    ".*western.*": "Western",
    ".*young adult.*": "Young Adult Audience"
}

# COMMAND ----------

CONDENSE_YOUTUBE_TAGS_MAP = {
    r'\bgaming\b|\bgameplay\b|\bgamer\b': 'gaming',
    r'\bvideo ?gam(e|ing)\b': 'gaming',
    r'\bmine ?craft\b': 'minecraft',
    r'\b(comedy|funny|skit|laugh|humou?r|prank)\b': 'comedy',
    r'\bvlogs?\b': 'vlog',
    r'\bnews\b': 'news',
    r'\btrailer\b': 'trailer',
    r'\bmovie\b': 'movie',
    r'\bmusic\b': 'music',
    r'\bcooking\b': 'food',
    r'\bfood\b': 'food',
    r'\bchallenge\b': 'challenge',
    r'\banimation\b': 'animation',
    r'\bsports?\b': 'sports',
    r'\be[ -]?sports?': 'e-sports',
    r'\bpewdiepie\b': 'pewdiepie',
    r'\byou ?tube\b': 'youtube',
    r'\bfashion\b': 'fashion',
    r'\bpolitic(s|al)?\b': 'politics',
    r'\bbasketball\b': 'basketball',
    r'\bnba\b': 'nba',
    r'\bnfl\b': 'nfl',
    r'\bfort[ -]?nite\b': 'fortnite',
    r'\bmarvel\b': 'marvel',
    r'\bsoccer\b': 'soccer',
    r'foot[ -]?ball': 'football',
    r'\bcar\b': 'car',
    r'\brap\b': 'rap',
    r'\brapper?s\b': 'rap',
    r'\bscience?s\b': 'sciencce',
    r'\beducation\b': 'education',
    r'\b(streaming|twitch)\b': 'streaming',
    r'\bmma\b': 'mma',
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
    r'\bmeme\b': 'meme',
    r'\bmr ?beast\b': 'mrbeast',
    r'\bchess\b': 'chess',
    r'\banime\b': 'anime',
    r'\bhorror\b': 'horror',
    r'\bacting\b': 'acting',
    r'\bdanc(e|ing)\b': 'dancing',
    r'\b(family|child|kid)[ -]?friendly\b': 'familyfriendly',
    r'lgbt|\bgay\b|\btrans\b|\bqueer\b': 'lgbt',
    r'\bamon?g ?us': 'among us',
    r'skibidi': 'skibidi',
    r'\bhistory\b': 'history',
    r'\bcelebrity\b': 'celebrity',
    r'\bk[ -]?pop\b': 'k-pop',
}

JAPANESE_REGEX = r'[\u3040-\u30FF\u31F0-\u31FF\uFF66-\uFF9F]'
ARABIC_REGEX = r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]'
KOREAN_REGEX = r'[\uAC00-\uD7AF\u1100-\u11FF]'
CHINESE_REGEX = r'[\u4E00-\u9FFF\u3400-\u4DBF]'
DEVANAGARI_REGEX = r'[\u0900-\u097F]'
THAI_REGEX = r'[\u0E00-\u0E7F]'
CYRILLIC_REGEX = r'[\u0400-\u04FF]'

NON_ENGLISH_REGEX_LIST = [JAPANESE_REGEX, ARABIC_REGEX, KOREAN_REGEX, CHINESE_REGEX, DEVANAGARI_REGEX, THAI_REGEX, CYRILLIC_REGEX]

# COMMAND ----------

conversion_rates_to_eur = {
        "EUR": 1,
        "AED": 4.0698,
        "AFN": 77.2396,
        "ALL": 99.486,
        "AMD": 429.0821,
        "ANG": 1.9837,
        "AOA": 1048.5841,
        "ARS": 1065.8065,
        "AUD": 1.6522,
        "AWG": 1.9837,
        "AZN": 1.885,
        "BAM": 1.9558,
        "BBD": 2.2164,
        "BDT": 132.2458,
        "BGN": 1.9558,
        "BHD": 0.4167,
        "BIF": 3201.8381,
        "BMD": 1.1082,
        "BND": 1.4373,
        "BOB": 7.6223,
        "BRL": 6.2265,
        "BSD": 1.1082,
        "BTN": 92.8397,
        "BWP": 14.7494,
        "BYN": 3.6139,
        "BZD": 2.2164,
        "CAD": 1.5054,
        "CDF": 3141.9907,
        "CHF": 0.94,
        "CLP": 1034.8253,
        "CNY": 7.8654,
        "COP": 4660.4429,
        "CRC": 573.8529,
        "CUP": 26.5967,
        "CVE": 110.265,
        "CZK": 25.1493,
        "DJF": 196.9495,
        "DKK": 7.4602,
        "DOP": 65.9131,
        "DZD": 146.8965,
        "DEM": 1.95583,
        "EGP": 53.6959,
        "ERN": 16.6229,
        "ESP": 166.386,
        "ETB": 125.7132,
        "FJD": 2.4564,
        "FKP": 0.8448,
        "FRF": 6.55957,
        "FOK": 7.4602,
        "GBP": 0.8448,
        "GEL": 2.9878,
        "GGP": 0.8448,
        "GHS": 17.833,
        "GIP": 0.8448,
        "GMD": 78.2166,
        "GNF": 9595.5348,
        "GTQ": 8.5297,
        "GYD": 230.427,
        "HKD": 8.6431,
        "HNL": 27.3378,
        "HRK": 7.5345,
        "HTG": 145.0358,
        "HUF": 395.221,
        "IDR": 17078.0349,
        "ILS": 4.1139,
        "IMP": 0.8448,
        "INR": 92.8401,
        "IQD": 1442.8884,
        "IRR": 46853.3296,
        "ISK": 152.3013,
        "JEP": 0.8448,
        "JMD": 173.1031,
        "JOD": 0.7857,
        "JPY": 156.2202,
        "KES": 143.0534,
        "KGS": 93.6238,
        "KHR": 4482.5733,
        "KID": 1.6524,
        "KMF": 491.9678,
        "KRW": 1473.6263,
        "KWD": 0.3381,
        "KYD": 0.9235,
        "KZT": 532.4556,
        "LAK": 24405.9975,
        "LBP": 99183.4517,
        "LKR": 333.4807,
        "LRD": 219.9283,
        "LSL": 19.6649,
        "LYD": 5.2488,
        "MAD": 10.8175,
        "MDL": 19.2813,
        "MGA": 4973.34,
        "MKD": 61.495,
        "MMK": 3589.7952,
        "MNT": 3772.9986,
        "MOP": 8.9019,
        "MRU": 43.7751,
        "MUR": 50.8276,
        "MVR": 17.0956,
        "MWK": 1934.3096,
        "MXN": 21.3005,
        "MYR": 4.7732,
        "MZN": 70.7864,
        "NAD": 19.6649,
        "NGN": 1831.1153,
        "NIO": 40.5863,
        "NOK": 11.7963,
        "NPR": 148.5435,
        "NZD": 1.7981,
        "OMR": 0.4261,
        "PAB": 1.1082,
        "PEN": 4.182,
        "PGK": 4.3313,
        "PHP": 62.0363,
        "PKR": 308.4382,
        "PLN": 4.2828,
        "PYG": 8554.8931,
        "QAR": 4.0338,
        "RON": 4.974,
        "RSD": 117.0549,
        "RUB": 100.5048,
        "RUR": 100.5048,
        "RWF": 1513.6375,
        "SAR": 4.1557,
        "SBD": 9.3751,
        "SCR": 15.1528,
        "SDG": 492.2299,
        "SEK": 11.3334,
        "SGD": 1.4374,
        "SHP": 0.8448,
        "SLE": 25.0451,
        "SLL": 25045.103,
        "SOS": 629.5749,
        "SRD": 33.4587,
        "SSP": 3705.6617,
        "STN": 24.5,
        "SYP": 14392.9983,
        "SZL": 19.6649,
        "THB": 36.9754,
        "TJS": 11.7919,
        "TMT": 3.8792,
        "TND": 3.367,
        "TOP": 2.5753,
        "TRY": 37.6999,
        "TRL": 37.6999,
        "TTD": 7.5794,
        "TVD": 1.6524,
        "TWD": 35.415,
        "TZS": 3006.3964,
        "UAH": 45.8354,
        "UGX": 4113.8011,
        "$": 1.1082,
        "UYU": 44.6903,
        "UZS": 14192.7842,
        "VES": 40.7781,
        "VND": 27183.4385,
        "VUV": 130.6163,
        "WST": 2.9899,
        "XAF": 655.957,
        "XCD": 2.9921,
        "XDR": 0.8214,
        "XOF": 655.957,
        "XPF": 119.332,
        "YER": 277.574,
        "ZAR": 19.6594,
        "ZMW": 29.1952,
        "ZWL": 15.4638,
        "NULL": None
    }

# COMMAND ----------

COUNTRY_CODE_TIMEZONES_MAP = {
    'BR': 'America/Sao_Paulo',
    'CA': 'America/Toronto',
    'DE': 'Europe/Berlin',
    'FR': 'Europe/Paris',
    'GB': 'Europe/London',
    'IN': 'Asia/Kolkata',
    'JP': 'Asia/Tokyo',
    'KR': 'Asia/Seoul',
    'MX': 'America/Mexico_City',
    'RU': 'Europe/Moscow',
    'US': 'America/New_York',
}

COUNTRY_CODES_TIMEZONES_MAP_BROADCAST = spark.sparkContext.broadcast(COUNTRY_CODE_TIMEZONES_MAP)