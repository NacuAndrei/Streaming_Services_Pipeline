# Databricks notebook source
# MAGIC %run "../shared/streaming_services_functions"

# COMMAND ----------

# MAGIC %run "../shared/constants"

# COMMAND ----------

class TimeoutException(Exception):
    pass

# COMMAND ----------

def get_show_imdb_ids(ia, titles: list):
    results = {}
    
    for title in titles:
        success = False
        retries = 0
        while not success and retries < 3:
            try:
                movies = ia.search_movie(title)
                if movies:
                    results[title] = movies[0].movieID 
                else:
                    results[title] = None
                success = True
            except Exception as e:
                retries += 1
                if retries < 3:
                    time.sleep(random.uniform(1, 2)) 
                else:
                    raise TimeoutException

    return results

# COMMAND ----------

def fetch_imdb_ids(partition):
    ia = Cinemagoer() 
    rows = list(partition) 
    titles = [row.title for row in rows]
    
    try:
        imdb_ids = get_show_imdb_ids(ia, titles)
    except TimeoutException as e:
        print(str(e))
        raise  
    
    updated_rows = []
    
    for row in rows:
        imdb_id = imdb_ids.get(row.title, None)
        
        updated_row = Row(
            show_id=row[SHOW_ID],
            type=row[CATEGORY],
            age_required=row[AGE_REQUIRED],
            title=row[TITLE],
            director=row[DIRECTOR],
            cast=row[CAST],
            country=row[COUNTRIES],
            date_added=row[DATE_ADDED],
            release_date=row[RELEASE_DATE],
            duration=row[DURATION],
            genres=row[GENRES],
            is_animation=row[IS_ANIMATION],
            platform=row[PLATFORM],
            is_scraped=row[IS_SCRAPED],
            last_updated_date = row["last_updated_date"],
            imdb_id=imdb_id
        )
        updated_rows.append(updated_row)
    
    return iter(updated_rows)

# COMMAND ----------

def get_show_data(ia, imdb_ids: list):
    results = {}

    for imdb_id in imdb_ids:
        success = False
        retries = 0
        while not success and retries < 3:
            try:

                show = ia.get_movie(imdb_id)
                show_data = {
                    IMDB_ID: int(imdb_id),
                    COUNTRIES: list_to_comma_separated_string(show.get('countries', [])),
                    RATING: float(show.get('rating', None)) if show.get('rating') else None,
                    VOTES: int(show.get('votes', None)) if show.get('votes') else None,
                    LANGUAGES: list_to_comma_separated_string(show.get('languages', [])),
                    COLOR: "Yes" if show.get('color') and show['color'][0] == 'Color' else "No",
                    TOP_250: "No" if show.get('top 250 rank', None) is None else "Yes",
                    BOX_OFFICE: show.get('box office', None),
                    CAST: list_to_comma_separated_string(get_names(show.get('cast', []))),
                    DIRECTOR: list_to_comma_separated_string(get_names(show.get('director', []))),
                    PRODUCTION_COMPANIES: list_to_comma_separated_string(get_names(show.get('production companies', []))),
                    WRITERS: list_to_comma_separated_string(get_names(show.get('writers', []))),
                    SERIES_YEARS: show.get('series years', None),
                    RELEASE_DATE: show.get('original air date', None)
                }
                
                results[imdb_id] = show_data
                success = True
            except Exception as e:
                retries += 1
                if retries < 3:
                    time.sleep(random.uniform(1, 2))
                else:
                    raise TimeoutException

    return results

# COMMAND ----------

def fetch_show_data(partition):
    ia = Cinemagoer()
    rows = list(partition)
    imdb_ids = [row[IMDB_ID] for row in rows if row[IMDB_ID]]

    try:
        shows_data = get_show_data(ia, imdb_ids)
    except Exception as e:
        print(str(e))
        raise

    updated_rows = []

    for row in rows:
        show_data = shows_data.get(row[IMDB_ID], {})
        
        updated_row = Row(
            show_id=row[SHOW_ID],
            category=row[CATEGORY],
            age_required=row[AGE_REQUIRED],
            title=row[TITLE],
            director=show_data.get(DIRECTOR) if show_data.get(DIRECTOR) is not None else row[DIRECTOR],
            cast=show_data.get(CAST) if show_data.get(CAST) is not None else row[CAST],
            countries=show_data.get(COUNTRIES) if show_data.get(COUNTRIES) is not None else row[COUNTRIES],
            date_added=row[DATE_ADDED],
            release_date=show_data.get(RELEASE_DATE) if show_data.get(RELEASE_DATE) is not None else row[RELEASE_DATE],
            duration=row[DURATION],
            genres=row[GENRES],
            is_animation=row[IS_ANIMATION],
            platform=row[PLATFORM],
            is_scraped="Yes",
            last_updated_date=row["last_updated_date"],
            imdb_id=row[IMDB_ID],
            rating=show_data.get(RATING),
            votes=show_data.get(VOTES),
            languages=show_data.get(LANGUAGES),
            color=show_data.get(COLOR),
            top_250=show_data.get(TOP_250),
            box_office=show_data.get(BOX_OFFICE),
            production_companies=show_data.get(PRODUCTION_COMPANIES),
            writers=show_data.get(WRITERS),
            series_years=show_data.get(SERIES_YEARS)
        )
        
        updated_rows.append(updated_row)

    return iter(updated_rows)