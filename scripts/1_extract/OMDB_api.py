import requests
from dotenv import load_dotenv
import os
import difflib
import pandas as pd
from pathlib import Path
import datetime
from tqdm import tqdm
import logging
import time

load_dotenv()

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_URL = "http://www.omdbapi.com/"

logging.basicConfig(
    filename="movie-rating/logs/api_extraction.log",   
    level=logging.INFO,             
    format="%(asctime)s - %(levelname)s - %(message)s" 
)

tqdm.pandas() # Bar progress on pandas

def check_authentication():
    params = {
        "apikey" : {OMDB_API_KEY}
    }
    response = requests.get(OMDB_URL, params=params)
    
    return response.status_code == 200


def get_data(data_type, original_title, release_year=None, threshold=0.6):
    """
        Calls OMDB API to retrieve information about a movie/show. If the name of the movie/show recieved doesn't match 
        enough with the name of the result movie/show, then it is not considered the same movie/show.
        
        - Type can be 'movie' or 'series'
    """
    
    params = {
        "apikey" : OMDB_API_KEY,
        "type": data_type,
        "t" : original_title,
        "y" : release_year,
        "r" : "json"
    }
    
    response = requests.get(OMDB_URL, params=params)
    
    if response.status_code == 200:
        
        data = response.json()
        if data.get("Response") == "True":
            data_title = data.get("Title").lower()
            input_title = original_title.lower()
            
            similarity = difflib.SequenceMatcher(None, input_title, data_title).ratio()
            
            if similarity >= threshold:
                logging.info(f"Match acepted: '{input_title}' vs '{data_title}' ({similarity:.2f})")
                return data
            else:
                logging.info(f"Low similarity: '{input_title}' vs '{data_title}' ({similarity:.2f})")
                return None
        else:
            logging.info(f"No match found for: '{data_type}' - '{original_title}' - '{release_year}'")
            return None
    else:
        logging.info(f"OMDB - {data_type} - {original_title} - Error: {response.status_code}")
        return None
    

def get_rating_and_votes(data_type, title, release_year=None):
    result = get_data(data_type, title, release_year)
    
    if result != None:
        return {
            "imdb_rating": result.get('imdbRating'),
            "imdb_votes": result.get('imdbVotes')
        }
        
    else:
        return {
            "imdb_rating": None,
            "imdb_votes": None
        }


if __name__ == '__main__':
    
    if check_authentication():        
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
        RAW_DIR = BASE_DIR / "data" / "1_bronze"

        movies_absolute_path = f"{RAW_DIR}\\TMDB_top_rated_movies.csv"
        shows_absolute_path = f"{RAW_DIR}\\TMDB_top_rated_shows.csv"


        TMDB_movies = pd.read_csv(movies_absolute_path, sep=';', index_col=0)
        TMDB_shows = pd.read_csv(shows_absolute_path, sep=';', index_col=0)

        # Fill NAs on date
        TMDB_movies.fillna({"release_date":'1111-11-11'}, inplace=True) 
        TMDB_shows.fillna({"first_air_date":'1111-11-11'}, inplace=True)

        # Extract year from the date
        TMDB_movies["year"] = TMDB_movies["release_date"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").date().year)
        TMDB_shows["year"] = TMDB_shows["first_air_date"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").date().year)

        # OMDB has a limit of 1000 API requests for Free accounts
        movies_enrichment = TMDB_movies[["id", "title_EN", "year"]][:1000]
        shows_enrichment = TMDB_shows[["id", "title_EN", "year"]][:1000]
        
        
        """ 
        # With progress bar
        
        movies_enrichment['imdb_rating'] = movies_enrichment.progress_apply(lambda x: get_rating("movie", x.loc["title_EN"], x.loc["year"]), axis=1)
        time.sleep(60) # Reset API timing
        shows_enrichment['imdb_rating'] = shows_enrichment.progress_apply(lambda x: get_rating("series", x.loc["title_EN"], x.loc["year"]), axis=1)
        """
        
        # Without progress bar
        print("-> Processing...")
        movies_ratings_votes = movies_enrichment.apply(lambda x: get_rating_and_votes("movie", x["title_EN"], x["year"]), axis=1, result_type='expand')
        movies_enrichment = pd.concat([movies_enrichment, movies_ratings_votes], axis=1)
        print("\tPart 1 finished")
        time.sleep(60) # Reset API timing
        shows_ratings_votes = shows_enrichment.apply(lambda x: get_rating_and_votes("series", x["title_EN"], x["year"]), axis=1, result_type='expand')
        shows_enrichment = pd.concat([shows_enrichment, shows_ratings_votes], axis=1)
        print("\tPart 2 finished \n-> Done!")
        
        movies_enrichment = movies_enrichment[['id', 'imdb_rating', 'imdb_votes']].rename(columns={"id": "tmdb_id"})
        shows_enrichment = shows_enrichment[['id', 'imdb_rating', 'imdb_votes']].rename(columns={"id": "tmdb_id"})
        
        # Save files
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
        RAW_DIR = BASE_DIR / "data" / "1_bronze"

        movies_absolute_path = f"{RAW_DIR}\\OMDB_imdb_rating_movies.csv"
        shows_absolute_path = f"{RAW_DIR}\\OMDB_imdb_rating_shows.csv"

        # Movies and shows have different columns, need to be merged later
        movies_enrichment.to_csv(movies_absolute_path, sep=';')
        shows_enrichment.to_csv(shows_absolute_path, sep=';')

        print(f"OMDB - Movies file saved on: {movies_absolute_path}")
        print(f"OMDB - Shows file saved on: {shows_absolute_path}")
        
    else:
        print('ERROR: Connection to API failed')
        