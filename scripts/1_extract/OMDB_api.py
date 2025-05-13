import requests
from dotenv import load_dotenv
import os
import difflib
import pandas as pd
from pathlib import Path

load_dotenv()

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_URL = "http://www.omdbapi.com/"

def check_authentication():
    params = {
        "apikey" : {OMDB_API_KEY}
    }
    response = requests.get(OMDB_URL, params=params)
    
    return response.status_code == 200


def get_data(type, original_title, release_year=None, threshold=0.8):
    """
        Calls OMDB API to retrieve information about a movie/show. If the name of the movie/show recieved doesn't match 
        enough with the name of the result movie/show, then it is not considered the same movie/show.
        
        - Type can be 'movie' or 'series'
    """
    
    params = {
        "apikey" : {OMDB_API_KEY},
        "type": type,
        "t" : original_title,
        "y" : release_year,
        "r" : "json"
    }
    
    response = requests.get(OMDB_URL, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if data.get("Response") == "True": # Found a possible match
            data_title = data.get("Title").lower()
            input_title = original_title.lower()
            
            similarity = difflib.SequenceMatcher(None, input_title, data_title).ratio()
            
            if similarity >= threshold:
                return data
            else:
                print(f"Low similarity: '{input_title}' vs '{data_title}' ({similarity:.2f})")
                return None
        else:
            print(f"No match found for: '{type}' - '{original_title}' - '{release_year}'")
            return None
    else:
        print(f"OMDB - Movies - Error: {response.status_code}")
        return None
        
        
if __name__ == '__main__':
    
    if check_authentication():
        print(get_data("series", "Guardians of the "))
        
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
        RAW_DIR = BASE_DIR / "data" / "1_raw"
        
        # Use TMDB data to search on OMDB and extract the IMDB rating
        movies_absolute_path = f"{RAW_DIR}\\TMDB\\TMDB_top_rated_movies.csv"
        shows_absolute_path = f"{RAW_DIR}\\TMDB\\TMDB_top_rated_shows.csv"

        TMDB_movies = pd.read_csv(movies_absolute_path, sep=';', index_col=0)
        TMDB_shows = pd.read_csv(shows_absolute_path, sep=';', index_col=0)
        

    else:
        print('ERROR: Connection to API failed')
        