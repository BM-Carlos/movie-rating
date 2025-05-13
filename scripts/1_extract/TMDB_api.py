import requests
import os
from dotenv import load_dotenv   
import pandas as pd
from tqdm import tqdm
from pathlib import Path

load_dotenv()

TMDB_TOKEN = os.getenv("TMDB_TOKEN")

HEADERS = {
    "accept": "application/json",
    "Authorization": "Bearer " + str(TMDB_TOKEN)
}


def check_authentication(headers):
    url = "https://api.themoviedb.org/3/authentication"
    response = requests.get(url, headers=headers)
    return response.status_code == 200


def get_movies_on_page(headers, language, getNumPages = False, page=1):
    """
        Used to get a specific page of records from the movies API in TMDB. To avoid overload when calling the API, the 
        list of movies/shows are splitted in pages you have to iterate over. 
        This function is used to iterate over all the pages in the method "get_top_rated_movies".
    """
    
    errors = []
    url = f"https://api.themoviedb.org/3/movie/top_rated?language={language}&page={page}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        results = data.get("results", [])
        
        if getNumPages:
            num_pages = data.get("total_pages", [])
            return pd.DataFrame(results), num_pages, errors
        else:
            return pd.DataFrame(results), errors
    
    else: 
        errors.append(f"TMDB - Movies - Error {response.status_code} on page {page}")
        return pd.DataFrame(), errors


def get_top_rated_movies(headers, language='es-ES'):
    total_result, num_pages, errors = get_movies_on_page(headers, language, getNumPages = True, page=1)
    
    for i in tqdm(range(2, num_pages), desc=f"TMDB - Retrieving movies [{language}] "):
        page_result, errors = get_movies_on_page(headers, language, page=i)
        
        total_result = pd.concat([total_result, page_result], ignore_index=True)
    
    total_result["type"] = 'movie'
    
    return total_result, errors


def get_shows_on_page(headers, language, getNumPages = False, page=1):
    """
        Used to get a specific page of records from the shows API in TMDB. To avoid overload when calling the API, the 
        list of movies/shows are splitted in pages you have to iterate over. 
        This function is used to iterate over all the pages in the method "get_top_rated_shows".
    """
    
    errors = []
    url = f"https://api.themoviedb.org/3/tv/top_rated?language={language}&page={page}"

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        results = data.get("results", [])
        
        if getNumPages:
            num_pages = data.get("total_pages", [])
            return pd.DataFrame(results), num_pages, errors
        else:
            return pd.DataFrame(results), errors
    
    else: 
        errors.append(f"TMDB - Shows - Error {response.status_code} on page {page}")
        return pd.DataFrame(), errors


def get_top_rated_shows(headers, language='es-ES'):
    total_result, num_pages, errors = get_shows_on_page(headers, language, getNumPages = True, page=1)
    
    for i in tqdm(range(2, num_pages), desc=f"TMDB - Retrieving shows  [{language}] "):
        page_result, errors = get_shows_on_page(headers, language, page=i)
        
        total_result = pd.concat([total_result, page_result], ignore_index=True)
    
    total_result["type"] = 'show'
    
    return total_result, errors



if __name__ == '__main__':
        
    if check_authentication(HEADERS):
        
        # Extraction in es-ES
        movies_ES, movies_errors = get_top_rated_movies(HEADERS)
        shows_ES, shows_errors = get_top_rated_shows(HEADERS)

        # Extraction in en-US (later needed to search in OMDB)
        movies_EN, movies_errors = get_top_rated_movies(HEADERS, language='en-US')
        shows_EN, shows_errors = get_top_rated_shows(HEADERS, language='en-US')
        
        # Merge ES-EN
        movies_titles_EN = movies_EN[["id", "title"]].rename(columns={"title": "title_EN"})
        result_movies = movies_ES.merge(movies_titles_EN, on='id', how='left').rename(columns={"title": "title_ES"})
        
        shows_titles_EN = shows_EN[["id", "name"]].rename(columns={"name": "title_EN"})
        result_shows = shows_ES.merge(shows_titles_EN, on='id', how='left').rename(columns={"name": "title_ES"})
        
        # Errors during process
        total_errors = movies_errors + shows_errors
        print("Errors during the extraction: ", total_errors)
        
        # Save files
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
        RAW_DIR = BASE_DIR / "data" / "1_raw"

        movies_absolute_path = f"{RAW_DIR}\\TMDB\\TMDB_top_rated_movies.csv"
        shows_absolute_path = f"{RAW_DIR}\\TMDB\\TMDB_top_rated_shows.csv"
        
        # Movies and shows have different columns, need to be merged later
        result_movies.to_csv(movies_absolute_path, sep=';')
        result_shows.to_csv(shows_absolute_path, sep=';')

        print(f"TMDB - Movies file saved on: {movies_absolute_path}")
        print(f"TMDB - Shows file saved on: {shows_absolute_path}")

    else: 
        print('ERROR: Connection to API failed')