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


def get_movies_on_page(headers, getNumPages = False, page=1):
    """
        Used to get a specific page of records from the movies API in TMDB. To avoid overload when calling the API, the 
        list of movies/shows are splitted in pages you have to iterate over. 
        This function is used to iterate over all the pages in the method "get_top_rated_movies".
    """
    
    errors = []
    url = "https://api.themoviedb.org/3/movie/top_rated?language=es-ES&page="+str(page)
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
        errors.append(f"Error {response.status_code} on page {page}")
        return pd.DataFrame(), errors


def get_top_rated_movies(headers):
    total_result, num_pages, errors = get_movies_on_page(headers, getNumPages = True, page=1)
    
    for i in tqdm(range(2, num_pages), desc="TMDB - Retrieving movies "):
        page_result, errors = get_movies_on_page(headers, page=i)
        
        total_result = pd.concat([total_result, page_result], ignore_index=True)
        
    total_result["type"] = 'movie'
    
    return total_result, errors


def get_shows_on_page(headers, getNumPages = False, page=1):
    """
        Used to get a specific page of records from the shows API in TMDB. To avoid overload when calling the API, the 
        list of movies/shows are splitted in pages you have to iterate over. 
        This function is used to iterate over all the pages in the method "get_top_rated_shows".
    """
    
    errors = []
    url = "https://api.themoviedb.org/3/tv/top_rated?language=es-ES&page="+str(page)

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
        errors.append(f"Error {response.status_code} on page {page}")
        return pd.DataFrame(), errors


def get_top_rated_shows(headers):
    total_result, num_pages, errors = get_shows_on_page(headers, getNumPages = True, page=1)
    
    for i in tqdm(range(2, num_pages), desc="TMDB - Retrieving shows  "):
        page_result, errors = get_shows_on_page(headers, page=i)
        
        total_result = pd.concat([total_result, page_result], ignore_index=True)
    
    total_result["type"] = 'show'
    
    return total_result, errors


if __name__ == '__main__':
    
    if check_authentication(HEADERS):
        movies, movies_errors = get_top_rated_movies(HEADERS)
        shows, shows_errors = get_top_rated_shows(HEADERS)
        
        BASE_DIR = Path(__file__).resolve().parent.parent.parent  # <- cwd = scripts/extract/
        RAW_DIR = BASE_DIR / "data" / "1_raw"

        movies_absolute_path = f"{RAW_DIR}\\TMDB\\TMDB_top_rated_movies.csv"
        shows_absolute_path = f"{RAW_DIR}\\TMDB\\TMDB_top_rated_shows.csv"

        movies.to_csv(movies_absolute_path, sep=';')
        shows.to_csv(shows_absolute_path, sep=';')

        print(f"TMDB - Movies file saved on: {movies_absolute_path}")
        print(f"TMDB - Shows file saved on: {shows_absolute_path}")

    else: 
        print('ERROR: Access to API failed')