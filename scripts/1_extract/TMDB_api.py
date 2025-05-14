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

def get_movie_genres(headers, language='es-ES'):
    url = f"https://api.themoviedb.org/3/genre/movie/list?language={language}"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data.get('genres'))
    
    else:
        return f"Error getting MOVIE genres - Status Code: {response.status_code}"
    
def get_shows_genres(headers, language='es-ES'):
    url = f"https://api.themoviedb.org/3/genre/tv/list?language={language}"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data.get('genres'))
    
    else:
        return f"Error getting SHOWS genres - Status Code: {response.status_code}"

if __name__ == '__main__':
    
    if check_authentication(HEADERS):
        
        # Extraction in es-ES
        movies_ES, movies_errors = get_top_rated_movies(HEADERS)
        shows_ES, shows_errors = get_top_rated_shows(HEADERS)

        # Extraction in en-US (later needed to search in OMDB)
        movies_EN, movies_errors = get_top_rated_movies(HEADERS, language='en-US')
        shows_EN, shows_errors = get_top_rated_shows(HEADERS, language='en-US')
        
        # Merge all of es-ES with titles in en-US
        movies_titles_EN = movies_EN[["id", "title"]].rename(columns={"title": "title_EN"})
        result_movies = movies_ES.merge(movies_titles_EN, on='id', how='left').rename(columns={"title": "title_ES"}).sort_values(by=['vote_average', 'vote_count'], ascending=False)
        
        shows_titles_EN = shows_EN[["id", "name"]].rename(columns={"name": "title_EN"})
        result_shows = shows_ES.merge(shows_titles_EN, on='id', how='left').rename(columns={"name": "title_ES"}).sort_values(by=['vote_average', 'vote_count'], ascending=False)
        
        # Show errors during process
        total_errors = movies_errors + shows_errors
        print("Errors during the extraction: ", total_errors)
        
        # Extraction of movie-shows genres
        movie_genres = get_movie_genres(HEADERS)
        shows_genres = get_shows_genres(HEADERS)
        
        # Save files
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
        RAW_DIR = BASE_DIR / "data" / "1_bronze"

        movies_absolute_path = f"{RAW_DIR}\\TMDB_top_rated_movies.csv"
        shows_absolute_path = f"{RAW_DIR}\\TMDB_top_rated_shows.csv"
        movies_genres_path = f"{RAW_DIR}\\TMDB_movies_genres.csv"
        shows_genres_path = f"{RAW_DIR}\\TMDB_shows_genres.csv"
        
        # Movies and shows have different columns, need to be merged later
        result_movies.to_csv(movies_absolute_path, sep=';')
        result_shows.to_csv(shows_absolute_path, sep=';')
        movie_genres.to_csv(movies_genres_path, sep=';')
        shows_genres.to_csv(shows_genres_path, sep=';')
        
        print(f"TMDB - Movies file saved on: {movies_absolute_path}")
        print(f"TMDB - Shows file saved on: {shows_absolute_path}")
        
    else: 
        print('ERROR: Connection to API failed')
    