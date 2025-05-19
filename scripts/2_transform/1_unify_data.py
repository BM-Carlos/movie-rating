import pandas as pd
from pathlib import Path
import ast

def parse_list(string_list):
    # This is necesary since TMDB API sends strings of lists
    try:
        return ast.literal_eval(string_list)
    except:
        return []
    
def get_genre(genre_ids, list_of_genres):
    return list(list_of_genres[list_of_genres['id'].isin(genre_ids)]['name'])


BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "1_bronze"

# TMDB files
movies_path = f"{BRONZE_DIR}\\TMDB_top_rated_movies.csv"
shows_path = f"{BRONZE_DIR}\\TMDB_top_rated_shows.csv"

TMDB_movies = pd.read_csv(movies_path, sep=';', index_col=0)
TMDB_shows = pd.read_csv(shows_path, sep=';', index_col=0)

# TMDB movie-shows genres
movies_genres_path = f"{BRONZE_DIR}\\TMDB_movies_genres.csv"
shows_genres_path = f"{BRONZE_DIR}\\TMDB_shows_genres.csv"

movies_genres = pd.read_csv(movies_genres_path, sep=';', index_col=0)
shows_genres = pd.read_csv(shows_genres_path, sep=';', index_col=0)

# IMDB files
imbd_rating_movies_path = f"{BRONZE_DIR}\\OMDB_imdb_rating_movies.csv"
imbd_rating_shows_path = f"{BRONZE_DIR}\\OMDB_imdb_rating_shows.csv"

IMBD_rating_movies = pd.read_csv(imbd_rating_movies_path, sep=';', index_col=0)
IMBD_rating_shows = pd.read_csv(imbd_rating_shows_path, sep=';', index_col=0)

# Add found IMDB ratings to TMDB data
movies_final_result = TMDB_movies.merge(IMBD_rating_movies, how='left', left_on='id', right_on='tmdb_id')
shows_final_result = TMDB_shows.merge(IMBD_rating_shows, how='left', left_on='id', right_on='tmdb_id')

# Cast string-lists to lists (['[18, 80]'] -> [18, 80])
movies_final_result['genre_ids'] = movies_final_result['genre_ids'].apply(parse_list)
shows_final_result['genre_ids'] = shows_final_result['genre_ids'].apply(parse_list)

# Create column 'genre' with real names instead of IDs
movies_final_result['genre'] = movies_final_result['genre_ids'].apply(lambda x: get_genre(x, movies_genres))
shows_final_result['genre'] = shows_final_result['genre_ids'].apply(lambda x: get_genre(x, shows_genres))

# Unify movies and shows in a single dataset
## Select valuable columns from movies
movies_final_result = movies_final_result[["type", "id", "title_ES", "overview", "release_date", "genre", "vote_average", "vote_count", "imdb_rating", "imdb_votes"]]
movies_final_result.rename(columns={"title_ES":"title", "vote_average":"tmdb_rating", "vote_count":"tmdb_count", "imdb_votes":"imdb_count"}, inplace=True)

## Select valuable columns from shows
shows_final_result = shows_final_result[["type", "id", "title_ES", "overview", "first_air_date", "genre", "vote_average", "vote_count", "imdb_rating", "imdb_votes"]]
shows_final_result.rename(columns={"title_ES":"title", "first_air_date":"release_date", "vote_average":"tmdb_rating", "vote_count":"tmdb_count", "imdb_votes":"imdb_count"}, inplace=True)

# Merge shows and movies (can be distiguished for 'type' column)
all_data = pd.concat([movies_final_result, shows_final_result], ignore_index=True)

# Save files
BASE_DIR = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "2_silver"

# TMDB files
all_data_path = f"{SILVER_DIR}\\base_movies_and_shows.csv"

all_data.to_csv(all_data_path, sep=';')