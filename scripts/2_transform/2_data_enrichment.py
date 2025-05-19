import pandas as pd
from pathlib import Path
import datetime

def is_popular(tmdb_count, imdb_votes):
    return (tmdb_count > 10000) or (imdb_votes > 10000)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "2_silver"

# TMDB files
movies_path = f"{SILVER_DIR}\\base_movies_and_shows.csv"

all_data = pd.read_csv(movies_path, sep=';', index_col=0)

# Cast imdb_votes to integer from string with commas
all_data['imdb_count'] = (all_data['imdb_count'].fillna(-1).astype(str).str.replace(",", "").astype(int))

# Replace nulls in ratings
all_data['imdb_rating'] = all_data['imdb_rating'].fillna(-1)
all_data['tmdb_rating'] = all_data['tmdb_rating'].fillna(-1)

# Replace nulls in release_date
all_data['release_date'] = all_data['release_date'].fillna('1111-11-11')

# Convert String dates (YYYY-MM-dd) in Datetime and transform to Spanish format (dd/MM/YYYY)
all_data['release_date'] = all_data['release_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%d/%m/%Y'))

# Create new column 'is_popular' based on the number of votes
all_data['is_popular'] = all_data.apply(lambda row: is_popular(row['tmdb_count'], row['imdb_count']), axis=1)

# For simplicity, pick only records with 2 ratings available
all_data = all_data[(all_data['imdb_rating'] != -1) & (all_data['tmdb_rating'] != -1)]

# Merge movies/shows with watch providers 
movies = all_data[all_data['type'] == 'movie'].copy()
shows = all_data[all_data['type'] == 'show'].copy()

BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "1_bronze"
movies_watch_providers_path = f"{BRONZE_DIR}\\TMDB_watch_providers_movies.csv"
shows_watch_providers_path = f"{BRONZE_DIR}\\TMDB_watch_providers_shows.csv"

movies_watch_providers = pd.read_csv(movies_watch_providers_path, sep=';', index_col=0)
shows_watch_providers = pd.read_csv(shows_watch_providers_path, sep=';', index_col=0)

# Modify list of values in 'genre' and 'watch_providers' to be more readable
movies["genre"] = movies["genre"].apply(lambda row: row.replace("'", "").replace("[", "").replace("]", ""))
movies_watch_providers["watch_providers"] = movies_watch_providers["watch_providers"].apply(lambda row: row.replace("'", "").replace("[", "").replace("]", ""))

shows["genre"] = shows["genre"].apply(lambda row: row.replace("'", "").replace("[", "").replace("]", ""))
shows_watch_providers["watch_providers"] = shows_watch_providers["watch_providers"].apply(lambda row: row.replace("'", "").replace("[", "").replace("]", ""))


movies = movies.merge(movies_watch_providers, on=["id"], how="left")
shows = shows.merge(shows_watch_providers, on=["id"], how="left")

enriched_data = pd.concat([movies, shows], ignore_index=True)

# Save file
enriched_data.to_csv(f"{SILVER_DIR}\\enriched_result_movies_shows.csv", sep=';')

