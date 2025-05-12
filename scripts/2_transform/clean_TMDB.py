import pandas as pd
from pathlib import Path

BASE_DIR = Path.cwd().parent.parent  # <- cwd = scripts/extract/
RAW_DIR = BASE_DIR / "data" / "1_raw"

movies_absolute_path = f"{RAW_DIR}\\TMDB\\TMDB_top_rated_movies.csv"
shows_absolute_path = f"{RAW_DIR}\\TMDB\\TMDB_top_rated_shows.csv"

movies = pd.read_csv(movies_absolute_path)
shows = pd.read_csv(shows_absolute_path)