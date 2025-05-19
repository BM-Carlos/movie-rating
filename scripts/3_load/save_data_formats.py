import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "2_silver"
GOLD_DIR = BASE_DIR / "data" / "3_gold"

enriched_data_path = f"{SILVER_DIR}\\enriched_result_movies_shows.csv"

enriched_data = pd.read_csv(enriched_data_path, sep=';', index_col=0)

final_data_name = f"{GOLD_DIR}\\movies_and_shows_result"

# Save as CSV
enriched_data.to_csv(f'{final_data_name}.csv', sep=';', index=False)

# Save as parquet
enriched_data.to_parquet(f'{final_data_name}.parquet', index=False)

# Save as json
enriched_data.to_json(f'{final_data_name}.json', orient='records', lines=True)

# Save as xslx (excel)
enriched_data.to_excel(f'{final_data_name}.xlsx', index=False)