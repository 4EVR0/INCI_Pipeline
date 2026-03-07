from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

KCIA_CSV_PATH = Path("/Users/hyeokjun/INCI_data/data/kcia_ingredient_dict.csv")
COSING_CSV_PATH = DATA_DIR / "cosing_latest_20260307_160953.csv"

OUTPUT_DIR = DATA_DIR / "mapping_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# fuzzy threshold
FUZZY_SCORE_THRESHOLD = 93
FUZZY_SCORE_REVIEW_THRESHOLD = 85