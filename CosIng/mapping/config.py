from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output" / "mapping_results"

KCIA_FILE = INPUT_DIR / "kcia_ingredient_dict_clean.csv"
COSING_FILE = INPUT_DIR / "cosing_clean_fixed.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FUZZY_AUTO_THRESHOLD = 95
FUZZY_REVIEW_THRESHOLD = 90