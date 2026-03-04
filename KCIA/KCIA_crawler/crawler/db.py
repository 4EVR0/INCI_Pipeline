import psycopg2
from psycopg2.extras import execute_values
from typing import List

from models import IngredientRow

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kcia_ingredient_dict (
    ingredient_code INTEGER PRIMARY KEY,
    std_name_ko TEXT NOT NULL,
    std_name_en TEXT NOT NULL,
    old_name_ko TEXT,
    old_name_en TEXT,
    as_of_date TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO kcia_ingredient_dict (
    ingredient_code, std_name_ko, std_name_en, old_name_ko, old_name_en, as_of_date
) VALUES %s
ON CONFLICT (ingredient_code) DO UPDATE SET
    std_name_ko = EXCLUDED.std_name_ko,
    std_name_en = EXCLUDED.std_name_en,
    old_name_ko = EXCLUDED.old_name_ko,
    old_name_en = EXCLUDED.old_name_en,
    as_of_date = EXCLUDED.as_of_date,
    updated_at = NOW();
"""

def get_conn(database_url: str):
    return psycopg2.connect(database_url)

def init_schema(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

def upsert_rows(conn, rows: List[IngredientRow], batch_size: int = 1000):
    if not rows:
        return

    tuples = [
        (
            r.ingredient_code,
            r.std_name_ko,
            r.std_name_en,
            r.old_name_ko,
            r.old_name_en,
            r.as_of_date,
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        for i in range(0, len(tuples), batch_size):
            chunk = tuples[i : i + batch_size]
            execute_values(cur, UPSERT_SQL, chunk, page_size=min(batch_size, 1000))
    conn.commit()