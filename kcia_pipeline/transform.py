from typing import List
from models import KciaRawRow, KciaBronzeRow


def clean_str(s):
    if s is None:
        return None
    s = s.strip()
    return s if s else None


def transform_to_bronze(raw_rows: List[KciaRawRow], settings):
    seen = set()
    result = []

    for r in raw_rows:
        key = r.ingredient_code
        if key in seen:
            continue
        seen.add(key)

        result.append(
            KciaBronzeRow(
                ingredient_code=clean_str(r.ingredient_code),
                std_name_ko=clean_str(r.std_name_ko),
                std_name_en=clean_str(r.std_name_en),
                old_name_ko=clean_str(r.old_name_ko),
                old_name_en=clean_str(r.old_name_en),
                as_of_date=clean_str(r.as_of_date),
                source="kcia",
                ingest_date=settings.ingest_date,
                batch_id=settings.batch_id,
            )
        )

    return result