import json
from typing import Any, Dict, List

import pandas as pd

from cosing_pipeline.transform.parser import parse_page


def transform_to_bronze(raw_pages: List[Dict[str, Any]], settings) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for payload in raw_pages:
        query = payload.get("_query")
        parsed = parse_page(payload)

        for row in parsed["parsed_rows"]:
            row_copy = dict(row)
            row_copy["query_text"] = query
            row_copy["api_version"] = parsed.get("api_version")
            row_copy["terms"] = parsed.get("terms")
            row_copy["response_time"] = parsed.get("response_time")
            row_copy["page_number"] = parsed.get("page_number")
            row_copy["page_size"] = parsed.get("page_size")
            row_copy["total_results"] = parsed.get("total_results")
            row_copy["sort"] = parsed.get("sort")
            row_copy["source"] = "cosing"
            row_copy["ingest_date"] = settings.ingest_date
            row_copy["batch_month"] = settings.batch_month
            row_copy["batch_id"] = settings.batch_id
            rows.append(row_copy)

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    object_columns = df.select_dtypes(include=["object"]).columns
    for col in object_columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        df[col] = df[col].replace("", None)

    if "raw_metadata" in df.columns:
        df["raw_metadata"] = df["raw_metadata"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x
        )

    return df