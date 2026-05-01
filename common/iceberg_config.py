"""
INCI Pipeline - Iceberg Catalog 설정

환경변수:
    ICEBERG_WAREHOUSE_PATH : Iceberg 메타데이터 저장 S3 경로
                             예) s3://my-bucket/inci_iceberg_metadata
    AWS_REGION             : AWS 리전 (기본값: ap-northeast-2)
"""

import os

from pyiceberg.catalog import load_catalog


class INCIIceberg:
    CATALOG_NAME = "glue"
    DATABASE = "inci_db"

    # KCIA Bronze
    KCIA_BRONZE_CURRENT_TABLE = f"{DATABASE}.kcia_bronze_current"
    KCIA_BRONZE_HISTORY_TABLE = f"{DATABASE}.kcia_bronze_history"

    # CosIng Bronze
    COSING_BRONZE_CURRENT_TABLE = f"{DATABASE}.cosing_bronze_current"
    COSING_BRONZE_HISTORY_TABLE = f"{DATABASE}.cosing_bronze_history"

    # Silver matched_final
    SILVER_MATCHED_CURRENT_TABLE = f"{DATABASE}.silver_kcia_cosing_matched_current"
    SILVER_MATCHED_HISTORY_TABLE = f"{DATABASE}.silver_kcia_cosing_matched_history"

    # Silver graphrag_map
    SILVER_GRAPHRAG_CURRENT_TABLE = f"{DATABASE}.silver_kcia_cosing_graphrag_current"
    SILVER_GRAPHRAG_HISTORY_TABLE = f"{DATABASE}.silver_kcia_cosing_graphrag_history"

    # Silver fuzzy_review
    SILVER_FUZZY_REVIEW_TABLE = f"{DATABASE}.silver_kcia_cosing_fuzzy_review_current"

    # Gold ingredients
    GOLD_INGREDIENTS_CURRENT_TABLE = f"{DATABASE}.gold_kcia_cosing_ingredients_current"
    GOLD_INGREDIENTS_HISTORY_TABLE = f"{DATABASE}.gold_kcia_cosing_ingredients_history"

    @staticmethod
    def get_catalog():
        region = os.getenv("AWS_REGION", "ap-northeast-2")
        warehouse = os.getenv("ICEBERG_WAREHOUSE_PATH")
        if not warehouse:
            raise ValueError("ICEBERG_WAREHOUSE_PATH is not set")
        return load_catalog(
            INCIIceberg.CATALOG_NAME,
            **{
                "type": "glue",
                "warehouse": warehouse,
                "s3.region": region,
            },
        )
