import math
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from config import OUTPUT_DIR, PAGE_SIZE
from src.client import CosIngClient
from src.parser import parse_page
from src.utils import dump_json, normalize_name


class CosIngCollector:
    def __init__(self, log_dir: Path):
        self.client = CosIngClient(log_dir=log_dir)
        self.output_dir = OUTPUT_DIR
        self.logger = self.client.logger

    def fetch_first_page(self, text: str = "*") -> Dict:
        payload = self.client.search(page_number=1, page_size=PAGE_SIZE, text=text)
        return parse_page(payload)

    def collect_single_query(
        self,
        text: str,
        save_raw_pages: bool = False,
        raw_dir: Optional[Path] = None,
    ) -> List[Dict]:
        first = self.fetch_first_page(text=text)

        total_results = int(first["total_results"] or 0)
        page_size = int(first["page_size"] or PAGE_SIZE)
        total_pages = math.ceil(total_results / page_size) if total_results > 0 else 0

        self.logger.info(
            f"[COLLECT] query={text}, total_results={total_results}, page_size={page_size}, total_pages={total_pages}"
        )

        all_rows: List[Dict] = []

        if total_results == 0:
            return all_rows

        all_rows.extend(first["parsed_rows"])

        if save_raw_pages and raw_dir is not None:
            dump_json(first, raw_dir / f"{self._safe_query_name(text)}_page_0001.json")

        for page_no in tqdm(range(2, total_pages + 1), desc=f"Collect {text}", leave=False):
            payload = self.client.search(page_number=page_no, page_size=page_size, text=text)
            parsed = parse_page(payload)
            rows = parsed["parsed_rows"]

            if not rows:
                self.logger.warning(f"[EMPTY_PAGE] query={text}, page={page_no}")
                break

            all_rows.extend(rows)

            if save_raw_pages and raw_dir is not None:
                dump_json(
                    parsed,
                    raw_dir / f"{self._safe_query_name(text)}_page_{page_no:04d}.json"
                )

        return all_rows

    def collect_by_queries(
        self,
        queries: List[str],
        save_raw_pages: bool = False,
        max_queries: Optional[int] = None,
    ) -> pd.DataFrame:
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_dir = self.output_dir / f"raw_pages_{run_ts}"

        if save_raw_pages:
            raw_dir.mkdir(parents=True, exist_ok=True)

        if max_queries is not None:
            queries = queries[:max_queries]

        all_rows: List[Dict] = []

        for query in tqdm(queries, desc="Query groups"):
            rows = self.collect_single_query(
                text=query,
                save_raw_pages=save_raw_pages,
                raw_dir=raw_dir,
            )
            all_rows.extend(rows)

        df = pd.DataFrame(all_rows)

        if df.empty:
            self.logger.warning("Collected dataframe is empty.")
            return df

        df["inci_name_norm"] = df["inci_name"].apply(normalize_name)
        df["cas_no_norm"] = df["cas_no"].astype(str).str.strip()
        df["ec_no_norm"] = df["ec_no"].astype(str).str.strip()

        if "substance_id" in df.columns:
            substance_mask = df["substance_id"].notna() & (df["substance_id"].astype(str).str.strip() != "")
            df_with_id = df[substance_mask].drop_duplicates(subset=["substance_id"], keep="first")
            df_without_id = df[~substance_mask].copy()
        else:
            df_with_id = pd.DataFrame(columns=df.columns)
            df_without_id = df.copy()

        if not df_without_id.empty:
            df_without_id = df_without_id.drop_duplicates(
                subset=["inci_name_norm", "cas_no_norm"],
                keep="first"
            )

        df_final = pd.concat([df_with_id, df_without_id], ignore_index=True)

        self.logger.info(f"Collected rows before dedup = {len(df)}")
        self.logger.info(f"Collected rows after dedup = {len(df_final)}")

        return df_final

    def save_outputs(self, df: pd.DataFrame) -> Dict[str, Path]:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        csv_path = self.output_dir / f"cosing_latest_{ts}.csv"
        parquet_path = self.output_dir / f"cosing_latest_{ts}.parquet"
        sample_path = self.output_dir / f"cosing_latest_sample_{ts}.csv"

        df_to_save = df.copy()

        if "raw_metadata" in df_to_save.columns:
            df_to_save["raw_metadata"] = df_to_save["raw_metadata"].astype(str)

        df_to_save.to_csv(csv_path, index=False, encoding="utf-8-sig")
        df_to_save.to_parquet(parquet_path, index=False)
        df_to_save.head(200).to_csv(sample_path, index=False, encoding="utf-8-sig")

        self.logger.info(f"Saved CSV: {csv_path}")
        self.logger.info(f"Saved Parquet: {parquet_path}")
        self.logger.info(f"Saved Sample CSV: {sample_path}")

        return {
            "csv": csv_path,
            "parquet": parquet_path,
            "sample": sample_path,
        }

    @staticmethod
    def _safe_query_name(text: str) -> str:
        name = text.replace("*", "STAR")
        name = name.replace("/", "_SLASH_")
        name = name.replace("(", "_LPAREN_")
        name = name.replace(")", "_RPAREN_")
        name = name.replace("-", "_DASH_")
        name = name.replace(" ", "_SPACE_")
        return name