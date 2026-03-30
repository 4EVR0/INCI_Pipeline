# 크롤링 직후, bronze raw 데이터를 1차 정제하는 코드

from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime
import pandas as pd


RAW_PATH = Path("/Users/hyeokjun/INCI_data/kcia.csv")
OUTPUT_PATH = Path("kcia_ingredient_dict_rebuilt.csv")


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_dashes(text: str) -> str:
    return re.sub(r"[‐-‒–—―−]", "-", text)


def remove_parentheses(text: str) -> str:
    return re.sub(r"\([^)]*\)", "", text).strip()


def remove_slashes(text: str) -> str:
    return text.replace("/", " ")


def make_name_key(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = normalize_dashes(text)
    text = normalize_spaces(text)
    return text


def make_name_key_noparen(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = normalize_dashes(text)
    text = remove_parentheses(text)
    text = normalize_spaces(text)
    return text


def make_name_key_noparen_noslash(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = normalize_dashes(text)
    text = remove_parentheses(text)
    text = remove_slashes(text)
    text = normalize_spaces(text)
    return text


def make_name_key_normdash(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = normalize_dashes(text)
    text = normalize_spaces(text)
    return text


def make_name_key_water_syn(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = normalize_dashes(text)
    text = text.replace("purified water", "aqua")
    text = text.replace("water", "aqua")
    text = normalize_spaces(text)
    return text


def make_cas_key(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).strip()
    text = re.sub(r"\s+", "", text)
    return text


def make_norm_name(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = normalize_dashes(text)
    text = remove_parentheses(text)
    text = remove_slashes(text)
    text = re.sub(r"[^a-z0-9\s\-]", "", text)
    text = normalize_spaces(text)
    return text


def build_kcia_ingredient_dict(raw_path: Path, output_path: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_path)
    print(f"[INFO] raw rows: {len(df)}")
    print(f"[INFO] raw columns: {list(df.columns)}")

    required_cols = [
        "ingredient_code",
        "std_name_ko",
        "std_name_en",
        "cas_no",
        "old_name_ko",
        "as_of_date",
        "ingest_date",
        "batch_id",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    text_cols = [
        "std_name_ko",
        "std_name_en",
        "cas_no",
        "old_name_ko",
        "as_of_date",
        "ingest_date",
        "batch_id",
    ]
    for col in text_cols:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["ingredient_code"] = df["ingredient_code"].astype(str).str.strip()

    df = df[df["ingredient_code"] != ""].copy()

    before_dedup = len(df)
    df = df.drop_duplicates(subset=["ingredient_code"]).copy()
    print(f"[INFO] after ingredient_code dedup: {len(df)} (removed {before_dedup - len(df)})")

    df["as_of_date"] = df["as_of_date"].mask(df["as_of_date"] == "", df["ingest_date"])

    df["std_name_en_key"] = df["std_name_en"].apply(make_name_key)
    df["std_name_en_key_noparen"] = df["std_name_en"].apply(make_name_key_noparen)
    df["std_name_en_key_noparen_noslash"] = df["std_name_en"].apply(make_name_key_noparen_noslash)
    df["std_name_en_key_normdash"] = df["std_name_en"].apply(make_name_key_normdash)
    df["std_name_en_key_water_syn"] = df["std_name_en"].apply(make_name_key_water_syn)
    df["cas_no_key"] = df["cas_no"].apply(make_cas_key)
    df["norm_name"] = df["std_name_en"].apply(make_norm_name)

    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["created_at"] = now_ts
    df["updated_at"] = now_ts

    final_cols = [
        "ingredient_code",
        "std_name_ko",
        "std_name_en",
        "old_name_ko",
        "as_of_date",
        "created_at",
        "updated_at",
        "std_name_en_key",
        "std_name_en_key_noparen",
        "std_name_en_key_noparen_noslash",
        "std_name_en_key_normdash",
        "std_name_en_key_water_syn",
        "cas_no_key",
        "norm_name",
    ]

    df_final = df[final_cols].copy()
    df_final.to_csv(output_path, index=False)

    print(f"[INFO] final rows: {len(df_final)}")
    print(f"[INFO] saved to: {output_path.resolve()}")
    print(f"[INFO] as_of_date blank count: {(df_final['as_of_date'] == '').sum()}")
    print(f"[INFO] cas_no_key filled count: {(df_final['cas_no_key'] != '').sum()}")
    print(df_final.head(10))

    return df_final


if __name__ == "__main__":
    build_kcia_ingredient_dict(
        raw_path=RAW_PATH,
        output_path=OUTPUT_PATH,
    )