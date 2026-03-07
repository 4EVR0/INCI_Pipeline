import re
import pandas as pd


def normalize_name(value: str) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).strip().upper()

    # 여러 공백 정리
    text = re.sub(r"\s+", " ", text)

    # 대표 특수문자 제거
    text = re.sub(r"[-/,().']", "", text)

    # ampersand 정리
    text = text.replace("&", "AND")

    # 다시 공백 정리
    text = re.sub(r"\s+", " ", text).strip()

    return text if text else None


def normalize_name_compact(value: str) -> str | None:
    """
    더 공격적인 비교용 정규화
    - 공백도 제거
    """
    text = normalize_name(value)
    if text is None:
        return None
    return text.replace(" ", "")


def normalize_cas(value: str) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text in {"", "-", "NAN", "NONE"}:
        return None

    return text


def pick_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {col.lower(): col for col in df.columns}

    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None