import re
import pandas as pd
from .alias_rules import ALIAS_RULES


def safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def apply_alias(name: str) -> str:
    name = safe_str(name)
    if not name:
        return ""
    key = name.strip().upper()
    return ALIAS_RULES.get(key, name)


def normalize_basic(name: str) -> str:
    name = apply_alias(name)
    name = safe_str(name).lower()
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def remove_parentheses(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r"\(.*?\)", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def normalize_separators(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r"[/,\-]+", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def normalize_full(name: str) -> str:
    name = normalize_basic(name)
    name = remove_parentheses(name)
    name = normalize_separators(name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def normalize_paren_removed(name: str) -> str:
    """
    괄호/광학표기/약한 특수문자 제거 강화
    예:
    (-)-alpha-bisabolol -> alpha bisabolol
    Riboflavin (Vitamin B2) -> riboflavin
    """
    name = apply_alias(name)
    name = normalize_basic(name)
    if not name:
        return ""

    # 광학이성질체/앞쪽 부호성 표기 제거
    name = re.sub(r"^\(?[+\-]\)?\s*", "", name)
    name = re.sub(r"^\(?dl\)?\s*[-]?\s*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"^\(?[drls]\)?\s*[-]?\s*", "", name, flags=re.IGNORECASE)

    # 괄호 제거
    name = remove_parentheses(name)

    # 약한 특수문자 공백화
    name = re.sub(r"[\"'`\.]", " ", name)
    name = re.sub(r"[&]+", " and ", name)

    # separator 통일
    name = normalize_separators(name)

    name = re.sub(r"\s+", " ", name)
    return name.strip()


def normalize_word_sorted(name: str) -> str:
    name = normalize_full(name)
    if not name:
        return ""
    tokens = [token for token in name.split() if token]
    tokens.sort()
    return " ".join(tokens)


def normalize_word_sorted_strict(name: str) -> str:
    """
    strict 토큰 정렬 키
    - 괄호/광학표기 제거 강화 버전 기반
    - 빈 토큰 제거
    - 완전 동일 토큰 집합일 때만 exact로 붙이기 위한 용도
    """
    name = normalize_paren_removed(name)
    if not name:
        return ""

    tokens = [token for token in re.split(r"[\s/,\-]+", name) if token]

    # 너무 약한 stopword 최소 제거
    stopwords = {"and"}
    tokens = [t for t in tokens if t not in stopwords]

    tokens.sort()
    return " ".join(tokens)


def build_name_keys(series: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({
        "key_basic": series.apply(normalize_basic),
        "key_full": series.apply(normalize_full),
        "key_sorted": series.apply(normalize_word_sorted),
        "key_paren_removed": series.apply(normalize_paren_removed),
        "key_sorted_strict": series.apply(normalize_word_sorted_strict),
    })