from __future__ import annotations

import re
import unicodedata
from typing import Optional

import pandas as pd

# Keep aliases tiny and auditable. Expand only when manually validated.
ALIAS_RULES = {
    "NIACINAMIDE": "NICOTINAMIDE",
}


INVALID_NULL_LIKE = {"", "-", "--", "nan", "none", "null"}


def safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def nullify(value) -> Optional[str]:
    text = safe_str(value)
    if text.lower() in INVALID_NULL_LIKE:
        return None
    return text or None


def apply_alias(name: str) -> str:
    text = safe_str(name)
    if not text:
        return ""
    return ALIAS_RULES.get(text.upper(), text)


def normalize_basic(name: str) -> str:
    name = apply_alias(name)
    name = unicodedata.normalize("NFKC", safe_str(name)).lower()
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
    name = normalize_basic(name)
    if not name:
        return ""

    name = re.sub(r"^\(?[+\-]\)?\s*", "", name)
    name = re.sub(r"^\(?dl\)?\s*[-]?\s*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"^\(?[drls]\)?\s*[-]?\s*", "", name, flags=re.IGNORECASE)

    name = remove_parentheses(name)
    name = re.sub(r"[\"'`\.]", " ", name)
    name = re.sub(r"[&]+", " and ", name)
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
    name = normalize_paren_removed(name)
    if not name:
        return ""

    tokens = [token for token in re.split(r"[\s/,\-]+", name) if token]
    stopwords = {"and"}
    tokens = [token for token in tokens if token not in stopwords]
    tokens.sort()
    return " ".join(tokens)


def normalize_cas(value: str) -> str:
    text = safe_str(value)
    text = text.replace("CAS No.", "").replace("CAS No", "").strip()
    if text.lower() in INVALID_NULL_LIKE:
        return ""
    text = re.sub(r"\s+", "", text)
    return text


def build_name_keys(series: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "key_basic": series.apply(normalize_basic),
            "key_full": series.apply(normalize_full),
            "key_sorted": series.apply(normalize_word_sorted),
            "key_paren_removed": series.apply(normalize_paren_removed),
            "key_sorted_strict": series.apply(normalize_word_sorted_strict),
        }
    )
