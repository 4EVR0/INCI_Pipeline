from dataclasses import dataclass
from typing import Optional


@dataclass
class KciaRawRow:
    ingredient_code: str
    std_name_ko: Optional[str]
    std_name_en: Optional[str]
    cas_no: Optional[str]
    old_name_ko: Optional[str]
    as_of_date: Optional[str]


@dataclass
class KciaBronzeRow:
    ingredient_code: str
    std_name_ko: Optional[str]
    std_name_en: Optional[str]
    cas_no: Optional[str]
    old_name_ko: Optional[str]
    as_of_date: Optional[str]

    source: str
    ingest_date: str
    batch_month: str
    batch_id: str
    batch_job: str
    batch_date: str  # ISO 8601 UTC, e.g. "2026-04-16T15:30:42+00:00"


@dataclass
class CrawlStats:
    total_expected: int
    total_collected: int


@dataclass
class ValidationResult:
    is_valid: bool
    message: str