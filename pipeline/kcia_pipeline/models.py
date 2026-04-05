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


@dataclass
class CrawlStats:
    total_expected: int
    total_collected: int


@dataclass
class ValidationResult:
    is_valid: bool
    message: str