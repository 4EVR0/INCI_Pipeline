from dataclasses import dataclass


@dataclass
class ExtractionStats:
    final_query_count: int
    oversized_query_count: int
    raw_page_count: int
    raw_result_count: int


@dataclass
class ValidationResult:
    is_valid: bool
    message: str
