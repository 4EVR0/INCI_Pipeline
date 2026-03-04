from dataclasses import dataclass
from typing import Optional

@dataclass
class IngredientRow:
    ingredient_code: int
    std_name_ko: str
    std_name_en: str
    old_name_ko: Optional[str] = None
    old_name_en: Optional[str] = None
    as_of_date: Optional[str] = None