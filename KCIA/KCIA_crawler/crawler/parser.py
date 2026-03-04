import re
import math
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup

from models import IngredientRow

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_total_count(soup: BeautifulSoup) -> Optional[int]:
    text = soup.get_text(" ", strip=True)

    m = re.search(r"총\s*([0-9,]+)\s*건", text)
    if m:
        return int(m.group(1).replace(",", ""))

    m = re.search(r"(검색결과|결과)\s*([0-9,]+)\s*건", text)
    if m:
        return int(m.group(2).replace(",", ""))

    return None

def find_result_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    best = None

    for t in tables:
        header_text = normalize_text(t.get_text(" ", strip=True))
        if any(k in header_text for k in ["성분코드", "국문명", "영문명", "INCI", "코드"]):
            best = t
            break

    return best or (tables[0] if tables else None)

def parse_page_rows(html: str) -> List[IngredientRow]:
    soup = BeautifulSoup(html, "html.parser")
    table = find_result_table(soup)
    if not table:
        title = normalize_text(soup.title.get_text()) if soup.title else ""
        raise RuntimeError(f"Result table not found. Page title: {title}")

    rows = table.find_all("tr")
    parsed: List[IngredientRow] = []

    for tr in rows:
        cols = tr.find_all(["td", "th"])
        if not cols:
            continue

        row_text = normalize_text(" ".join(c.get_text(" ", strip=True) for c in cols))
        if any(h in row_text for h in ["성분코드", "국문명", "영문명", "INCI", "번호"]):
            continue

        cells = [normalize_text(c.get_text(" ", strip=True)) for c in cols]
        if not cells:
            continue

        code_str = cells[0].replace(",", "")
        if not re.fullmatch(r"\d+", code_str):
            continue

        ingredient_code = int(code_str)
        std_name_ko = cells[1] if len(cells) > 1 else ""
        std_name_en = cells[2] if len(cells) > 2 else ""

        old_name_ko = cells[3] if len(cells) > 3 and cells[3] else None
        old_name_en = cells[4] if len(cells) > 4 and cells[4] else None

        as_of_date = None
        for c in cells[5:]:
            if re.search(r"\d{4}[-.]\d{2}[-.]\d{2}", c):
                as_of_date = c.replace(".", "-")
                break

        parsed.append(
            IngredientRow(
                ingredient_code=ingredient_code,
                std_name_ko=std_name_ko,
                std_name_en=std_name_en,
                old_name_ko=old_name_ko,
                old_name_en=old_name_en,
                as_of_date=as_of_date,
            )
        )

    return parsed

def compute_total_pages(first_page_html: str, per_page_guess: int = 10) -> Tuple[int, int, int]:
    soup = BeautifulSoup(first_page_html, "html.parser")
    total = parse_total_count(soup)
    if total is None:
        raise RuntimeError("Could not parse total count (e.g., '총 21709건'). Page structure might have changed.")

    rows = parse_page_rows(first_page_html)
    per_page = len(rows) if len(rows) > 0 else per_page_guess
    total_pages = math.ceil(total / per_page)
    return total, per_page, total_pages