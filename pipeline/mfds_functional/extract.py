"""고시 PDF 텍스트(pdftotext -layout 결과)에서 기능성 원료 목록을 뽑는다.

출처 두 가지를 합친다.
- 「기능성화장품 기준 및 시험방법」 각조: 별표 2 미백, 3 주름개선, 4 자외선차단, 8 여드름, 9 탈모.
  (별표 5는 2·3 원료의 복합 제제라 새 원료가 없고, 6 염모·7 제모는 스킨케어 추천과 무관해 제외)
- 「기능성화장품 심사에 관한 규정」 [별표 4] 자료제출이 생략되는 기능성화장품의 종류:
  1 자외선차단, 2 미백, 3 주름개선, 6 여드름의 성분·함량. (4 염모, 5 제모 제외)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

FUNCTIONS = {
    "whitening": "미백",
    "anti_wrinkle": "주름개선",
    "uv_protection": "자외선차단",
    "acne": "여드름",
    "hair_loss": "탈모",
}
STANDARD_APPENDIX = {2: "whitening", 3: "anti_wrinkle", 4: "uv_protection", 8: "acne", 9: "hair_loss"}
ANNEX4_SECTIONS = {1: "uv_protection", 2: "whitening", 3: "anti_wrinkle", 6: "acne"}
# [별표 4] 6항: 인체세정용 제품류(씻어내는 제형)에 한정된 여드름 기능성
CONDITIONS = {"acne": "인체세정용(씻어내는) 제품에 한정"}

_DOSAGE_SUFFIX = re.compile(r"\s*(로션제|액제|크림제|침적\s*마스크|고형제|액\s*\([^)]*\)|\d+\s*%)\s*$")
_KOR_HEADING = re.compile(r"[가-힣0-9(),·ㆍ\-\s/.%]+")
_ENG_LINE = re.compile(r"[A-Za-z0-9(),\-\s/.'’%]+")


def norm_name(name: str) -> str:
    """병합 키: 공백·가운뎃점 제거."""
    return re.sub(r"[\s·ㆍ∙․]", "", name)


def base_name(name: str) -> str:
    """제형 접미사와 '및 그 염류'를 떼고 공백을 정리한 원료명."""
    name = re.sub(r"\s*및\s*그\s*염류\s*$", "", name.strip())
    previous = None
    while previous != name:
        previous, name = name, _DOSAGE_SUFFIX.sub("", name)
    return re.sub(r"\s+", "", name)


@dataclass
class FunctionalIngredient:
    kor_name: str
    function: str
    aliases: set[str] = field(default_factory=set)
    sources: list[str] = field(default_factory=list)
    max_content: str = ""

    @property
    def condition(self) -> str:
        return CONDITIONS.get(self.function, "")


def _appendix_bounds(lines: list[str]) -> list[tuple[int, int, int]]:
    starts = [(i, int(m.group(1))) for i, line in enumerate(lines)
              if (m := re.match(r"^\[별표\s*(\d+)\]", line.strip()))]
    starts.append((len(lines), 0))
    return [(s, e, n) for (s, n), (e, _) in zip(starts, starts[1:])]


def parse_standard(text: str) -> list[tuple[str, str, str]]:
    """기준 고시 각조 제목 → (원료명, 기능, 출처). 제목은 가운데 정렬 한글명 + 다음 줄 영문명."""
    lines = text.split("\n")
    found: list[tuple[str, str, str]] = []
    for start, end, number in _appendix_bounds(lines):
        function = STANDARD_APPENDIX.get(number)
        if not function:
            continue
        seg = lines[start:end]
        for i, line in enumerate(seg[:-1]):
            title = line.strip()
            if (len(line) - len(line.lstrip()) < 15 or not 2 <= len(title) <= 40
                    or not _KOR_HEADING.fullmatch(title)):
                continue
            nxt = next((x.strip() for x in seg[i + 1:i + 4] if x.strip()), "")
            if not _ENG_LINE.fullmatch(nxt) or not re.search(r"[A-Za-z]{3}", nxt):
                continue
            found.append((base_name(title), function, f"기준고시 별표{number}"))
    return found


def parse_annex4(text: str) -> list[tuple[str, str, str, str]]:
    """[별표 4] 표 → (원료명, 기능, 출처, 함량). '<삭 제>' 행은 건너뛴다."""
    found: list[tuple[str, str, str, str]] = []
    section = None
    for line in text.split("\n"):
        if m := re.match(r"^\s*(\d)\.\s+\S", line):
            section = int(m.group(1))
            continue
        function = ANNEX4_SECTIONS.get(section or 0)
        if not function:
            continue
        if re.search(r"<\s*삭\s*제\s*>", line):
            continue
        # 함량 칸은 숫자(또는 '산으로')로 시작해 %·IU/g을 포함한다. PDF 텍스트는 원료명 안에도
        # 공백이 끼어 있어('살리실 릭 애씨드') 함량 칸을 기준으로 자른다.
        m = re.match(r"^\s*(\d{1,2})\s+(\S.*?)\s+((?:산으로\s*)?[\d.,]+\s*(?:[～~\-]\s*[\d.,]+\s*)?(?:%|IU/g).*?)\s*$",
                     line)
        if not m:
            continue
        found.append((m.group(2).strip(), function, f"심사규정 별표4-{section}번 {m.group(1)}", m.group(3).strip()))
    return found


def merge(standard: list[tuple[str, str, str]],
          annex4: list[tuple[str, str, str, str]]) -> list[FunctionalIngredient]:
    """같은 기능의 같은 원료를 합친다. 괄호 안 이명(예: 폴리실리콘-15(디메치코디에칠벤잘말로네이트))도 키로 쓴다."""
    items: dict[tuple[str, str], FunctionalIngredient] = {}
    alias_index: dict[tuple[str, str], tuple[str, str]] = {}

    def upsert(name: str, function: str, source: str, content: str = "", aliases: set[str] | None = None):
        keys = {norm_name(name)} | {norm_name(a) for a in (aliases or set())}
        key = next((alias_index[(k, function)] for k in keys if (k, function) in alias_index), None)
        if key is None:
            key = (norm_name(name), function)
            items[key] = FunctionalIngredient(kor_name=name, function=function)
        item = items[key]
        item.aliases |= (aliases or set()) - {item.kor_name}
        if source not in item.sources:
            item.sources.append(source)
        if content and not item.max_content:
            item.max_content = content
        for k in keys:
            alias_index[(k, function)] = key

    for raw, function, source, content in annex4:
        name = base_name(raw)
        alias = set()
        if m := re.fullmatch(r"(.+?)\((.+)\)", name):
            name, alias = m.group(1), {m.group(2)}
        upsert(name, function, source, content, alias)
    for name, function, source in standard:
        upsert(name, function, source)
    return sorted(items.values(), key=lambda x: (list(FUNCTIONS).index(x.function), x.kor_name))
