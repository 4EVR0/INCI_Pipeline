from typing import Any, Dict, List, Optional


def first_or_none(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def join_list(value, sep=" | "):
    if isinstance(value, list):
        joined = sep.join(str(v).strip() for v in value if str(v).strip())
        return joined or None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_result_item(item: Dict[str, Any]) -> Dict[str, Any]:
    metadata = item.get("metadata", {}) or {}
    item_type_raw = first_or_none(metadata.get("itemType"))

    return {
        "reference_uuid": item.get("reference"),
        "database": item.get("database"),
        "database_label": item.get("databaseLabel"),
        "language": item.get("language"),
        "content_type": item.get("contentType"),
        "weight": item.get("weight"),
        "substance_id": first_or_none(metadata.get("substanceId")),
        "inci_name": first_or_none(metadata.get("inciName")),
        "common_ingredient_name": first_or_none(metadata.get("nameOfCommonIngredientsGlossary")),
        "inn_name": first_or_none(metadata.get("innName")),
        "inci_usa_name": first_or_none(metadata.get("inciUsaName")),
        "chemical_name": first_or_none(metadata.get("chemicalName")),
        "chemical_description": first_or_none(metadata.get("chemicalDescription")),
        "cas_no": first_or_none(metadata.get("casNo")),
        "ec_no": first_or_none(metadata.get("ecNo")),
        "function_names": join_list(metadata.get("functionName")),
        "function_count": len(metadata.get("functionName", []) or []),
        "cosmetic_restriction": join_list(metadata.get("cosmeticRestriction")),
        "other_restrictions": join_list(metadata.get("otherRestrictions")),
        "maximum_concentration": join_list(metadata.get("maximumConcentration")),
        "annex_no": join_list(metadata.get("annexNo")),
        "identified_ingredient": join_list(metadata.get("identifiedIngredient")),
        "classification_information": first_or_none(metadata.get("classificationInformation")),
        "item_type": item_type_raw,
        "status": first_or_none(metadata.get("status")),
        "official_journal_publication": first_or_none(metadata.get("officialJournalPublication")),
        "perfuming": first_or_none(metadata.get("perfuming")),
        "ph_eur_name": first_or_none(metadata.get("phEurName")),
        "es_ingest_date": first_or_none(metadata.get("esDA_IngestDate")),
        "es_queue_date": first_or_none(metadata.get("esDA_QueueDate")),
        "current_version": first_or_none(metadata.get("currentVersion")),
        "corporate_search_version": first_or_none(metadata.get("corporate-search-version")),
        "raw_metadata": metadata,
    }


def parse_page(payload: Dict[str, Any]) -> Dict[str, Any]:
    results = payload.get("results", []) or []
    parsed_rows: List[Dict[str, Any]] = [parse_result_item(item) for item in results]

    return {
        "api_version": payload.get("apiVersion"),
        "terms": payload.get("terms"),
        "response_time": payload.get("responseTime"),
        "total_results": payload.get("totalResults", 0),
        "page_number": payload.get("pageNumber"),
        "page_size": payload.get("pageSize"),
        "sort": payload.get("sort"),
        "parsed_rows": parsed_rows,
    }
