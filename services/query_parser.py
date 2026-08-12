import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable

DEFAULT_RULES_PATH = Path(__file__).resolve().parents[1] / "config" / "query_rules.json"


def normalize(value: str) -> str:
    value = "".join(
        character for character in unicodedata.normalize("NFD", value.casefold())
        if unicodedata.category(character) != "Mn"
    )
    value = re.sub(r"[^\w\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def load_rules(path: Path = DEFAULT_RULES_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _term_spans(query: str, terms: Iterable[str]) -> list[tuple[int, int]]:
    spans = []
    for term in terms:
        normalized_term = normalize(term)
        if normalized_term:
            pattern = r"(?<!\w)" + re.escape(normalized_term).replace(r"\ ", r"\s+") + r"(?!\w)"
            spans.extend(match.span() for match in re.finditer(pattern, query))
    return spans


def _remove_contained(spans: list[tuple[int, int]], opposite: list[tuple[int, int]]) -> list[tuple[int, int]]:
    return [span for span in spans if not any(
        other[0] <= span[0] and span[1] <= other[1] and other != span
        for other in opposite
    )]


def _detect_boolean(query: str, rules: dict[str, list[str]]) -> bool | None:
    true_spans = _term_spans(query, rules.get("true", []))
    false_spans = _term_spans(query, rules.get("false", []))
    original_true, original_false = true_spans, false_spans
    true_spans = _remove_contained(original_true, original_false)
    false_spans = _remove_contained(original_false, original_true)
    if bool(true_spans) == bool(false_spans):
        return None
    return bool(true_spans)


def _first_alias(query: str, aliases: dict[str, list[str]]) -> str | None:
    candidates = []
    for canonical, terms in aliases.items():
        for start, end in _term_spans(query, terms):
            candidates.append((start, -(end - start), canonical))
    return min(candidates)[2] if candidates else None


def apply_deterministic_filters(
    query: str,
    llm_filters: dict[str, Any] | None,
    available_cities: Iterable[str],
    available_types: Iterable[str],
    rules: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rules = rules or load_rules()
    result = dict(llm_filters or {})
    normalized_query = normalize(query)
    cities = list(available_cities)
    types = set(available_types)

    city_aliases: dict[str, list[str]] = {}
    for alias, canonical in rules["city_aliases"].items():
        city_aliases.setdefault(canonical, []).append(alias)
    for city in cities:
        city_aliases.setdefault(city, []).append(city)
    explicit_city = _first_alias(normalized_query, city_aliases)
    if explicit_city:
        result["cidade"] = explicit_city
    elif result.get("cidade") not in cities:
        result["cidade"] = None

    type_aliases = {canonical: aliases for canonical, aliases in rules["type_aliases"].items() if canonical in types}
    explicit_type = _first_alias(normalized_query, type_aliases)
    if explicit_type:
        result["tipo"] = explicit_type
    elif result.get("tipo") not in types:
        result["tipo"] = None

    for field, boolean_rules in rules["boolean_filters"].items():
        result[field] = _detect_boolean(normalized_query, boolean_rules)

    result["termo_semantico"] = str(result.get("termo_semantico") or query)
    return result
