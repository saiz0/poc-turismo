"""Valida os artefatos que podem ser conferidos sem subir os containers."""

import json
import py_compile
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FIELDS = {
    "nome",
    "cidade",
    "tipo",
    "is_gratis",
    "tem_acessibilidade",
    "descricao",
}


def validate_python() -> None:
    paths = [ROOT / "app.py", ROOT / "coordinator.py", ROOT / "worker.py", *ROOT.glob("services/*.py"), *ROOT.glob("scripts/*.py"), *ROOT.glob("tests/*.py")]
    for path in paths:
        py_compile.compile(str(path), doraise=True)


def validate_rules() -> None:
    rules = json.loads((ROOT / "config" / "query_rules.json").read_text(encoding="utf-8"))
    assert set(rules) == {"city_aliases", "type_aliases", "boolean_filters"}
    assert set(rules["boolean_filters"]) == {"is_gratis", "tem_acessibilidade"}
    for boolean_rule in rules["boolean_filters"].values():
        assert set(boolean_rule) == {"true", "false"}
        assert all(boolean_rule.values()), "Listas de termos booleanos não podem estar vazias"


def validate_data() -> None:
    data = json.loads((ROOT / "dados_bahia.json").read_text(encoding="utf-8"))
    assert len(data) == 5_000, f"Esperados 5.000 locais; encontrados {len(data)}"
    assert all(set(item) == REQUIRED_FIELDS for item in data), "Há registros com campos inesperados"
    assert len({item["nome"] for item in data}) == len(data), "Os nomes devem ser únicos"
    assert all(isinstance(item["is_gratis"], bool) for item in data), "is_gratis deve ser booleano"
    assert all(isinstance(item["tem_acessibilidade"], bool) for item in data), "tem_acessibilidade deve ser booleano"

    cities = Counter(item["cidade"] for item in data)
    categories = Counter(item["tipo"] for item in data)
    assert len(cities) == 20 and set(cities.values()) == {250}, "Distribuição de cidades inválida"
    assert len(categories) == 10 and set(categories.values()) == {500}, "Distribuição de categorias inválida"


if __name__ == "__main__":
    validate_python()
    validate_rules()
    validate_data()
    print("Projeto válido: Python compilado e 5.000 locais verificados.")
