import unittest

from services.query_parser import apply_deterministic_filters, normalize

CITIES = ["Salvador", "Lençóis", "Cachoeira"]
TYPES = ["Praia", "Cachoeira", "Trilha", "Parque Ecológico", "Centro Histórico"]


def parse(query: str, llm: dict | None = None) -> dict:
    return apply_deterministic_filters(query, llm, CITIES, TYPES)


class QueryParserTests(unittest.TestCase):
    def test_normalizes_accents_punctuation_and_spaces(self):
        self.assertEqual(normalize("  LENÇÓIS, grátis! "), "lencois gratis")

    def test_extracts_explicit_filters(self):
        result = parse("parque pago em Lençóis e com acessibilidade")
        self.assertEqual(result["cidade"], "Lençóis")
        self.assertEqual(result["tipo"], "Parque Ecológico")
        self.assertIs(result["is_gratis"], False)
        self.assertIs(result["tem_acessibilidade"], True)

    def test_resolves_capital_alias(self):
        result = parse("praias bonitas perto da capital")
        self.assertEqual(result["cidade"], "Salvador")
        self.assertEqual(result["tipo"], "Praia")

    def test_negative_accessibility_has_precedence(self):
        self.assertIs(parse("parque não acessível")["tem_acessibilidade"], False)
        self.assertIs(parse("trilha sem acessibilidade")["tem_acessibilidade"], False)

    def test_rejecting_paid_means_free(self):
        self.assertIs(parse("não quero lugar pago")["is_gratis"], True)

    def test_conflicting_price_terms_do_not_create_filter(self):
        self.assertIsNone(parse("pode ser gratuito ou pago")["is_gratis"])

    def test_does_not_match_substrings_inside_words(self):
        self.assertIsNone(parse("conhecer o litoralista local").get("tipo"))

    def test_discards_invalid_llm_hard_filters(self):
        result = parse("um lugar tranquilo", {
            "cidade": "Cidade inventada", "tipo": "Categoria inventada",
            "is_gratis": True, "tem_acessibilidade": True,
            "termo_semantico": "lugar tranquilo",
        })
        self.assertIsNone(result["cidade"])
        self.assertIsNone(result["tipo"])
        self.assertIsNone(result["is_gratis"])
        self.assertIsNone(result["tem_acessibilidade"])


if __name__ == "__main__":
    unittest.main()
