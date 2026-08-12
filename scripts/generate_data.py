import json
from pathlib import Path

cidades = ["Salvador", "Lençóis", "Porto Seguro", "Ilhéus", "Itacaré", "Morro de São Paulo", "Cairu", "Mucugê", "Palmeiras", "Andaraí", "Jacobina", "Juazeiro", "Paulo Afonso", "Prado", "Santa Cruz Cabrália", "Valença", "Cachoeira", "São Félix", "Mata de São João", "Nova Viçosa"]
tipos = {
    "Praia": ("Enseada", "mar calmo, faixa de areia e pôr do sol", "banho de mar e descanso"),
    "Cachoeira": ("Queda", "águas cristalinas cercadas por mata nativa", "banho e contemplação"),
    "Trilha": ("Caminho", "paisagens naturais, mirantes e vegetação regional", "caminhada e aventura"),
    "Museu": ("Memória", "acervo sobre cultura, arte e história baiana", "visita cultural"),
    "Centro Histórico": ("Largo", "casarios, igrejas e tradições locais", "passeio histórico"),
    "Parque Ecológico": ("Reserva", "área verde, fauna local e espaços de contemplação", "ecoturismo tranquilo"),
    "Mirante": ("Vista", "vista panorâmica marcante da paisagem", "fotografia e pôr do sol"),
    "Mercado Cultural": ("Sabores", "artesanato, culinária regional e música", "experiência gastronômica"),
    "Passeio Náutico": ("Rota das Águas", "ilhas, piscinas naturais e costa preservada", "passeio de barco"),
    "Sítio Arqueológico": ("Vestígios", "formações rochosas e registros históricos", "exploração educativa"),
}
qualificadores = [
    "do Sol", "das Palmeiras", "da Lua", "do Descobrimento", "das Águas",
    "do Dendê", "dos Coqueiros", "da Maré", "do Farol", "da Mata",
    "do Cacau", "da Chapada", "do Recôncavo", "dos Ventos", "da Aurora",
    "do Sertão", "da Esperança", "da Alegria", "do Horizonte", "da Conquista",
    "dos Encantos", "da Harmonia", "do Sossego", "da Liberdade", "da Bahia",
]

dados = []
for cidade_i, cidade in enumerate(cidades):
    for tipo_i, (tipo, (prefixo, paisagem, atividade)) in enumerate(tipos.items()):
        for variante, qualificador in enumerate(qualificadores):
            gratis = (cidade_i + tipo_i + variante) % 3 != 0
            acessivel = (cidade_i * 2 + tipo_i + variante) % 4 < 2
            dados.append({
                "nome": f"{prefixo} {qualificador} de {cidade}",
                "cidade": cidade,
                "tipo": tipo,
                "is_gratis": gratis,
                "tem_acessibilidade": acessivel,
                "descricao": f"Local fictício para demonstração em {cidade}, com {paisagem}. Indicado para {atividade}{' e com estrutura acessível' if acessivel else ''}."
            })

destino = Path(__file__).resolve().parents[1] / "dados_bahia.json"
destino.write_text(json.dumps(dados, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"{len(dados)} locais gravados em {destino}")
