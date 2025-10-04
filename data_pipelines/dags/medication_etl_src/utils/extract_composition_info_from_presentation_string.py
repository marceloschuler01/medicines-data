import re
from dataclasses import dataclass
from typing import List


@dataclass
class ItemComposicao:
    principio_ativo: str
    quantidade: float | None
    unidade: str


_UNITS = (
    # unidades compostas com barra (prioridade)
    "MG/ML", "MCG/ML", "UG/ML", "µG/ML", "MG/G", "UI/G", "G/L", "MG/DL", "MMOL/L", "MEQ/ML",
    # simples
    "MG", "G", "MCG", "UG", "µG", "UI", "%"
)
_UNITS_RE = "|".join(map(re.escape, _UNITS))

# número com milhar opcional (ponto) e decimal opcional (ponto ou vírgula)
_NUM = r"(?:\d{1,3}(?:\.\d{3})+|\d+)(?:[.,]\d+)?"

# Padrões
PAIR_PATTERN = re.compile(rf"(?P<num>{_NUM})\s*(?P<unit>{_UNITS_RE})")
SHARED_UNIT_PATTERN = re.compile(rf"\(?\s*(?P<numlist>{_NUM}(?:\s*\+\s*{_NUM})*)\s*\)?\s*(?P<unit>{_UNITS_RE})")


def _normalize(s: str) -> str:
    s = s.upper().replace(",", ".")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_packaging_tail(s: str) -> str:
    # corta no " X " (indicador comum de embalagem), para não confundir volume do frasco com composição
    return re.split(r"\sX\s", s, maxsplit=1)[0]


def _to_float(num_str: str) -> float:
    # remove '.' de milhar (quando seguidos por 3 dígitos) mas mantém decimais com ponto
    cleaned = re.sub(r"(?<=\d)\.(?=\d{3}(?:\D|$))", "", num_str)
    return float(cleaned)


def _dedup_ingredients(names: List[str]) -> List[str]:
    # remove sinônimos onde um é substring de outro; mantém o mais longo
    ordered = sorted(set(names), key=len, reverse=True)
    kept: List[str] = []
    for name in ordered:
        if not any(name in longer for longer in kept):
            kept.append(name)
    # volta à ordem original, mantendo apenas os escolhidos
    return [n for n in names if n in kept]


def _extract_pairs(head: str) -> List[tuple[float, str]]:
    """
    Tenta primeiro extrair pares explícitos (n+unidade), ex.: "1 MG + 2.5 MG + 100000 UI/G".
    Se não houver, tenta o formato "n(+n...) UNIDADE" (unidade compartilhada), ex.: "(20+5) MG/ML".
    """
    # 1) pares explícitos
    pairs = PAIR_PATTERN.findall(head)
    if pairs:
        # Exemplo: [('1', 'MG'), ('2.5', 'MG'), ('100.000', 'UI/G')]
        return [(_to_float(num), unit) for (num, unit) in pairs]

    # 2) unidade compartilhada
    m = SHARED_UNIT_PATTERN.search(head)
    if not m:
        return []
    nums = [x.strip() for x in m.group("numlist").split("+") if x.strip()]
    unit = m.group("unit")
    return [(_to_float(x), unit) for x in nums]


def extract_composition_info_from_presentation_string(presentation: str, active_ingredients: List[str]) -> List[ItemComposicao]:
    pres = _normalize(presentation)
    head = _strip_packaging_tail(pres)

    pairs = _extract_pairs(head)
    if not pairs:
        return []

    # dedup de ativos (evita "CIPROFLOXACINO" + "CLORIDRATO DE CIPROFLOXACINO")
    actives = _dedup_ingredients([_normalize(a) for a in active_ingredients])

    # mapeamento 1:1 em ordem
    items: List[ItemComposicao] = []
    for i, pa in enumerate(actives):
        q, u = (pairs[i] if i < len(pairs) else (None, pairs[-1][1]))  # se faltar quantidade, None; unidade herdada da última conhecida
        items.append(ItemComposicao(principio_ativo=pa, quantidade=q, unidade=u))
    return items
