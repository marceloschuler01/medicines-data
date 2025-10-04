# medication_etl_src/utils/extract_composition_info_from_presentation_string.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class ItemComposicao:
    principio_ativo: str
    quantidade: float | None
    unidade: str

def extract_composition_info_from_presentation_string(
    presentation: str,
    active_ingredients: List[str],
) -> List[ItemComposicao]:
    """
    Extrai a composição do medicamento.
    """
    return CompositionParser.parse(presentation, active_ingredients)


class CompositionParser:
    """
    Parser de composições de medicamentos a partir da string de 'presentation'.

    - Corta cauda de embalagem após " X " (ex.: "X 5 ML").
    - Suporta:
        * Pares explícitos por componente: "1 MG + 2,5 MG + 0,25 MG + 100.000 UI/G"
        * Unidade compartilhada: "(20 + 5) MG/ML"
    - Trata separador de milhar (ponto) e decimal (vírgula/ponto).
    - Deduplica nomes de princípios ativos quando um é substring de outro,
      preservando o mais específico (mais longo) e mantendo a ordem original.
    """

    # Unidades (as compostas com "/" primeiro para priorização no regex)
    _UNITS: Tuple[str, ...] = (
        "MG/ML", "MCG/ML", "UG/ML", "µG/ML", "MG/G", "UI/G", "G/L", "MG/DL", "MMOL/L", "MEQ/ML",
        "MG", "G", "MCG", "UG", "µG", "UI", "%",
    )
    _UNITS_RE: str = "|".join(map(re.escape, _UNITS))

    # número com milhar opcional (ponto) e decimal opcional (ponto ou vírgula)
    _NUM: str = r"(?:\d{1,3}(?:\.\d{3})+|\d+)(?:[.,]\d+)?"

    # Padrões:
    # 1) lista de pares num+unidade ("1 MG", "2,5 MG", "100.000 UI/G", ...)
    _PAIR_PATTERN = re.compile(rf"(?P<num>{_NUM})\s*(?P<unit>{_UNITS_RE})")
    # 2) unidade compartilhada ("(20+5) MG/ML")
    _SHARED_UNIT_PATTERN = re.compile(
        rf"\(?\s*(?P<numlist>{_NUM}(?:\s*\+\s*{_NUM})*)\s*\)?\s*(?P<unit>{_UNITS_RE})"
    )

    @classmethod
    def _normalize(cls, s: str) -> str:
        s = s.upper().replace(",", ".")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @classmethod
    def _strip_packaging_tail(cls, s: str) -> str:
        # Corta no " X " (indicador comum de embalagem), para não confundir volume do frasco com composição
        return re.split(r"\sX\s", s, maxsplit=1)[0]

    @classmethod
    def _to_float(cls, num_str: str) -> float:
        # Remove '.' de milhar (quando seguidos por 3 dígitos) mas mantém decimais com ponto
        cleaned = re.sub(r"(?<=\d)\.(?=\d{3}(?:\D|$))", "", num_str)
        return float(cleaned)

    @classmethod
    def _dedup_ingredients(cls, names: List[str]) -> List[str]:
        """
        Remove sinônimos onde um é substring de outro; mantém o mais longo.
        Retorna na ordem original, contendo apenas os escolhidos.
        """
        ordered = sorted(set(names), key=len, reverse=True)  # maiores primeiro
        kept: List[str] = []
        for name in ordered:
            if not any(name in longer for longer in kept):
                kept.append(name)
        return [n for n in names if n in kept]

    @classmethod
    def _extract_pairs(cls, head: str) -> List[Tuple[float, str]]:
        """
        Tenta primeiro extrair pares explícitos (n+unidade), ex.: "1 MG + 2.5 MG + 100000 UI/G".
        Se não houver, tenta o formato "n(+n...) UNIDADE" (unidade compartilhada), ex.: "(20+5) MG/ML".
        Retorna lista de (quantidade, unidade) na ordem encontrada.
        """
        # 1) pares explícitos
        pairs = cls._PAIR_PATTERN.findall(head)
        if pairs:
            return [(cls._to_float(num), unit) for (num, unit) in pairs]

        # 2) unidade compartilhada
        m = cls._SHARED_UNIT_PATTERN.search(head)
        if not m:
            return []
        nums = [x.strip() for x in m.group("numlist").split("+") if x.strip()]
        unit = m.group("unit")
        return [(cls._to_float(x), unit) for x in nums]

    @classmethod
    def parse(cls, presentation: str, active_ingredients: List[str]) -> List[ItemComposicao]:
        pres = cls._normalize(presentation)
        head = cls._strip_packaging_tail(pres)

        pairs = cls._extract_pairs(head)
        if not pairs:
            return []

        actives = cls._dedup_ingredients([cls._normalize(a) for a in active_ingredients])

        items: List[ItemComposicao] = []
        for i, pa in enumerate(actives):
            if i < len(pairs):
                q, u = pairs[i]
            else:
                q, u = None, pairs[-1][1]  # se sobrar ativo sem quantidade explícita
            items.append(ItemComposicao(principio_ativo=pa, quantidade=q, unidade=u))
        return items

