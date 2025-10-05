# medication_etl_src/utils/extract_composition_from_presentation_string.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple, Optional


@dataclass
class ItemComposicao:
    principio_ativo: str
    dosagem: Optional[float]
    unidade_de_medida: Optional[str]
    id_apresentacao_medicamento: str


class CompositionParser:
    """
    Parser de composições a partir do texto 'presentation'.

    Regras:
    - Corta a cauda após " X " (evita capturar volume/embalagem).
    - Suporta:
        * Pares explícitos: "1 MG + 2,5 MG + 100.000 UI/G"
        * Unidade compartilhada: "(20 + 5) MG/ML" ou "450 + 50 MG"
    - Trata decimal com vírgula e milhar com ponto.
    - Só mapeia quantidades para princípios ativos quando há correspondência 1-para-1.
      Caso contrário (ambiguidade), todos os ativos retornam dosagem/unidade = None.
    - Remove apenas **duplicatas exatas** de ativos (mantém sinônimos distintos).
    """

    _UNITS: Tuple[str, ...] = (
        # compostas com barra (priorizar)
        "MG/ML", "MCL/ML", "MCG/ML", "UG/ML", "µG/ML", "MG/G", "UI/G", "U/G", "G/G", "G/L",
        "MG/DL", "MMOL/L", "MEQ/ML",
        # simples
        "MG", "G", "MCG", "UG", "µG", "UI", "U", "UTR", "%",
    )
    _UNITS_SORTED: Tuple[str, ...] = tuple(sorted(set(_UNITS), key=len, reverse=True))
    _UNITS_RE: str = "|".join(map(re.escape, _UNITS_SORTED))

    _NUM: str = r"(?:\d{1,3}(?:\.\d{3})+|\d+)(?:[.,]\d+)?"

    _PAIR_PATTERN = re.compile(rf"(?P<num>{_NUM})\s*(?P<unit>{_UNITS_RE})")
    _SHARED_UNIT_PATTERN = re.compile(
        rf"\(?\s*(?P<numlist>{_NUM}(?:\s*\+\s*{_NUM})*)\s*\)?\s*(?P<unit>{_UNITS_RE})"
    )

    @classmethod
    def _normalize_text(cls, s: str) -> str:
        s = s.upper().replace(",", ".")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @classmethod
    def _strip_packaging_tail(cls, s: str) -> str:
        # corta no " X " (indicador comum de embalagem), para não confundir volume com composição
        return re.split(r"\sX\s", s, maxsplit=1)[0]

    @classmethod
    def _to_float(cls, num_str: str) -> float:
        # remove '.' de milhar (quando seguidos por 3 dígitos), preserva decimais com '.'
        cleaned = re.sub(r"(?<=\d)\.(?=\d{3}(?:\D|$))", "", num_str)
        return float(cleaned)

    @classmethod
    def _extract_pairs_explicit(cls, head: str) -> List[Tuple[float, str]]:
        found = cls._PAIR_PATTERN.findall(head)
        return [(cls._to_float(n), u) for (n, u) in found]

    @classmethod
    def _extract_pairs_shared_unit(cls, head: str) -> List[Tuple[float, str]]:
        m = cls._SHARED_UNIT_PATTERN.search(head)
        if not m:
            return []
        nums = [x.strip() for x in m.group("numlist").split("+") if x.strip()]
        unit = m.group("unit")
        return [(cls._to_float(x), unit) for x in nums]

    @classmethod
    def _dedup_exact(cls, items: List[str]) -> List[str]:
        """Remove apenas duplicatas exatas, preservando a ordem."""
        seen = set()
        out: List[str] = []
        for it in items:
            if it not in seen:
                out.append(it)
                seen.add(it)
        return out

    @classmethod
    def parse(
        cls,
        presentation: str,
        active_ingredients: List[str],
        id_apresentacao: str,
    ) -> List[ItemComposicao]:
        # Normaliza somente o texto da apresentação
        pres = cls._normalize_text(presentation)
        head = cls._strip_packaging_tail(pres)

        if None in active_ingredients:
            active_ingredients = [pa for pa in active_ingredients if pa is not None]

        # Preserva a grafia original dos ativos (para comparação com os testes),
        # mas remove duplicatas exatas.
        actives = cls._dedup_exact(active_ingredients)

        # Estratégia: preferir "unidade compartilhada" quando houver 2+ números;
        # caso contrário, usar "pares explícitos".
        shared = cls._extract_pairs_shared_unit(head)
        explicit = cls._extract_pairs_explicit(head)

        if len(shared) >= 2:
            pairs = shared
        elif explicit:
            pairs = explicit
        else:
            pairs = []

        # Mapeia somente se houver correspondência 1-para-1
        if len(pairs) != len(actives) or len(pairs) == 0:
            return [
                ItemComposicao(
                    principio_ativo=pa,
                    dosagem=None,
                    unidade_de_medida=None,
                    id_apresentacao_medicamento=id_apresentacao,
                )
                for pa in actives
            ]

        return [
            ItemComposicao(
                principio_ativo=pa,
                dosagem=qtd,
                unidade_de_medida=unit,
                id_apresentacao_medicamento=id_apresentacao,
            )
            for pa, (qtd, unit) in zip(actives, pairs)
        ]


def extract_composition_from_presentation_string(
    presentation: str,
    active_ingredients: List[str],
    id_apresentacao: str,
) -> List[ItemComposicao]:
    return CompositionParser.parse(presentation, active_ingredients, id_apresentacao)
