# medication_etl_src/utils/extract_composition_info_from_presentation_string.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple, Optional


@dataclass
class ItemComposicao:
    principio_ativo: str
    dosagem: Optional[float]
    unidade_de_medida: str
    id_apresentacao_medicamento: str


class CompositionParser:
    """
    Parser de composições a partir da string 'presentation'.

    Suporta:
      - Pares explícitos: "2,5 MG + 150 UTR"
      - Unidade compartilhada: "(20+5) MG/ML" ou "450 + 50 MG"
    Regras:
      - Corta a cauda após ' X ' (evita volume/quantidade da embalagem)
      - Corta ao encontrar primeiro marcador de forma/embalagem (COM, POM, CREM, CT, BL, ...)
      - Normaliza vírgula para ponto, trata milhar com ponto.
      - Dedup de ativos (remove duplicatas e substrings; mantém o mais longo).
    """

    # Unidades conhecidas (compostas primeiro para priorização no regex)
    _UNITS: Tuple[str, ...] = (
        # compostas
        "MG/ML", "MCG/ML", "UG/ML", "µG/ML", "MG/G", "UI/G", "U/G", "G/L", "MG/DL", "MMOL/L", "MEQ/ML", "G/G", "MCL/ML",
        # simples
        "MG", "G", "MCG", "UG", "µG", "UI", "UTR", "%"
    )
    _UNITS_RE: str = "|".join(map(re.escape, _UNITS))

    # número com milhar opcional (.) e decimal opcional (.,)
    _NUM: str = r"(?:\d{1,3}(?:\.\d{3})+|\d+)(?:[.,]\d+)?"

    # Padrões principais
    _PAIR_PATTERN = re.compile(rf"(?P<num>{_NUM})\s*(?P<unit>{_UNITS_RE})")
    # >>> Exige pelo menos um "+" dentro da lista numérica (evita casar "1 MG" como se fosse compartilhada)
    _SHARED_UNIT_PATTERN = re.compile(
        rf"\(?\s*(?P<numlist>{_NUM}(?:\s*\+\s*{_NUM})+)\s*\)?\s*(?P<unit>{_UNITS_RE})"
    )

    # Marcadores comuns de forma/embalagem para cortar a cabeça de composição
    _FORM_MARKERS = [
        " COM ", " CAPS ", " CAP ", " CPR ", " COMP ", " DRG ",
        " POM ", " CREM ", " GEL ", " SOL OFT ", " SOL ", " SUSP ", " XAROPE ", " COL ", " LOCAO ",
        " LIB ", " RETARD ", " PROL ",
        " CT ", " CX ", " FR ", " BG ", " BL ", " AL ",
    ]
    _FORM_SPLIT_RE = re.compile("|".join(map(re.escape, _FORM_MARKERS)))

    @classmethod
    def _normalize(cls, s: str) -> str:
        s = s.upper().replace(",", ".")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @classmethod
    def _strip_packaging_tail(cls, s: str) -> str:
        # Corta em " X " (quantidade/volume da embalagem)
        return re.split(r"\sX\s", s, maxsplit=1)[0]

    @classmethod
    def _strip_after_form_marker(cls, s: str) -> str:
        # Corta no primeiro marcador de forma/embalagem
        m = cls._FORM_SPLIT_RE.search(s)
        return s[:m.start()] if m else s

    @classmethod
    def _to_float(cls, num_str: str) -> float:
        # Remove pontos de milhar (quando seguidos por 3 dígitos) e preserva decimal com ponto
        cleaned = re.sub(r"(?<=\d)\.(?=\d{3}(?:\D|$))", "", num_str)
        return float(cleaned)

    @classmethod
    def _dedup_ingredients(cls, names: List[str]) -> List[str]:
        """
        - Remove duplicatas exatas preservando ordem.
        - Remove sinônimos onde um nome é substring de outro; mantém o mais longo.
        - Retorna apenas a primeira ocorrência de cada nome mantido.
        """
        uniq_in_order: List[str] = []
        for n in names:
            if n not in uniq_in_order:
                uniq_in_order.append(n)

        ordered = sorted(set(uniq_in_order), key=len, reverse=True)
        kept_longer_first: List[str] = []
        for name in ordered:
            if not any(name in longer for longer in kept_longer_first):
                kept_longer_first.append(name)

        final: List[str] = []
        for n in uniq_in_order:
            if n in kept_longer_first and n not in final:
                final.append(n)
        return final

    @classmethod
    def _extract_pairs(cls, head: str) -> List[Tuple[float, str]]:
        """
        1) Tenta UNIDADE COMPARTILHADA (ex.: "450 + 50 MG", "(20+5) MG/ML").
        2) Se não houver, tenta PARES EXPLÍCITOS (ex.: "1 MG + 100.000 UI/G").
        """
        # 1) unidade compartilhada (agora exige pelo menos um '+')
        m = cls._SHARED_UNIT_PATTERN.search(head)
        if m:
            nums = [x.strip() for x in m.group("numlist").split("+") if x.strip()]
            unit = m.group("unit")
            return [(cls._to_float(x), unit) for x in nums]

        # 2) pares explícitos
        pairs = cls._PAIR_PATTERN.findall(head)
        if pairs:
            return [(cls._to_float(num), unit) for (num, unit) in pairs]

        return []

    @classmethod
    def parse(
        cls,
        presentation: str,
        active_ingredients: List[str],
        id_apresentacao_medicamento: str,
    ) -> List[ItemComposicao]:
        pres = cls._normalize(presentation)
        head = cls._strip_packaging_tail(pres)
        head = cls._strip_after_form_marker(head)

        pairs = cls._extract_pairs(head)
        if not pairs:
            return []

        actives = cls._dedup_ingredients(active_ingredients)

        items: List[ItemComposicao] = []
        for i, pa in enumerate(actives):
            if i < len(pairs):
                q, u = pairs[i]
            else:
                q, u = None, pairs[-1][1]
            items.append(ItemComposicao(
                principio_ativo=pa,
                dosagem=q,
                unidade_de_medida=u,
                id_apresentacao_medicamento=id_apresentacao_medicamento,
            ))
        return items


def extract_composition_from_presentation_string(
    presentation: str,
    active_ingredients: List[str],
    id_apresentacao_medicamento: str,
) -> List[ItemComposicao]:
    return CompositionParser.parse(presentation, active_ingredients, id_apresentacao_medicamento)
