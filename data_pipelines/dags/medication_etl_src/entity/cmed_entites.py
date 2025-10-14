from dataclasses import dataclass

@dataclass
class CmedPriceDefinition:
    numero_registro_anvisa: str
    ggrem: str
    ean_gtin: str
    ean_2: str
    regime_preco: str
    aliquotas: dict[str, float]
