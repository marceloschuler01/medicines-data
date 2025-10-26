import pandas as pd
from medication_etl_src.entity.cmed_entites import CmedPriceDefinition


class CMEDPricesAdapter:

    mapper = {
        "REGISTRO": "numero_registro_anvisa",
        "CÓDIGO GGREM": "ggrem",
        "EAN 1": "ean_gtin",
        "EAN 2": "ean_2",
        "REGIME DE PREÇO": "regime_preco",
        "aliquotas_dict": "aliquotas",
    }

    def adapt(self, data: list[dict]) -> list[CmedPriceDefinition]:

        df = pd.DataFrame(data)
        cols_pf_pmc = df.columns[df.columns.str.startswith(('PF', 'PMC', 'PMVG'))]

        df['aliquotas_dict'] = df[cols_pf_pmc].apply(
            lambda row: {k: v for k, v in row.items() if pd.notna(v)},
            axis=1
        )


        df = df.rename(columns=self.mapper)
        df = df[list(self.mapper.values())]

        for column in ['numero_registro_anvisa', 'ggrem', 'ean_gtin', 'ean_2', 'regime_preco']:
            df.loc[df[column].isnull(), column] = None
            df[column] = df[column].astype(str)
            df[column] = df[column].str.removesuffix(".0")
            df[column] = df[column].str.strip()
            df[column] = df[column].replace('', None)
            df[column] = df[column].replace('-', None)


        entities = [CmedPriceDefinition(**row) for row in df.to_dict(orient="records")]
        return entities
