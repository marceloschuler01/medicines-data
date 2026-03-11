import pandas as pd
import uuid
from dataclasses import asdict, dataclass

from medication_etl_src.database.api_database import ApiDatabase as sql
from medication_etl_src.database.db_connector import with_database_connection
from medication_etl_src.staging_db.staging_db import StagingDB
from medication_etl_src.api.adapter.anvisa.anvisa_apresentations_adapter import AnvisaApresentationsAdapter
from medication_etl_src.utils import extract_composition_from_presentation_string
from medication_etl_src.utils.extract_composition_from_presentation_string import ItemComposicao


@dataclass
class Result:
    medicines: pd.DataFrame
    regulatory_categories: pd.DataFrame
    enterprises: pd.DataFrame


class ExtractTransformAndLoadApresentacoes:

    def __init__(self, staging_db=StagingDB()):
        self.staging_db = staging_db

    @with_database_connection
    def main(self, conn=None) -> None:

        self._delete_all_old_presentations_data(conn=conn)
        self._etl_presentations_with_pagination(from_active_medines=True, conn=conn)
        # TODO Enable from inactive medicines
        self._etl_presentations_with_pagination(from_active_medines=False, conn=conn)

    @with_database_connection
    def _delete_all_old_presentations_data(self, conn=None) -> None:

        print("Deleting all old presentations data...")
        sql.delete(table_name="embalagem_apresentacao_medicamento", conn=conn)
        sql.delete(table_name="forma_farmaceutica_apresentacao_medicamento", conn=conn)
        sql.delete(table_name="forma_farmaceutica", conn=conn)
        sql.delete(table_name="classe_terapeutica_medicamento", conn=conn)
        sql.delete(table_name="classe_terapeutica", conn=conn)
        sql.delete(table_name="apresentacao_medicamento", conn=conn)
        print("Delete completed.")

    @with_database_connection
    def _etl_presentations_with_pagination(self, from_active_medines: bool, conn=None) -> None:

        page = 1

        while True:

            apresentacoes = self._read_from_staging_db(active_medicines=from_active_medines, page=page)
            if not apresentacoes:
                break

            print(f"Processing presentations from {'in' if not from_active_medines else ''}active medicines. Page:", page, "with", len(apresentacoes), "presentations")
            self._extract_transform_and_load(active_medicines=from_active_medines, apresentacoes=apresentacoes, conn=conn)

            page += 1

    @with_database_connection
    def _extract_transform_and_load(self, active_medicines: bool, apresentacoes: list[dict], conn=None):

        parsed_data = AnvisaApresentationsAdapter().adapt(apresentacoes=apresentacoes)

        df_produtos = pd.DataFrame([asdict(item) for item in parsed_data.produtos])
        del parsed_data.produtos
        df_produtos = self._join_products_with_medicine_id(active_medicines=active_medicines, products=df_produtos, conn=conn)
        self._update_id_medicamento_referencia_in_medicines(active_medicines=active_medicines, products=df_produtos, conn=conn)
        self._extract_therapeutic_classes_and_load(products=df_produtos, conn=conn)
        del df_produtos

        df_presentations = pd.DataFrame([asdict(item) for item in parsed_data.apresentacoes])
        del parsed_data.apresentacoes
        if "apresentacao" not in df_presentations.columns:
            df_presentations["apresentacao"] = None

        df_presentations = df_presentations.dropna(subset=["apresentacao"])

        if not df_presentations.empty:

            df_presentations["id_apresentacao_medicamento"] = [self._generate_uuid() for _ in range(len(df_presentations))]

            df_presentations["codigo_anvisa_medicamento"] = df_presentations["codigo_anvisa_medicamento"].astype(str)

            medicines_codigo_anvisa_to_id_mapper = self._get_medicines_from_presentations_id_mapper(active_medicines=active_medicines, presentations=df_presentations, conn=conn)

            df_presentations["id_medicamento"] = df_presentations["codigo_anvisa_medicamento"].map(medicines_codigo_anvisa_to_id_mapper)

            # TODO: Investigate why some presentations are coming without a valid medicine id, and fix the root cause instead of dropping these presentations
            df_presentations = df_presentations.dropna(subset=["id_medicamento"])

            # TODO, DO NOT DROP
            df_presentations.drop(columns=["fabricantes_nacionais", "fabricantesInternacionais"], inplace=True)

            LoadPresentationsToDB().main(df_presentations=df_presentations, conn=conn)
            self._extract_pharmaceutic_forms_and_load(presentations=df_presentations, conn=conn)
            self._extract_packaging_and_load(presentations=df_presentations, conn=conn)

            df_presentations = self._extract_other_entities_data_from_presentations(presentations=df_presentations, conn=conn)

    def _read_from_staging_db(self, active_medicines: bool, page: int) -> list[dict]:

        if active_medicines:
            presentations = self.staging_db.select("presentations_from_active_medicines", page=page, page_size=5000)
        else:
            presentations = self.staging_db.select("presentations_from_inactive_medicines", page=page, page_size=5000)

        return presentations

    @with_database_connection
    def _extract_other_entities_data_from_presentations(self, presentations: pd.DataFrame, conn=None):

        df_active_ingredients = self._add_missing_active_ingredients_and_return_all_active_ingredients(presentations=presentations, conn=conn)
        self._extract_compositions_and_load_to_database(presentations=presentations, active_medicines=df_active_ingredients, conn=conn)

        return presentations

    @with_database_connection
    def _extract_therapeutic_classes_and_load(self, products: pd.DataFrame, conn=None) -> None:

        therapeutic_classes_in_batch = self._extract_therapeutic_classes_from_products(products=products)

        if therapeutic_classes_in_batch.empty:
            return

        therapeutic_classes_database = sql.select_with_pandas(
            "classe_terapeutica",
            columns=["id_classe_terapeutica", "classe_terapeutica"],
            conn=conn,
        )

        missing_therapeutic_classes = therapeutic_classes_in_batch[
            ~therapeutic_classes_in_batch["classe_terapeutica"].isin(therapeutic_classes_database["classe_terapeutica"])
        ].copy()
        missing_therapeutic_classes["id_classe_terapeutica"] = [self._generate_uuid() for _ in range(len(missing_therapeutic_classes))]

        if not missing_therapeutic_classes.empty:
            sql.insert_with_copy(
                table_name="classe_terapeutica",
                data=missing_therapeutic_classes.to_dict(orient="records"),
                conn=conn,
            )

        therapeutic_classes = pd.concat(
            [therapeutic_classes_database, missing_therapeutic_classes],
            ignore_index=True,
        )
        therapeutic_classes = therapeutic_classes[
            therapeutic_classes["classe_terapeutica"].isin(therapeutic_classes_in_batch["classe_terapeutica"])
        ]

        relationships = self._extract_therapeutic_class_relationships(
            products=products,
            therapeutic_classes=therapeutic_classes,
        )

        if not relationships.empty:
            sql.insert_with_copy(
                table_name="classe_terapeutica_medicamento",
                data=relationships.to_dict(orient="records"),
                conn=conn,
            )

    @with_database_connection
    def _extract_pharmaceutic_forms_and_load(self, presentations: pd.DataFrame, conn=None) -> None:

        pharmaceutic_forms_in_batch = self._extract_pharmaceutic_forms_from_presentations(presentations=presentations)

        if pharmaceutic_forms_in_batch.empty:
            return

        pharmaceutic_forms_database = sql.select_with_pandas(
            "forma_farmaceutica",
            columns=["id_forma_farmaceutica", "forma_farmaceutica"],
            conn=conn,
        )

        missing_pharmaceutic_forms = pharmaceutic_forms_in_batch[
            ~pharmaceutic_forms_in_batch["forma_farmaceutica"].isin(pharmaceutic_forms_database["forma_farmaceutica"])
        ].copy()
        missing_pharmaceutic_forms["id_forma_farmaceutica"] = [self._generate_uuid() for _ in range(len(missing_pharmaceutic_forms))]

        if not missing_pharmaceutic_forms.empty:
            sql.insert_with_copy(
                table_name="forma_farmaceutica",
                data=missing_pharmaceutic_forms.to_dict(orient="records"),
                conn=conn,
            )

        pharmaceutic_forms = pd.concat(
            [pharmaceutic_forms_database, missing_pharmaceutic_forms],
            ignore_index=True,
        )
        pharmaceutic_forms = pharmaceutic_forms[
            pharmaceutic_forms["forma_farmaceutica"].isin(pharmaceutic_forms_in_batch["forma_farmaceutica"])
        ]

        relationships = self._extract_pharmaceutic_form_relationships(
            presentations=presentations,
            pharmaceutic_forms=pharmaceutic_forms,
        )

        if not relationships.empty:
            sql.insert_with_copy(
                table_name="forma_farmaceutica_apresentacao_medicamento",
                data=relationships.to_dict(orient="records"),
                conn=conn,
            )

    @staticmethod
    def _extract_therapeutic_classes_from_products(products: pd.DataFrame) -> pd.DataFrame:

        if "classes_terapeuticas" not in products.columns:
            return pd.DataFrame(columns=["classe_terapeutica"])

        therapeutic_classes = products["classes_terapeuticas"].explode().dropna()

        if therapeutic_classes.empty:
            return pd.DataFrame(columns=["classe_terapeutica"])

        therapeutic_classes = therapeutic_classes.astype(str).str.strip()
        therapeutic_classes = therapeutic_classes[therapeutic_classes != ""]

        if therapeutic_classes.empty:
            return pd.DataFrame(columns=["classe_terapeutica"])

        return pd.DataFrame({"classe_terapeutica": therapeutic_classes.unique()})

    @staticmethod
    def _extract_therapeutic_class_relationships(products: pd.DataFrame, therapeutic_classes: pd.DataFrame) -> pd.DataFrame:

        if products.empty or therapeutic_classes.empty or "classes_terapeuticas" not in products.columns:
            return pd.DataFrame(columns=["id_classe_terapeutica", "id_medicamento"])

        relationships = products[["id_medicamento", "classes_terapeuticas"]].copy()
        relationships = relationships.dropna(subset=["id_medicamento"])
        relationships = relationships.explode("classes_terapeuticas").dropna(subset=["classes_terapeuticas"])

        if relationships.empty:
            return pd.DataFrame(columns=["id_classe_terapeutica", "id_medicamento"])

        relationships["classes_terapeuticas"] = relationships["classes_terapeuticas"].astype(str).str.strip()
        relationships = relationships[relationships["classes_terapeuticas"] != ""]

        if relationships.empty:
            return pd.DataFrame(columns=["id_classe_terapeutica", "id_medicamento"])

        classes_mapper = therapeutic_classes.set_index("classe_terapeutica")["id_classe_terapeutica"].to_dict()
        relationships["id_classe_terapeutica"] = relationships["classes_terapeuticas"].map(classes_mapper)
        relationships = relationships.dropna(subset=["id_classe_terapeutica"])
        relationships = relationships[["id_classe_terapeutica", "id_medicamento"]].drop_duplicates()

        return relationships

    @staticmethod
    def _extract_pharmaceutic_forms_from_presentations(presentations: pd.DataFrame) -> pd.DataFrame:

        if "formas_farmaceuticas" not in presentations.columns:
            return pd.DataFrame(columns=["forma_farmaceutica"])

        pharmaceutic_forms = presentations["formas_farmaceuticas"].explode().dropna()

        if pharmaceutic_forms.empty:
            return pd.DataFrame(columns=["forma_farmaceutica"])

        pharmaceutic_forms = pharmaceutic_forms.astype(str).str.strip()
        pharmaceutic_forms = pharmaceutic_forms[pharmaceutic_forms != ""]

        if pharmaceutic_forms.empty:
            return pd.DataFrame(columns=["forma_farmaceutica"])

        return pd.DataFrame({"forma_farmaceutica": pharmaceutic_forms.unique()})

    @staticmethod
    def _extract_pharmaceutic_form_relationships(presentations: pd.DataFrame, pharmaceutic_forms: pd.DataFrame) -> pd.DataFrame:

        if presentations.empty or pharmaceutic_forms.empty or "formas_farmaceuticas" not in presentations.columns:
            return pd.DataFrame(columns=["id_apresentacao_medicamento", "id_forma_farmaceutica"])

        relationships = presentations[["id_apresentacao_medicamento", "formas_farmaceuticas"]].copy()
        relationships = relationships.dropna(subset=["id_apresentacao_medicamento"])
        relationships = relationships.explode("formas_farmaceuticas").dropna(subset=["formas_farmaceuticas"])

        if relationships.empty:
            return pd.DataFrame(columns=["id_apresentacao_medicamento", "id_forma_farmaceutica"])

        relationships["formas_farmaceuticas"] = relationships["formas_farmaceuticas"].astype(str).str.strip()
        relationships = relationships[relationships["formas_farmaceuticas"] != ""]

        if relationships.empty:
            return pd.DataFrame(columns=["id_apresentacao_medicamento", "id_forma_farmaceutica"])

        forms_mapper = pharmaceutic_forms.set_index("forma_farmaceutica")["id_forma_farmaceutica"].to_dict()
        relationships["id_forma_farmaceutica"] = relationships["formas_farmaceuticas"].map(forms_mapper)
        relationships = relationships.dropna(subset=["id_forma_farmaceutica"])
        relationships = relationships[["id_apresentacao_medicamento", "id_forma_farmaceutica"]].drop_duplicates()

        return relationships

    @with_database_connection
    def _extract_packaging_and_load(self, presentations: pd.DataFrame, conn=None) -> None:

        packaging = self._extract_packaging_from_presentations(presentations=presentations)

        if packaging.empty:
            return

        sql.insert_with_copy(
            table_name="embalagem_apresentacao_medicamento",
            data=packaging.to_dict(orient="records"),
            conn=conn,
        )

    @staticmethod
    def _extract_packaging_from_presentations(presentations: pd.DataFrame) -> pd.DataFrame:

        packaging_records = []

        if "embalagens_primarias" not in presentations.columns and "embalagens_secundarias" not in presentations.columns:
            return pd.DataFrame(columns=["id_embalagem_medicamento", "primaria", "tipo", "observacao", "id_apresentacao_medicamento"])

        for _, row in presentations.iterrows():
            if pd.isna(row.get("id_apresentacao_medicamento")):
                continue

            pres_id = row["id_apresentacao_medicamento"]

            primary = row.get("embalagens_primarias")
            if isinstance(primary, list):
                for pkg in primary:
                    if isinstance(pkg, dict) and pkg.get("tipo"):
                        packaging_records.append({
                            "id_embalagem_medicamento": str(uuid.uuid4()),
                            "primaria": True,
                            "tipo": pkg.get("tipo", "").strip() if pkg.get("tipo") else "",
                            "observacao": pkg.get("observacao", "").strip() if pkg.get("observacao") else None,
                            "id_apresentacao_medicamento": pres_id,
                        })

            secondary = row.get("embalagens_secundarias")
            if isinstance(secondary, list):
                for pkg in secondary:
                    if isinstance(pkg, dict) and pkg.get("tipo"):
                        packaging_records.append({
                            "id_embalagem_medicamento": str(uuid.uuid4()),
                            "primaria": False,
                            "tipo": pkg.get("tipo", "").strip() if pkg.get("tipo") else "",
                            "observacao": pkg.get("observacao", "").strip() if pkg.get("observacao") else None,
                            "id_apresentacao_medicamento": pres_id,
                        })

        if not packaging_records:
            return pd.DataFrame(columns=["id_embalagem_medicamento", "primaria", "tipo", "observacao", "id_apresentacao_medicamento"])

        return pd.DataFrame(packaging_records)

    @with_database_connection
    def _add_missing_active_ingredients_and_return_all_active_ingredients(self, presentations: pd.DataFrame, conn=None) -> pd.DataFrame:

        active_ingredients = presentations["principios_ativos"].explode().dropna().unique().tolist()

        df_active_ingredients = pd.DataFrame(active_ingredients, columns=["nome"])

        active_ingredients_database = sql.select_with_pandas("principio_ativo", columns=["id_principio_ativo", "nome"], conn=conn)

        missing_active_ingredients = df_active_ingredients[~df_active_ingredients["nome"].isin(active_ingredients_database["nome"])]
        missing_active_ingredients["id_principio_ativo"] = [self._generate_uuid() for _ in range(len(missing_active_ingredients))]

        if not missing_active_ingredients.empty:
            sql.insert_with_copy(table_name="principio_ativo", data=missing_active_ingredients.to_dict(orient="records"), conn=conn)

        df_active_ingredients = pd.concat([active_ingredients_database, missing_active_ingredients], ignore_index=True)

        # Filter only the active ingredients present in the current batch of presentations
        df_active_ingredients = df_active_ingredients[df_active_ingredients["nome"].isin(active_ingredients)]

        return df_active_ingredients

    @with_database_connection
    def _extract_compositions_and_load_to_database(self, presentations: pd.DataFrame, active_medicines: pd.DataFrame, conn=None) -> None:

        compositions = self._extract_composition_from_presentations(presentations=presentations, active_medicines=active_medicines)

        #sql.delete(table_name="composicao_apresentacao_medicamento", filters=sql.filter("id_apresentacao_medicamento", presentations["id_apresentacao_medicamento"].tolist(), "IN"))

        sql.insert_with_copy(table_name="composicao_apresentacao_medicamento", data=compositions.to_dict(orient="records"), conn=conn)

    def _extract_composition_from_presentations(self, presentations: pd.DataFrame, active_medicines: pd.DataFrame) -> pd.DataFrame:

        presentations = presentations[["id_apresentacao_medicamento", "apresentacao", "principios_ativos"]].copy()

        compositions: list[ItemComposicao] = []

        for row in presentations.itertuples(index=False):
            extracted_compositions = extract_composition_from_presentation_string(row.apresentacao, row.principios_ativos, row.id_apresentacao_medicamento)
            compositions.extend(extracted_compositions)

        compositions: list[dict] = [asdict(item) for item in compositions]

        compositions: pd.DataFrame = pd.DataFrame(compositions)

        id_active_ingredient_mapper = active_medicines.set_index("nome")["id_principio_ativo"].to_dict()
        compositions["id_principio_ativo"] = compositions["principio_ativo"].map(id_active_ingredient_mapper)

        return compositions[["id_apresentacao_medicamento", "id_principio_ativo", "dosagem", "unidade_de_medida"]]

    @with_database_connection
    def _get_medicines_from_presentations_id_mapper(self, active_medicines: bool, presentations: pd.DataFrame, conn=None) -> dict[str, str]:

        codigos_medicamento = presentations["codigo_anvisa_medicamento"].dropna().unique().tolist()

        filters = [sql.filter("registro_ativo", active_medicines), sql.filter("codigo_anvisa", codigos_medicamento, "IN")]

        medicines = sql.select_with_pandas("medicamento", filters=filters, columns=["id_medicamento", "codigo_anvisa"], conn=conn)

        return medicines.set_index("codigo_anvisa")["id_medicamento"].to_dict()

    @with_database_connection
    def _update_id_medicamento_referencia_in_medicines(self, active_medicines: bool, products: pd.DataFrame, conn=None) -> None:

        products = products[["id_medicamento", "medicamento_referencia"]].copy()

        filters = sql.filter("registro_ativo", active_medicines)
        medicines = sql.select_with_pandas("medicamento", columns=["id_medicamento", "nome_comercial"], filters=filters, conn=conn)

        medicines = medicines.drop_duplicates(subset="nome_comercial")

        medicines = medicines.dropna(subset=["nome_comercial"])
        products = products.dropna(subset=["medicamento_referencia"])

        products["medicamento_referencia"] = products["medicamento_referencia"].str.strip().replace("", pd.NA)
        medicines["nome_comercial"] = medicines["nome_comercial"].str.strip().replace("", pd.NA)

        medicines = medicines.dropna(subset=["nome_comercial"])
        products = products.dropna(subset=["medicamento_referencia"])

        medicines_mapper = medicines.set_index("nome_comercial")["id_medicamento"].to_dict()

        products["id_medicamento_referencia"] = products["medicamento_referencia"].map(medicines_mapper)
        products = products.dropna(subset=["id_medicamento_referencia"])

        products = products[["id_medicamento", "id_medicamento_referencia"]]

        sql.update_in_bulk(table_name="medicamento", data=products, filter_column="id_medicamento", conn=conn)

    @with_database_connection
    def _join_products_with_medicine_id(self, active_medicines: bool, products: pd.DataFrame, conn=None) -> pd.DataFrame:

        filters = sql.filter("registro_ativo", active_medicines)
        medicines = sql.select_with_pandas("medicamento", columns=["id_medicamento", "codigo_anvisa"], filters=filters, conn=conn)

        medicines["codigo_anvisa"] = medicines["codigo_anvisa"].astype("int64")
        products["codigo_anvisa"] = products["codigo_anvisa"].astype("int64")

        medicines = medicines.drop_duplicates(subset="codigo_anvisa")
        medicines = medicines.dropna(subset=["codigo_anvisa"])

        mapper_codigo_anvisa_to_id = medicines.set_index("codigo_anvisa")["id_medicamento"].to_dict()
        products["id_medicamento"] = products["codigo_anvisa"].map(mapper_codigo_anvisa_to_id)

        return products

    @staticmethod
    def _generate_uuid() -> str:
        return str(uuid.uuid4())


class LoadPresentationsToDB:

    @with_database_connection
    def main(self, df_presentations: pd.DataFrame, conn=None):

        sql.insert_with_copy(table_name="apresentacao_medicamento", data=df_presentations.to_dict(orient="records"), conn=conn)


if __name__ == "__main__":
    ExtractTransformAndLoadApresentacoes().main()
