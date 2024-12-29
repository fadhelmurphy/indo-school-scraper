from google.cloud import bigquery
from typing import List, Set
from models import Province, Kabupaten, Kecamatan, School


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset_id = "school_data"

    def _ensure_tables_exist(self):
        dataset_ref = self.client.dataset(self.dataset_id)

        schemas = {
            "provinces": [
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("name", "STRING"),
            ],
            "kabupatens": [
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("province_code", "STRING"),
            ],
            "kecamatans": [
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("kabupaten_code", "STRING"),
            ],
            "schools": [
                bigquery.SchemaField("npsn", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("kecamatan_code", "STRING"),
            ],
        }

        for table_name, schema in schemas.items():
            table_ref = dataset_ref.table(table_name)
            try:
                self.client.get_table(table_ref)
            except Exception:
                table = bigquery.Table(table_ref, schema=schema)
                self.client.create_table(table)

    def save_provinces(self, provinces: List[Province]):
        self._ensure_tables_exist()
        table_id = f"{self.dataset_id}.provinces"

        rows = [{"code": p.code, "name": p.name} for p in provinces]

        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            print("Errors inserting provinces:", errors)

    def save_kabupatens(self, kabupatens: List[Kabupaten]):
        table_id = f"{self.dataset_id}.kabupatens"

        rows = [
            {"code": k.code, "name": k.name, "province_code": k.province_code}
            for k in kabupatens
        ]

        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            print("Errors inserting kabupatens:", errors)

    def save_kecamatans(self, kecamatans: List[Kecamatan]):
        table_id = f"{self.dataset_id}.kecamatans"

        rows = [
            {"code": k.code, "name": k.name, "kabupaten_code": k.kabupaten_code}
            for k in kecamatans
        ]

        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            print("Errors inserting kecamatans:", errors)

    def save_schools(self, schools: List[School]):
        table_id = f"{self.dataset_id}.schools"

        rows = [
            {"npsn": s.npsn, "name": s.name, "kecamatan_code": s.kecamatan_code}
            for s in schools
        ]

        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            print("Errors inserting schools:", errors)

    def get_processed_provinces(self) -> Set[str]:
        query = """
        SELECT DISTINCT code 
        FROM `{}.provinces`
        """.format(
            self.dataset_id
        )

        return {row.code for row in self.client.query(query)}

    def get_processed_kabupatens(self) -> Set[str]:
        query = """
        SELECT DISTINCT code 
        FROM `{}.kabupatens`
        """.format(
            self.dataset_id
        )

        return {row.code for row in self.client.query(query)}

    def get_processed_kecamatans(self) -> Set[str]:
        query = """
        SELECT DISTINCT code 
        FROM `{}.kecamatans`
        """.format(
            self.dataset_id
        )

        return {row.code for row in self.client.query(query)}

    def get_processed_schools(self) -> Set[str]:
        query = """
        SELECT DISTINCT npsn 
        FROM `{}.schools`
        """.format(
            self.dataset_id
        )

        return {row.npsn for row in self.client.query(query)}
