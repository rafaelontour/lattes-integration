from utils import (
    build_data_storage_path,
    save_parquet_into_data_storage,
    get_open_alex_data_request
    )
import polars as pl
from datetime import datetime
from typing import List, Dict, Any

class AuthorsApiToBronze:
    def __init__(self, execution_date):
        self.execution_date = execution_date
        self.execution_date_str = execution_date.strftime('%d%m%Y')

        self.source_name = "open-alex"
        self.entity = "authors"
        
        self.bronze_write_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date=self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity
        )
        self.bronze_pesquisadores_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date=self.execution_date_str,
            source_name="lattes",
            entity="pesquisadores",
        )

    def get_pesquisadores_lattes_parquet_file(self):
        parquet_file_names = list(self.bronze_pesquisadores_path.glob("pesquisadores*.parquet"))
        return parquet_file_names
    
    def list_pesquisadores_orcids(self, parquet_file=None) -> List[str]:
        if parquet_file is None:
            parquet_files = self.get_pesquisadores_lattes_parquet_file()
            if not parquet_files:
                return []
            parquet_file = parquet_files[0]

        df = pl.read_parquet(parquet_file)
        orcid_list = df['orcid_id'].to_list()
        
        return orcid_list
    
    def fetch_authors_by_orcid(self, params: Dict[str, Any]) -> Dict[str, Any] | None:
        data = get_open_alex_data_request("/authors", params)
        return data
        
    def execute(self):
        pesquisadores = self.get_pesquisadores_lattes_parquet_file()
        if len(pesquisadores) == 0:
            print("No pesquisadores parquet (lattes) file found in the bronze path.")
            return None
        orcid_list = self.list_pesquisadores_orcids(pesquisadores[0])
        
        params = {"per_page": 50,
                  "filter": f"orcid:{'|'.join(orcid_list)}",
                  "select": "orcid, display_name, full_name, works_count, cited_by_count, summary_stats, updated_date, works_api_url"}
        
        data = self.fetch_authors_by_orcid(params)
        
        if data is not None:
            df = pl.DataFrame(data["results"])
            print(self.bronze_write_path)
            save_parquet_into_data_storage(df, self.bronze_write_path, self.execution_date_str, "authors")
            print('Data fetched and saved successfully.')
            return 0
        else:
            print("No data fetched from OpenAlex API - Authors.")
            return None