from utils import (
    build_data_storage_path,
    save_parquet_into_data_storage,
    get_open_alex_data_request,
    get_num_paginas_openalex
)
import polars as pl
import json
from typing import List, Dict, Any

class WorksApiToBronze:
    def __init__(self, execution_date):
        self.execution_date = execution_date
        self.execution_date_str = execution_date.strftime('%d%m%Y')

        self.source_name = "open-alex"
        self.entity = "works"
        
        # Path save file
        self.bronze_write_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date=self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity
        )
        
        # Path read authors parquet file
        self.silver_authors_path = build_data_storage_path(
            medallion_layer="02-silver",
            date="22052026",
            source_name=self.source_name,
            entity="authors",
        )

        self.params = {
            "per_page": 100,
            "filter": None,
            "select": ",".join(["id", "doi", "display_name", "title", "publication_year",
                        "publication_date", "type", "language", "abstract_inverted_index", "cited_by_count",
                        "open_access", "primary_location", "best_oa_location", "authorships", "ids", "biblio",
                        "created_date", "updated_date", "keywords", "topics", "referenced_works_count"])
        }

    def get_authors_parquet_file(self):
        parquet_file_names = list(self.silver_authors_path.glob("authors_*.parquet"))
        return parquet_file_names
    
    def list_openalex_ids_from_authors_dataframe(self, parquet_file) -> List[str]:
        parquet_files = self.get_authors_parquet_file()
        parquet_file = parquet_files[0]

        df = pl.read_parquet(parquet_file)
        openalex_id_list = df['openalex_id'].to_list()
        
        return openalex_id_list
    
    def get_data_from_api(self, params: Dict[str, Any]) -> Dict[str, Any] | None:
        data = get_open_alex_data_request("/works", params)
        return data
    
    def get_paginated_data_from_api_works(self, num_pages: int, params: Dict[str, Any]):
        data_api = []
        
        for page in range(1, num_pages + 1):
            params['page'] = page
            response_data = self.get_data_from_api(params)
            for tentativa in range(2, 7):
                if 'faultstring' in response_data:
                    response_data = self.get_data_from_api(params)
                    print(f"Requisição n° {tentativa}")
                else:
                    break

            response_data = response_data['results']
            data_api.extend(response_data)
            
        data_api_dict = {"results" : data_api}
        return data_api_dict

    def execute(self):
        authors = self.get_authors_parquet_file()
        if len(authors) > 0:
            openalex_id_list = self.list_openalex_ids_from_authors_dataframe(authors[0])
        else:
            print("No authors parquet file found in the silver path of Open Alex.")
            return None

        print("ORCID LISTA: ", openalex_id_list)
        
        works_params = self.params.copy()
        works_params['filter'] = f"author.id:{'|'.join(openalex_id_list)},type:book|book-chapter|article"
        num_pages = get_num_paginas_openalex("/works", works_params)
        
        data_api = self.get_paginated_data_from_api_works(num_pages, works_params)
        
        if data_api is not None:
            for record in data_api["results"]:
                if isinstance(record.get("abstract_inverted_index"), dict):
                    record["abstract_inverted_index"] = json.dumps(record["abstract_inverted_index"])
                else:
                    record["abstract_inverted_index"] = None
            df = pl.DataFrame(data_api["results"])
            print(self.bronze_write_path)
            save_parquet_into_data_storage(df, self.bronze_write_path, self.execution_date_str, self.entity)
            print('Data fetched and saved successfully.')
            return 0
        else:
            print("No data fetched from OpenAlex API - Works.")
            return None