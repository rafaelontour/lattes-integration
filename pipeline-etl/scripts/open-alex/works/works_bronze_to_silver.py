from utils import (
    build_data_storage_path,
    save_parquet_into_data_storage,
    select_and_rename_polars_columns,
    convert_string_columns_to_date,
    create_timestamp_column,
    join_abstract_indexes_from_polars_column,
    generate_producao_autor_df,
    extract_journal_info
)
import polars as pl
from datetime import datetime
from typing import List, Dict, Any

class WorksBronzeToSilver:
    def __init__(self, execution_date):
        self.execution_date = execution_date
        self.execution_date_str = execution_date.strftime('%d%m%Y')

        self.source_name = "open-alex"
        self.entity = "works"
        
        self.bronze_read_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date="24052026",
            source_name=self.source_name,
            entity=self.entity
        )
        self.silver_producao_write_path = build_data_storage_path(
            medallion_layer="02-silver",
            date = self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity,
        )
        
        self.silver_autores_producao_write_path = build_data_storage_path(
            medallion_layer="02-silver",
            date = self.execution_date_str,
            source_name=self.source_name,
            entity="autores_producao",
        )
        
        self.rename_columns_mapping = {
            "id": "openalex_id",
            "doi": "doi",
            "title": "titulo",
            "publication_year": "ano_publicacao",
            "publication_date": "data_publicacao",
            "type": "tipo_producao",
            "language": "idioma",
            "abstract": "abstract",
            "cited_by_count": "qtd_citacoes_producao",
            "referenced_works_count": "qtd_referencias_producao",
            # "authorships": "autores",
            # "primary_location": "local_publicacao",
            "journal_name": "journal_name",
            "issn_l": "issn",
            # "keywords": "keywords",
            "palavras_chave": "palavras_chave",
            "created_date": "created_at",
            # "updated_date": "updated_date"
        }

    def deduplicate_works(self, df: pl.DataFrame):
        df = (df
            .sort("doi", nulls_last=True)
            .unique(subset=["display_name"], keep="first")
        )
        return df
        
    def get_keywords_list(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.col("keywords")
            .list.eval(pl.element().struct.field("display_name"))
            .alias("palavras_chave")
        )
        return df

    def explode_and_select_works_authorships(self, df: pl.DataFrame) -> pl.DataFrame:
        if "authorships" not in df.columns:
            raise ValueError("DataFrame has no 'authorships' column.")

        exploded = (
            df.explode("authorships")
            .with_columns([
                pl.col("authorships").struct.field("author_position").alias("author_position"),
                pl.col("authorships").struct.field("is_corresponding").alias("is_corresponding"),
                pl.col("authorships").struct.field("raw_author_name").alias("nome_autor"),
                pl.col("authorships").struct.field("raw_affiliation_strings").alias("raw_affiliation_strings"),
                pl.col("authorships").struct.field("countries").alias("paises"),
                pl.col("authorships").struct.field("institutions").alias("instituicoes"),
                pl.col("authorships").struct.field("author").struct.field("id").alias("openalex_autor_id"),
                pl.col("authorships").struct.field("author").struct.field("display_name").alias("display_name_author"),
                pl.col("authorships").struct.field("author").struct.field("orcid").alias("orcid_id"),
            ])
        )
        return exploded
    
    def deduplicate_works(self, df):
        df = (df
            .sort("doi", nulls_last=True)
            .unique(subset=["display_name"], keep="first")
        )
        return df
        
    def execute(self):
        parquet_file_name = list(self.bronze_read_path.glob("works*.parquet"))
        if len(parquet_file_name) == 0:
            print("No Works parquet file found in the bronze path.")
            return None

        df_bronze = pl.read_parquet(parquet_file_name[0])
        print(f'Extracted {df_bronze.height} lines from bronze.')

        if df_bronze.height > 0:
            df = self.deduplicate_works(df_bronze)
            print(f'{df.height} lines after dedup.')
            
            ## ----- Criando e Salvando DataFrame de Producao <-> Autores ------
            df_authorships = self.explode_and_select_works_authorships(df)
            df_producao_autor = generate_producao_autor_df(df_authorships)
            save_parquet_into_data_storage(df_producao_autor, self.silver_autores_producao_write_path, self.execution_date_str, "autores_producao")
            ## ----------------------------------------------------------------------
            
            df = self.get_keywords_list(df)
            df = extract_journal_info(df)
            
            df = join_abstract_indexes_from_polars_column(df)
            df = convert_string_columns_to_date(df, ['publication_date', 'created_date'])
            df = select_and_rename_polars_columns(df, self.rename_columns_mapping, rename=True)
            df = create_timestamp_column(df)
            
            
            save_parquet_into_data_storage(df, self.silver_producao_write_path, self.execution_date_str, self.entity)
            return 0
        else:
            print("The Works parquet file in the bronze path is empty.")
            return None