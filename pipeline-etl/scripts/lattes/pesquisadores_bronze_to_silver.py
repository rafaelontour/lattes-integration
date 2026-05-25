import polars as pl
from utils import (build_data_storage_path, save_parquet_into_data_storage,
                   select_and_rename_polars_columns, create_timestamp_column)
from utils_pesquisadores import convert_string_columns_to_date


class PesquisadoresBronzeToSilver:
    def __init__(self, execution_date):
        self.execution_date = execution_date
        self.execution_date_str = execution_date.strftime('%d%m%Y')
        
        self.source_name = "lattes"
        self.entity_pesquisadores = "pesquisadores"
        self.entity_authors = "authors"
        
        self.read_bronze_pesquisadores_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date="17052026",
            source_name=self.source_name,
            entity=self.entity_pesquisadores,
        )
        self.read_silver_authors_path = build_data_storage_path(
            medallion_layer="02-silver",
            date="24052026",
            source_name="open-alex",
            entity=self.entity_authors,
        )
        self.write_silver_pesquisadores = build_data_storage_path(
            medallion_layer="02-silver",
            date=self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity_pesquisadores,
        )
    
        self.rename_columns = {
            'lattes_id': 'lattes_id',
            'orcid_id': 'orcid_id',
            'openalex_id': 'openalex_id',
            'nome_completo': 'nome_completo',
            'texto_resumo_cv': 'resumo_cv',
            'indice_h': 'indice_h',
            'indice_i10': 'indice_i10',
            'qtd_producoes': 'qtd_producoes',
            'qtd_citacoes_pesquisador': 'qtd_citacoes_pesquisador',
            'nacionalidade': 'nacionalidade',
            'instituicao_empresa': 'instituicao_empresa',
            'nome_orgao': 'nome_orgao',
            'codigo_instituicao': 'codigo_instituicao',
            'data_atualizacao': 'data_atualizacao_lattes',
        }
    
    def join_openalex_index_into_lattes(self, df_author, df_pesquisador) -> pl.DataFrame:
        df_author = df_author.select(['openalex_id', 'orcid_id', 'indice_h', 'indice_i10', 'qtd_producoes', 'qtd_citacoes_pesquisador'])
        df_enriched = df_pesquisador.join(df_author, on="orcid_id", how="left")
        return df_enriched
        
    def execute(self):
        parquet_author = list(self.read_silver_authors_path.glob("authors*.parquet"))
        parquet_pesquisador = list(self.read_bronze_pesquisadores_path.glob("pesquisadores*.parquet"))
        
        if len(parquet_author) == 0 or len(parquet_pesquisador) == 0:
            print("No Pesquisadores/Authors parquet file found in the Bronze/Silver path.")
            return None
        
        df_author = pl.read_parquet(parquet_author[0])
        df_pesquisador = pl.read_parquet(parquet_pesquisador[0])
        

        if df_author.height > 0 and df_pesquisador.height > 0:
            df = self.join_openalex_index_into_lattes(df_author, df_pesquisador)
            df = convert_string_columns_to_date(df, ["data_atualizacao"])
            df = select_and_rename_polars_columns(df, self.rename_columns)
            df = create_timestamp_column(df, "updated_at")
            save_parquet_into_data_storage(df, self.write_silver_pesquisadores, self.execution_date_str, "pesquisadores")
            return df
        else:
            print("The Works parquet file in the bronze path is empty.")
            return None
