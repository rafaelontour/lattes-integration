from utils import (
    build_data_storage_path,
    save_parquet_into_data_storage,
    select_and_rename_polars_columns,
    get_sao_paulo_datetime
)
import polars as pl

class AuthorsBronzeToSilver:
    def __init__(self, execution_date):
        self.execution_date = execution_date
        self.execution_date_str = execution_date.strftime('%d%m%Y')

        self.source_name = "open-alex"
        self.entity = "authors"
        
        self.bronze_read_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date=self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity,
        )
        self.silver_path = build_data_storage_path(
            medallion_layer="02-silver",
            date=self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity,
        )

        self.rename_columns_mapping = {
            "orcid": "orcid_id",
            "h_index": "h_index",
            "i10_index": "i10_index",
            "works_count": "works_count",
            "cited_by_count": "cited_by_count",
            "works_api_url": "works_api_url",
            "updated_date": "updated_at"
        }

    def unnest_summary_stats_to_get_indexs(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.unnest("summary_stats")
        return df
    
    def transform_updated_at_column(self, df: pl.DataFrame) -> pl.DataFrame:
        sp_date = get_sao_paulo_datetime()
        df = df.with_columns(pl.lit(sp_date).alias("updated_at"))
        return df

    def execute(self):
        parquet_file_name = list(self.bronze_read_path.glob("authors*.parquet"))
        if len(parquet_file_name) == 0:
            print("No authors parquet file found in the bronze path.")
            return None

        df_bronze = pl.read_parquet(parquet_file_name[0])

        if df_bronze.height > 0:
            df = self.unnest_summary_stats_to_get_indexs(df_bronze)
            df = select_and_rename_polars_columns(df, self.rename_columns_mapping)
            df = self.transform_updated_at_column(df)

            save_parquet_into_data_storage(df, self.silver_path, self.execution_date_str, self.entity)
            return 0
        else:
            print("The authors parquet file in the bronze path is empty.")
            return None