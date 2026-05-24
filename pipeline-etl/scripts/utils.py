import requests
import polars as pl
import os
import pyarrow.parquet as pq
import pytz
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict
from deep_translator import GoogleTranslator
import math
from typing import List, Dict, Any

BASE_URL = "https://api.openalex.org"

load_dotenv()


def get_open_alex_data_request(endpoint: str, params: dict) -> dict:
    """
    Get data from the OpenAlex API for a given endpoint and optional parameters.

    Parameters
    ----------
    endpoint : str
        The endpoint of the OpenAlex API to retrieve data from

    params : dict, optional
        A dictionary of query parameters to include in the API request

    Returns
    -------
    dict
        The JSON response from the OpenAlex API containing the requested data
    """

    full_url = BASE_URL + endpoint
    api_token = os.getenv("OPENALEX_API_KEY")

    params["api_key"] = api_token

    headers = {
        "accept": "application/json",
    }
    try:
        response = requests.get(full_url, params=params, headers=headers)
    except requests.RequestException as e:
        print(f"Error fetching OpenAlex data: {e}")
        raise
    response.raise_for_status()
    data = response.json()

    return data


def find_project_root(
    root_dir_name: str = "lattes-integration", start_path: Path | None = None
) -> Path:
    """Find the repository root path starting from the current working directory.

    The function walks up from the current working directory until it finds
    either a directory with the given root name or a directory containing
    the pipeline-etl folder.

    Parameters
    ----------
    root_dir_name : str, optional
        The name of the root directory to look for (default is "lattes-integration")

    start_path : Path, optional
        The path to start searching from (default is the current working directory)

    Returns
    -------
    Path
        The path to the project root directory

    """
    start_path = Path.cwd() if start_path is None else Path(start_path)
    for path in [start_path, *start_path.parents]:
        if path.name == root_dir_name or (path / "pipeline-etl").exists():
            return path
    raise FileNotFoundError(
        f"Could not locate project root '{root_dir_name}' from {start_path}."
    )


def build_data_storage_path(
    medallion_layer: str,
    date: str,
    source_name: str,
    entity: str,
    file_name: str | None = None,
    project_root: Path | None = None,
    pipeline_dir: str = "pipeline-etl",
    data_storage_dir: str = "data-storage",
    extension: str = ".parquet",
) -> Path:
    """Build a generic data-storage path for the ETL pipeline.

    Returns either a directory path or a full file path if `file_name` is provided.
    """
    project_root = find_project_root() if project_root is None else project_root
    path = (
        project_root / pipeline_dir / data_storage_dir / medallion_layer / source_name
    )
    if date:
        path = path / date
    path = path / entity
    if file_name:
        return path / f"{file_name}_{date}{extension}"
    return path


def save_parquet_into_data_storage(
    df: pl.DataFrame,
    path: Path,
    date: str,
    file_name: str,
    extension: str = ".parquet",
):
    """Save DataFrame to Parquet file with a specific naming convention.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to save

    path : Path
        The directory where the Parquet file will be saved

    date : str
        The date to include in the file name

    file_name : str
        The name of the file (without extension)

    extension : str, optional
        The file extension to use (default is ".parquet")

    Returns
    -------
    None
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
        output_file = path / f"{file_name}_{date}{extension}"
        df.write_parquet(output_file)
        print(f"{file_name.upper()} Parquet file saved to: {output_file}")
    except Exception as e:
        print(f"Error while saving Parquet file: {e}")


def select_and_rename_polars_columns(
    df: pl.DataFrame, dict_column_names: dict[str, str], rename: bool = True
) -> pl.DataFrame:
    """Select and rename columns in a Polars DataFrame based on a provided mapping.

    Parameters
    ----------
    df : polar.DataFrame
        DataFrame to be processed

    dict_column_names : dict[str, str]
        Dictionary mapping original column names (keys) to new column names (values)
    Returns
    -------
    polars.DataFrame
        DataFrame with selected and renamed columns
    """

    cols = [c for c in dict_column_names.keys() if c in df.columns]
    df = df.select([pl.col(c) for c in cols])
    if rename:
        df = df.rename(dict_column_names)
    return df


def get_sao_paulo_datetime():
    """Função para obter datetime
    em tempo de execução relativo ao
    timezone de São Paulo.

    Parameters
    ----------
    Returns
    -------
    datetime
        Objeto com data e hora convertida para horário de São Paulo.
    """

    timezone_str = "America/Sao_Paulo"
    timezone = pytz.timezone(timezone_str)
    utc_dt = datetime.now()
    br_dt = utc_dt.astimezone(timezone)
    br_dt = br_dt.replace(tzinfo=None)
    return br_dt


import json

def join_abstract_indexes(abstract_inverted_index: str | dict | None) -> str:
    """Join abstract words based on their positions from OpenAlex inverted index."""
    
    if isinstance(abstract_inverted_index, str):
        try:
            abstract_inverted_index = json.loads(abstract_inverted_index)
        except (json.JSONDecodeError, TypeError):
            return ""

    if not isinstance(abstract_inverted_index, dict) or not abstract_inverted_index:
        return ""

    max_position = max(
        (
            position
            for positions in abstract_inverted_index.values()
            for position in positions
            if isinstance(position, int) and position >= 0
        ),
        default=-1,
    )

    if max_position < 0:
        return ""

    words = [""] * (max_position + 1)

    for word, positions in abstract_inverted_index.items():
        for pos in positions:
            if isinstance(pos, int) and 0 <= pos <= max_position:
                words[pos] = word

    return " ".join(word for word in words if word)


def translate_abstract(abstract: str) -> str:
    """Translate abstract from English to Portuguese."""
    abstracted_translated = GoogleTranslator(source="en", target="pt").translate(
        abstract
    )
    return abstracted_translated


def join_abstract_indexes_from_polars_column(
    df: pl.DataFrame, abstract_column: str = "abstract_inverted_index"
) -> pl.DataFrame:
    """Create a plain-text abstract column from a Polars inverted index column."""
    if abstract_column not in df.columns:
        raise ValueError(f"DataFrame has no '{abstract_column}' column.")

    return df.with_columns(
        pl.col(abstract_column)
        .map_elements(join_abstract_indexes, return_dtype=pl.String)
        .alias("abstract")
    )


def transform_abstract_and_translate_if_needed(
    df: pl.DataFrame, abstract_column: str = "abstract_inverted_index"
) -> pl.DataFrame:
    """Transform abstract from inverted index to plain text and translate if needed."""
    df = join_abstract_indexes_from_polars_column(df, abstract_column)
    df = df.with_columns(
        pl.when(pl.col("language") == "en")
        .then(
            pl.col("abstract").map_elements(translate_abstract, return_dtype=pl.String)
        )
        .otherwise(pl.col("abstract"))
        .alias("resumo_traduzido")
    )
    return df


def get_num_paginas_openalex(endpoint: str, params: Dict[str, Any]) -> int:
    """Get the total number of pages for a given OpenAlex API
    request based on the total record count.

    Parameters
    ----------

    Returns
    -------
    int
        Returns the total number of pages for the OpenAlex API request.

    """

    resultado_request = get_open_alex_data_request(endpoint, params)

    # The OpenAlex response contains a 'meta' dict with 'count'.
    total_records = 0
    if isinstance(resultado_request, dict):
        total_records = resultado_request.get("meta", {}).get("count", 0)

    num_paginas = math.ceil(total_records / 100) if total_records > 0 else 0

    if total_records == 0:
        print(resultado_request)

    return num_paginas


def extract_journal_info(df: pl.DataFrame) -> pl.DataFrame:
    """Extract journal/source metadata from primary_location."""
    return df.with_columns(
        [
            pl.col("primary_location")
            .struct.field("source")
            .struct.field("display_name")
            .alias("journal_name"),
            pl.col("primary_location")
            .struct.field("source")
            .struct.field("issn_l")
            .alias("issn_l"),
            pl.col("primary_location").struct.field("license").alias("license"),
        ]
    )


def generate_producao_autor_df(df_pl: pl.DataFrame) -> pl.DataFrame:
    POSITION_TO_ORDER = {"first": 1, "middle": 2, "last": 3}
    df = df_pl.with_columns(
        [
            pl.col("id").alias("openalex_producao_id"),
            pl.col("openalex_autor_id"),
            pl.col("orcid_id"),
            pl.col("nome_autor"),
            pl.col("author_position")
            .replace_strict(POSITION_TO_ORDER, default=None)
            .alias("ordem_autoria"),
            (pl.col("author_position") == "first").alias("autor_principal"),
            pl.col("is_corresponding").alias("correspondente"),
            pl.col("raw_affiliation_strings").list.join("; ").alias("afiliacao_autor"),
        ]
    ).select(
        [
            "openalex_producao_id",
            "openalex_autor_id",
            "orcid_id",
            "nome_autor",
            "ordem_autoria",
            "autor_principal",
            "correspondente",
            "afiliacao_autor",
        ]
    )
    return df


def convert_string_columns_to_date(
    df: pl.DataFrame,
    date_columns: list,
    strict: bool = True,
) -> pl.DataFrame:
    v_date_columns = [
        col for col in date_columns
        if col in df.columns and df[col].dtype == pl.String
    ]

    if not v_date_columns:
        return df

    exprs = []
    for col in v_date_columns:
        sample = df[col].drop_nulls().first()
        if sample and "T" in sample:
            expr = pl.col(col).str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False).cast(pl.Date)
        elif sample and "-" in sample:
            expr = pl.col(col).str.to_date("%Y-%m-%d", strict=False)
        else:
            expr = pl.col(col).str.to_date("%d/%m/%Y", strict=strict)

        exprs.append(expr.dt.strftime("%d/%m/%Y").alias(col))

    return df.with_columns(exprs)


def create_timestamp_column(df_pl: pl.DataFrame, column_name: str = 'updated_at') -> pl.DataFrame:
    sp_timestamp = get_sao_paulo_datetime()
    if column_name not in set(df_pl.columns):
        df_pl = df_pl.with_columns((pl.lit(sp_timestamp).alias(column_name)))

    return df_pl
