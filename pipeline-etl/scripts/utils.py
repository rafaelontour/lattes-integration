import requests
import polars as pl
import os
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import pytz
from dotenv import load_dotenv

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

def find_project_root(root_dir_name: str = "lattes-integration", start_path: Path | None = None) -> Path:
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
    path = project_root / pipeline_dir / data_storage_dir / medallion_layer / source_name
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
        print(f'Parquet file saved to: {output_file}')
    except Exception as e:
        print(f'Error while saving Parquet file: {e}')
        
def select_and_rename_polars_columns(df: pl.DataFrame, dict_column_names: dict[str, str]) -> pl.DataFrame:
    """ Select and rename columns in a Polars DataFrame based on a provided mapping.

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

    list_column_names = [pl.col(k).alias(v) for k, v in dict_column_names.items()]
    df = df.select(list_column_names)

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

    timezone_str = 'America/Sao_Paulo'
    timezone = pytz.timezone(timezone_str)
    utc_dt = datetime.now()
    br_dt = utc_dt.astimezone(timezone)
    br_dt = br_dt.replace(tzinfo=None)
    return br_dt