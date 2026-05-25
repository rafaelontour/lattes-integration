import zipfile
import xmltodict
import polars as pl
from typing import Dict, Any, Optional, List
from datetime import datetime
from utils import build_data_storage_path, save_parquet_into_data_storage
from utils_pesquisadores import (
    extract_dados_gerais,
    extract_endereco_profissional,
    extract_formacao_academica,
    extract_areas_atuacao
)


class PesquisadoresLandingToBronze:
    def __init__(self, execution_date):
        self.execution_date = execution_date
        self.execution_date_str = execution_date.strftime('%d%m%Y')
        
        self.source_name = "lattes"
        self.entity_pesquisadores = "pesquisadores"
        self.entity_formacoes = "formacoes"
        self.entity_areas_atuacao = "areas_atuacao"
        
        self.bronze_pesquisadores_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date=self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity_pesquisadores,
        )
        self.bronze_formacoes_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date=self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity_formacoes,
        )
        self.bronze_areas_atuacao_path = build_data_storage_path(
            medallion_layer="01-bronze",
            date=self.execution_date_str,
            source_name=self.source_name,
            entity=self.entity_areas_atuacao,
        )
        
        self.landing_path = "lattesNAPI.zip"    
               
    def list_xml_files(self) -> List[str]:
        with zipfile.ZipFile(self.landing_path, 'r') as z:
            return [f.filename for f in z.infolist() if f.filename.endswith('.xml')]
    
    def parse_xml_from_zip(self, xml_filename: str) -> Dict[str, Any]:
        with zipfile.ZipFile(self.landing_path, 'r') as z:
            with z.open(xml_filename) as f:
                raw_bytes = f.read()
                return xmltodict.parse(raw_bytes)
    
    def save_all_xmls_to_parquet(self, pesquisadores: List[Dict], formacoes: List[Dict], 
                        areas: List[Dict]):
        
        if pesquisadores:
            df_pesquisadores = pl.DataFrame(pesquisadores)
            save_parquet_into_data_storage(
                df_pesquisadores,
                self.bronze_pesquisadores_path,
                self.execution_date_str,
                self.entity_pesquisadores,
            )
            print(f"\nSalvo: {self.bronze_pesquisadores_path} ({len(pesquisadores)} registros)")
        
        if formacoes:
            df_formacoes = pl.DataFrame(formacoes)
            save_parquet_into_data_storage(
                df_formacoes,
                self.bronze_formacoes_path,
                self.execution_date_str,
                self.entity_formacoes,
            )
            print(f"Salvo: {self.bronze_formacoes_path} ({len(formacoes)} registros)")
        
        if areas:
            df_areas = pl.DataFrame(areas)
            save_parquet_into_data_storage(
                df_areas,
                self.bronze_areas_atuacao_path,
                self.execution_date_str,
                self.entity_areas_atuacao,
            )
            print(f"Salvo: {self.bronze_areas_atuacao_path} ({len(areas)} registros)")
        
    def execute(self):
        xml_files = self.list_xml_files()
        print(f"Encontrados {len(xml_files)} arquivos XML no ZIP")
        
        pesquisadores_data = []
        formacoes_data = []
        areas_data = []
        
        for xml_file in xml_files:
            try:
                xml_dict = self.parse_xml_from_zip(xml_file)
                
                dados_gerais = extract_dados_gerais(xml_dict)
                endereco = extract_endereco_profissional(xml_dict)
                
                pesquisador = {**dados_gerais, **endereco}
                pesquisadores_data.append((pesquisador))
                
                formacoes = extract_formacao_academica(xml_dict)
                for formacao in formacoes:
                    formacao['lattes_id'] = dados_gerais['lattes_id']
                    formacoes_data.append((formacao))
                
                areas = extract_areas_atuacao(xml_dict)
                for area in areas:
                    area['lattes_id'] = dados_gerais['lattes_id']
                    areas_data.append((area))
                    
                print(f"Processado: {dados_gerais['nome_completo']}")
                
            except Exception as e:
                print(f"Erro ao processar {xml_file}: {str(e)}")
                continue
        
        self.save_all_xmls_to_parquet(pesquisadores_data, formacoes_data, areas_data)
        return 0
