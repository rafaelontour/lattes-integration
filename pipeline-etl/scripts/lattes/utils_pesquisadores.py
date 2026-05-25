from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime
import polars as pl

def extract_areas_atuacao(xml_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        dados_gerais = xml_dict.get('CURRICULO-VITAE', {}).get('DADOS-GERAIS', {})
        areas = dados_gerais.get('AREAS-DE-ATUACAO', {})
        
        areas_list = areas.get('AREA-DE-ATUACAO', [])
        if not isinstance(areas_list, list):
            areas_list = [areas_list] if areas_list else []
        
        result = []
        for area in areas_list:
            if area:
                result.append({
                    'sequencia': area.get('@SEQUENCIA-AREA-DE-ATUACAO'),
                    'nome_grande_area': area.get('@NOME-GRANDE-AREA-DO-CONHECIMENTO'),
                    'codigo_grande_area': area.get('@CODIGO-GRANDE-AREA-DO-CONHECIMENTO'),
                    'nome_area': area.get('@NOME-DA-AREA-DO-CONHECIMENTO'),
                    'codigo_area': area.get('@CODIGO-DA-AREA-DO-CONHECIMENTO'),
                    'nome_sub_area': area.get('@NOME-DA-SUB-AREA-DO-CONHECIMENTO'),
                    'codigo_sub_area': area.get('@CODIGO-DA-SUB-AREA-DO-CONHECIMENTO'),
                    'nome_especialidade': area.get('@NOME-DA-ESPECIALIDADE'),
                    'codigo_especialidade': area.get('@CODIGO-DA-ESPECIALIDADE')
                })
        
        return result
    except Exception as e:
        raise ValueError(f"Erro ao extrair áreas de atuação: {str(e)}")


def extract_dados_gerais(xml_dict: Dict[str, Any]) -> Dict[str, Any]:
    try:
        curriculo = xml_dict.get('CURRICULO-VITAE', {})
        dados_gerais = curriculo.get('DADOS-GERAIS', {})
        
        return {
            'lattes_id': extract_id_lattes(xml_dict),
            'orcid_id': dados_gerais.get('@ORCID-ID'),
            'nome_completo': dados_gerais.get('@NOME-COMPLETO'),
            'nome_citacao': dados_gerais.get('@NOME-EM-CITACOES-BIBLIOGRAFICAS'),
            'nacionalidade': dados_gerais.get('@PAIS-DE-NACIONALIDADE'),
            'pais_nascimento': dados_gerais.get('@PAIS-DE-NASCIMENTO'),
            'uf_nascimento': dados_gerais.get('@UF-NASCIMENTO'),
            'cidade_nascimento': dados_gerais.get('@CIDADE-NASCIMENTO'),
            'data_atualizacao': curriculo.get('@DATA-ATUALIZACAO') or dados_gerais.get('@DATA-ATUALIZACAO'),
            'data_hora_atualizacao': curriculo.get('@HORA-ATUALIZACAO') or dados_gerais.get('@DATA-HORA-ATUALIZACAO'),
            'sigla_pais_nacionalidade': dados_gerais.get('@SIGLA-PAIS-NACIONALIDADE'),
            'texto_resumo_cv': extract_resumo_cv(dados_gerais)
        }
    except Exception as e:
        raise ValueError(f"Erro ao extrair dados gerais: {str(e)}")


def extract_id_lattes(xml_dict: Dict[str, Any]) -> Optional[str]:
    try:
        return xml_dict.get('CURRICULO-VITAE', {}).get('@NUMERO-IDENTIFICADOR')
    except:
        return None


def extract_resumo_cv(dados_gerais: Dict[str, Any]) -> Optional[str]:
    try:
        resumo = dados_gerais.get('RESUMO-CV', {})
        if isinstance(resumo, dict):
            return resumo.get('@TEXTO-RESUMO-CV-RH')
        return None
    except:
        return None
    

def extract_endereco_profissional(xml_dict: Dict[str, Any]) -> Dict[str, Any]:
    try:
        dados_gerais = xml_dict.get('CURRICULO-VITAE', {}).get('DADOS-GERAIS', {})
        endereco = dados_gerais.get('ENDERECO', {})
        endereco_prof = endereco.get('ENDERECO-PROFISSIONAL', {}) if endereco else {}
        
        return {
            'instituicao_empresa': endereco_prof.get('@NOME-INSTITUICAO-EMPRESA'),
            'codigo_instituicao': endereco_prof.get('@CODIGO-INSTITUICAO-EMPRESA'),
            'nome_orgao': endereco_prof.get('@NOME-ORGAO'),
            'nome_unidade': endereco_prof.get('@NOME-UNIDADE'),
            'logradouro': endereco_prof.get('@LOGRADOURO-COMPLEMENTO'),
            'pais': endereco_prof.get('@PAIS'),
            'uf': endereco_prof.get('@UF'),
            'cidade': endereco_prof.get('@CIDADE'),
            'bairro': endereco_prof.get('@BAIRRO'),
            'email': endereco_prof.get('@E-MAIL'),
            'home_page': endereco_prof.get('@HOME-PAGE')
        }
    except Exception as e:
        raise ValueError(f"Erro ao extrair endereço profissional: {str(e)}")
    

def extract_formacao_academica(xml_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        dados_gerais = xml_dict.get('CURRICULO-VITAE', {}).get('DADOS-GERAIS', {})
        formacao = dados_gerais.get('FORMACAO-ACADEMICA-TITULACAO', {})
        
        formacoes = []
        
        formacoes.extend(extract_graduacao(formacao))
        formacoes.extend(extract_mestrado(formacao))
        formacoes.extend(extract_doutorado(formacao))
        formacoes.extend(extract_pos_doutorado(formacao))
        
        return formacoes
    except Exception as e:
        raise ValueError(f"Erro ao extrair formação acadêmica: {str(e)}")


def extract_graduacao(formacao: Dict[str, Any]) -> List[Dict[str, Any]]:
    graduacoes = formacao.get('GRADUACAO', [])
    if not isinstance(graduacoes, list):
        graduacoes = [graduacoes] if graduacoes else []
    
    result = []
    for grad in graduacoes:
        if grad:
            result.append({
                'tipo_formacao': 'GRADUACAO',
                'codigo_curso': grad.get('@CODIGO-CURSO'),
                'nome_curso': grad.get('@NOME-CURSO'),
                'codigo_instituicao': grad.get('@CODIGO-INSTITUICAO'),
                'nome_instituicao': grad.get('@NOME-INSTITUICAO'),
                'status': grad.get('@STATUS-DO-CURSO'),
                'ano_inicio': grad.get('@ANO-DE-INICIO'),
                'ano_conclusao': grad.get('@ANO-DE-CONCLUSAO'),
                'titulo_trabalho': grad.get('@TITULO-DO-TRABALHO-DE-CONCLUSAO-DE-CURSO'),
                'nome_orientador': grad.get('@NOME-DO-ORIENTADOR')
            })
    return result


def extract_mestrado(formacao: Dict[str, Any]) -> List[Dict[str, Any]]:
    mestrados = formacao.get('MESTRADO', [])
    if not isinstance(mestrados, list):
        mestrados = [mestrados] if mestrados else []
    
    result = []
    for mest in mestrados:
        if mest:
            result.append({
                'tipo_formacao': 'MESTRADO',
                'codigo_curso': mest.get('@CODIGO-CURSO'),
                'nome_curso': mest.get('@NOME-CURSO'),
                'codigo_instituicao': mest.get('@CODIGO-INSTITUICAO'),
                'nome_instituicao': mest.get('@NOME-INSTITUICAO'),
                'status': mest.get('@STATUS-DO-CURSO'),
                'ano_inicio': mest.get('@ANO-DE-INICIO'),
                'ano_conclusao': mest.get('@ANO-DE-CONCLUSAO'),
                'titulo_dissertacao': mest.get('@TITULO-DA-DISSERTACAO-TESE'),
                'nome_orientador': mest.get('@NOME-COMPLETO-DO-ORIENTADOR'),
                'codigo_agencia_financiadora': mest.get('@CODIGO-AGENCIA-FINANCIADORA'),
                'nome_agencia': mest.get('@NOME-AGENCIA')
            })
    return result


def extract_doutorado(formacao: Dict[str, Any]) -> List[Dict[str, Any]]:
    doutorados = formacao.get('DOUTORADO', [])
    if not isinstance(doutorados, list):
        doutorados = [doutorados] if doutorados else []
    
    result = []
    for dout in doutorados:
        if dout:
            result.append({
                'tipo_formacao': 'DOUTORADO',
                'codigo_curso': dout.get('@CODIGO-CURSO'),
                'nome_curso': dout.get('@NOME-CURSO'),
                'codigo_instituicao': dout.get('@CODIGO-INSTITUICAO'),
                'nome_instituicao': dout.get('@NOME-INSTITUICAO'),
                'status': dout.get('@STATUS-DO-CURSO'),
                'ano_inicio': dout.get('@ANO-DE-INICIO'),
                'ano_conclusao': dout.get('@ANO-DE-CONCLUSAO'),
                'titulo_tese': dout.get('@TITULO-DA-DISSERTACAO-TESE'),
                'nome_orientador': dout.get('@NOME-COMPLETO-DO-ORIENTADOR'),
                'codigo_agencia_financiadora': dout.get('@CODIGO-AGENCIA-FINANCIADORA'),
                'nome_agencia': dout.get('@NOME-AGENCIA')
            })
    return result


def extract_pos_doutorado(formacao: Dict[str, Any]) -> List[Dict[str, Any]]:
    pos_docs = formacao.get('POS-DOUTORADO', [])
    if not isinstance(pos_docs, list):
        pos_docs = [pos_docs] if pos_docs else []
    
    result = []
    for pos in pos_docs:
        if pos:
            result.append({
                'tipo_formacao': 'POS-DOUTORADO',
                'codigo_instituicao': pos.get('@CODIGO-INSTITUICAO'),
                'nome_instituicao': pos.get('@NOME-INSTITUICAO'),
                'status': pos.get('@STATUS-DO-ESTAGIO'),
                'ano_inicio': pos.get('@ANO-DE-INICIO'),
                'ano_conclusao': pos.get('@ANO-DE-CONCLUSAO'),
                'codigo_agencia_financiadora': pos.get('@CODIGO-AGENCIA-FINANCIADORA'),
                'nome_agencia': pos.get('@NOME-DA-AGENCIA')
            })
    return result

def convert_string_columns_to_date(df, date_columns, dt_format= '%d/%m/%Y', strict= True):
    """Função para converter algumas colunas
    string para o formato date.
    É opcional informar o formato de data
    esperado.

    Parameters
    ----------
    df : polars.DataFrame
        DataFrame a sofrer transformação.
    
    columns_list : list
        Lista de colunas string.

    dt_format : str, optional
        Formato de data esperado. Ex: '%d/%m/%Y'

    Returns
    -------
    polars.DataFrame
        DataFrame com colunas transformadas.
    """

    df = df.with_columns(pl.col(date_columns).str.to_date(format="%d%m%Y"))
    return df