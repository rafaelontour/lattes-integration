# Pipeline ETL - Lattes Integration

Pipeline de extração, transformação e carga dos dados de currículos Lattes, utilizando a Arquitetura de Medalhão (Landing, Bronze, Silver e Gold)

## Requisitos

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (gerenciador de pacotes)

## Setup

### 1. Instalar o uv

#### Windows via PowerShell:

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Após instalar, feche e reabra o terminal para o PATH atualizar.

#### Linux

[oq tenho a ver, olhe a docs aqui](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer)

### 2. Instalar as dependências

```bash
uv sync
```

## Estrutura do Data Storage

Os dados da pipeline são salvos em `pipeline-etl/data-storage` seguindo a arquitetura de medalhão, com a intenção de simular um Data Lake.

A organização é por camada (bronze, silver ou gold), fonte (open_alex ou lattes), data de execução e entidade.

O padrão de diretório é:

`pipeline-etl/data-storage/<camada>/<source_name>/<data_execucao>/<entidade>/<arquivo_<data_execucao>.parquet`

```
pipeline-etl/data-storage/
├── 00-landing/                 # Apenas o XML zipado
├── 01-bronze/                  # Extração das fontes
│   ├── lattes/
│   │   └── <data_execucao>/
│   │       ├── areas_atuacao/
│   │       ├── formacoes/
│   │       └── pesquisadores/
│   └── open-alex/
│       └── <data_execucao>/
│           ├── authors/
│           └── works/
├── 02-silver/                  # Tratamento das fontes e entidades
│   ├── lattes/
│   │   └── <data_execucao>/
│   │       ├── pesquisadores/
│   │       └── producoes/
│   └── open-alex/
│       └── <data_execucao>/
│           ├── authors/
│           └── works/
│── 03-gold/                    # Enriquecimento com dados da OpenAlex
│   └── lattes/
│      └── <data_execucao>/
│          ├── pesquisadores/
│          └── producoes/

```
