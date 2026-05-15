# Pipeline ETL - Lattes Integration

Pipeline de extração, transformação e carga dos dados de currículos Lattes.

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
