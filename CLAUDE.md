# Data Publishing

Pipelines de publicacao de dados do DestaquesGovBr.

## Estrutura

```
data-publishing/
├── src/data_publishing/
│   └── hf/                          # Modulo HuggingFace (deploy como plugin Composer)
│       ├── schema.py                 # PyArrow schema + HF_COLUMNS + conversao
│       ├── dedup.py                  # Consulta IDs existentes via Dataset Viewer API
│       ├── uploader.py               # Upload parquet shard + cleanup metadata
│       └── readme_sanitizer.py       # Sanitiza README.md (remove stale splits)
├── dags/
│   ├── sync_postgres_to_huggingface.py  # DAG principal
│   └── requirements.txt                 # Deps pip para Composer
├── tests/unit/
└── .github/workflows/
    └── composer-deploy-dags.yaml     # Deploy via reusable workflow
```

## Arquitetura

A DAG orquestra o sync PostgreSQL → HuggingFace. A logica de negocio esta nos modulos `src/data_publishing/hf/`, que sao deployados como **plugins** no Cloud Composer (adicionados ao `PYTHONPATH` dos workers via `{bucket}/plugins/`).

### Deploy

O workflow usa o reusable workflow `composer-deploy-dags.yml@v1` do repo `reusable-workflows`:
- DAGs vao para `{bucket}/dags/data-publishing/`
- Plugins vao para `{bucket}/plugins/data-publishing/`

### Airflow Connections

- `postgres_default` — PostgreSQL (Cloud SQL)
- `huggingface_default` — HF token (no campo password)

## Datasets HuggingFace

- **Full**: `nitaibezerra/govbrnews` (24 colunas)
- **Reduced**: `nitaibezerra/govbrnews-reduced` (4 colunas: published_at, agency, title, url)

## Desenvolvimento

```bash
# Instalar deps
poetry install

# Rodar testes
poetry run pytest

# Rodar testes unitarios
poetry run pytest tests/unit/
```

## Repositorios Relacionados

| Repo | Descricao |
|------|-----------|
| `reusable-workflows` | Workflow reutilizavel de deploy (v1.2.0+) |
| `scraper` | Scrapers que alimentam o PostgreSQL |
| `data-platform` | Enriquecimento (Cogfy), embeddings, Typesense |
| `infra` | Terraform (Cloud SQL, Composer, etc) |
