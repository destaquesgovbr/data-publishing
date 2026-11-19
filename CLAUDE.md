# Data Publishing

Pipelines de publicacao de dados do DestaquesGovBr.

## Estrutura

```
data-publishing/
├── src/data_publishing/
│   └── hf/                          # Modulo HuggingFace (deploy como plugin do Airflow)
│       ├── schema.py                 # PyArrow schema + HF_COLUMNS + conversao
│       ├── dedup.py                  # Consulta IDs existentes via Dataset Viewer API
│       ├── uploader.py               # Upload parquet shard + cleanup metadata
│       └── readme_sanitizer.py       # Sanitiza README.md (remove stale splits)
├── dags/
│   ├── sync_postgres_to_huggingface.py  # DAG principal
│   └── requirements.txt                 # Deps pip (referencia; nao e publicado, na VM vem da imagem)
├── tests/unit/
└── .github/workflows/
    └── composer-deploy-dags.yaml     # Deploy via reusable workflow
```

## Arquitetura

A DAG orquestra o sync PostgreSQL → HuggingFace. A logica de negocio esta nos modulos `src/data_publishing/hf/`, que sao deployados como **plugins** no Airflow (a pasta `plugins/` entra no `PYTHONPATH`).

A DAG roda no Airflow 3.1 proprio da VM `destaquesgovbr-airflow` (UI em `https://airflow.dgb.app.gov.br`, via IAP); o Cloud Composer foi desligado em 18/11/2026. O container `dag-sync` da VM espelha `gs://destaquesgovbr-airflow/{dags,plugins}` a cada 60 s. Os pacotes pip (`huggingface_hub`, `pyarrow`, `requests`) vem da imagem do Airflow (`infra/airflow/requirements.txt`, repo `infra`), nao do `dags/requirements.txt`.

Os crons usam o fuso padrao do Airflow (`America/Sao_Paulo`): o `0 6 * * *` da DAG roda as 06h de Brasilia (09h UTC), apesar do comentario no codigo.

### Deploy

O workflow usa o reusable workflow `airflow-deploy-dags.yml@v3` do repo `reusable-workflows`, que publica no bucket `destaquesgovbr-airflow` do Airflow proprio (a lista de buckets e o default da reusable, e o do Composer sai dela so la):
- DAGs vao para `{bucket}/dags/data-publishing/` (sem o `requirements.txt`)
- Plugins vao para `{bucket}/plugins/data_publishing/`

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
| `reusable-workflows` | Workflow reutilizavel de deploy (`airflow-deploy-dags.yml@v3`) |
| `scraper` | Scrapers que alimentam o PostgreSQL |
| `data-platform` | Enriquecimento (Cogfy), embeddings, Typesense |
| `infra` | Terraform (Cloud SQL, VM e imagem do Airflow, etc) |
