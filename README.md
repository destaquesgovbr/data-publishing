# data-publishing

Pipelines de publicacao de dados do [DestaquesGovBr](https://github.com/destaquesgovbr).

## O que faz

Sincroniza noticias do PostgreSQL (Cloud SQL) para o HuggingFace, mantendo dois datasets publicos atualizados diariamente:

- [nitaibezerra/govbrnews](https://huggingface.co/datasets/nitaibezerra/govbrnews) — dataset completo (24 colunas)
- [nitaibezerra/govbrnews-reduced](https://huggingface.co/datasets/nitaibezerra/govbrnews-reduced) — dataset reduzido (4 colunas)

## Como funciona

A DAG `sync_postgres_to_huggingface` roda diariamente as 6 AM UTC no Cloud Composer:

1. Busca noticias do dia anterior no PostgreSQL
2. Consulta IDs ja existentes no HuggingFace (via Dataset Viewer API)
3. Filtra apenas registros novos
4. Cria parquet shard e faz upload incremental
5. Sanitiza metadata do README.md

Uso de memoria: ~10MB (apenas novos registros) vs ~1-2GB se baixasse o dataset completo.

## Estrutura

```
src/data_publishing/hf/    # Modulos Python (deploy como plugins no Composer)
dags/                      # DAGs Airflow
tests/unit/                # Testes unitarios
```

## Licenca

[AGPL-3.0](LICENSE)
