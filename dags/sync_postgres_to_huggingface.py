"""
DAG para sincronizar noticias do PostgreSQL para HuggingFace.

Executa diariamente apos o pipeline de scraper/enrichment.
Processa noticias do dia anterior (logical_date - 1 day).

ABORDAGEM: Append incremental via parquet shards
- Consulta IDs existentes via Dataset Viewer API (sem baixar dataset)
- Cria parquet shard com novos registros
- Upload direto via huggingface_hub

Memoria: ~10MB (apenas novos registros) vs ~1-2GB (dataset completo)
"""

from datetime import datetime, timedelta, timezone
import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook


@dag(
    dag_id="sync_postgres_to_huggingface",
    description="Sincroniza noticias do PostgreSQL para HuggingFace diariamente",
    schedule="0 6 * * *",  # 6 AM UTC (apos pipeline das 4 AM)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sync", "huggingface", "postgres", "daily"],
    default_args={
        "owner": "data-publishing",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
)
def sync_postgres_to_huggingface_dag():
    """
    DAG que sincroniza noticias do PostgreSQL para HuggingFace.

    Usa abordagem incremental via parquet shards para evitar OOM.
    """

    @task
    def sync_news_to_huggingface(logical_date=None) -> dict:
        """
        Task que sincroniza noticias do PostgreSQL para HuggingFace.

        Abordagem:
        1. Le noticias do dia anterior do PostgreSQL
        2. Consulta IDs existentes via HuggingFace API (sem baixar dataset)
        3. Filtra apenas novos registros
        4. Cria parquet shard e faz upload

        Returns:
            dict: Estatisticas do sync
        """
        from huggingface_hub import HfApi
        from data_publishing.hf.schema import (
            HF_COLUMNS, REDUCED_COLUMNS,
            DATASET_PATH, REDUCED_DATASET_PATH,
            SQL_QUERY,
            build_arrow_schema, build_reduced_schema,
            records_to_arrow_table,
        )
        from data_publishing.hf.dedup import get_existing_ids_for_date
        from data_publishing.hf.uploader import upload_shard, force_metadata_refresh
        from data_publishing.hf.readme_sanitizer import sanitize_readme

        # Obter data alvo (dia anterior ao logical_date)
        if logical_date is None:
            logical_date = datetime.now(timezone.utc)
            logging.info("Execucao manual detectada - usando data atual como logical_date")
        target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        logging.info(f"Iniciando sync para data: {target_date}")

        # Configurar HF_TOKEN da connection
        hf_conn = BaseHook.get_connection('huggingface_default')
        hf_token = hf_conn.password

        # ==========================================
        # 1. Query PostgreSQL
        # ==========================================
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        records = pg_hook.get_records(SQL_QUERY, parameters=[target_date, target_date])
        logging.info(f"Encontrados {len(records)} registros no PostgreSQL para {target_date}")

        if not records:
            logging.warning(f"Nenhum registro encontrado para {target_date}. Pulando sync.")
            return {
                "status": "skipped",
                "target_date": target_date,
                "records_from_pg": 0,
                "records_synced": 0,
            }

        # ==========================================
        # 2. Consultar IDs existentes via API
        # ==========================================
        existing_ids = get_existing_ids_for_date(DATASET_PATH, target_date)

        # ==========================================
        # 3. Filtrar apenas novos registros
        # ==========================================
        # Converter records para filtrar por unique_id (index 0)
        new_records = [r for r in records if r[0] not in existing_ids]

        if not new_records:
            logging.info(f"Todos os {len(records)} registros ja existem no HuggingFace. Pulando sync.")
            return {
                "status": "skipped",
                "target_date": target_date,
                "records_from_pg": len(records),
                "records_already_exist": len(existing_ids),
                "records_synced": 0,
            }

        logging.info(f"Novos registros a sincronizar: {len(new_records)} de {len(records)}")

        # ==========================================
        # 4. Criar tabelas Arrow e upload
        # ==========================================
        full_schema = build_arrow_schema()
        full_table = records_to_arrow_table(new_records, HF_COLUMNS, full_schema)

        timestamp = datetime.now(timezone.utc).strftime('%H%M%S')
        api = HfApi(token=hf_token)

        # Upload full dataset shard
        shard_name = upload_shard(api, full_table, DATASET_PATH, target_date, timestamp)
        force_metadata_refresh(api, DATASET_PATH)

        # ==========================================
        # 5. Atualizar dataset reduzido
        # ==========================================
        reduced_schema = build_reduced_schema()
        # Build reduced records: published_at(2), agency(1), title(5), url(8)
        reduced_indices = [HF_COLUMNS.index(c) for c in REDUCED_COLUMNS]
        reduced_records = [tuple(r[i] for i in reduced_indices) for r in new_records]
        reduced_table = records_to_arrow_table(reduced_records, REDUCED_COLUMNS, reduced_schema)

        upload_shard(api, reduced_table, REDUCED_DATASET_PATH, target_date, timestamp)
        force_metadata_refresh(api, REDUCED_DATASET_PATH)

        # ==========================================
        # 6. Sanitizar README.md (remover splits metadata)
        # ==========================================
        sanitize_readme(api, DATASET_PATH)
        sanitize_readme(api, REDUCED_DATASET_PATH)

        # ==========================================
        # 7. Log final
        # ==========================================
        logging.info("=" * 60)
        logging.info("PostgreSQL -> HuggingFace Sync Concluido")
        logging.info("=" * 60)
        logging.info(f"Data processada: {target_date}")
        logging.info(f"Registros do PostgreSQL: {len(records)}")
        logging.info(f"Registros ja existentes: {len(existing_ids)}")
        logging.info(f"Registros sincronizados: {len(new_records)}")
        logging.info(f"Shard: {shard_name}")
        logging.info("=" * 60)

        return {
            "status": "success",
            "target_date": target_date,
            "records_from_pg": len(records),
            "records_already_exist": len(existing_ids),
            "records_synced": len(new_records),
            "shard_name": shard_name,
            "dataset_path": DATASET_PATH,
        }

    # Executar task
    sync_news_to_huggingface()


# Instanciar DAG
dag_instance = sync_postgres_to_huggingface_dag()
