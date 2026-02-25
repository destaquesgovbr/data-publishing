"""
Parquet shard upload to HuggingFace.

Handles temporary file creation, upload via HfApi, and metadata cleanup.
"""

import logging
import os
import tempfile

import pyarrow.parquet as pq
from huggingface_hub import HfApi
import pyarrow as pa


def upload_shard(
    api: HfApi,
    table: pa.Table,
    dataset_path: str,
    target_date: str,
    timestamp: str,
) -> str:
    """Save a PyArrow table as a Parquet shard and upload to HuggingFace.

    Args:
        api: Authenticated HfApi instance.
        table: PyArrow Table with the records to upload.
        dataset_path: HuggingFace dataset identifier.
        target_date: Date string (YYYY-MM-DD) for the shard filename.
        timestamp: Time string (HHMMSS) for the shard filename.

    Returns:
        The shard path in the repo (e.g. "data/train-2025-01-15-060000.parquet").
    """
    shard_name = f"data/train-{target_date}-{timestamp}.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        local_path = tmp.name
        pq.write_table(table, local_path, compression='snappy')
        logging.info(f"Parquet shard criado: {local_path}")

    try:
        api.upload_file(
            path_or_fileobj=local_path,
            path_in_repo=shard_name,
            repo_id=dataset_path,
            repo_type="dataset",
            commit_message=f"Add {table.num_rows} news from {target_date}",
        )
        logging.info(f"Parquet shard enviado: {shard_name}")
    finally:
        os.unlink(local_path)

    return shard_name


def force_metadata_refresh(api: HfApi, dataset_path: str) -> None:
    """Delete dataset_info.json to force the Hub to regenerate metadata.

    This avoids NonMatchingSplitsSizesError when new shards are added.

    Args:
        api: Authenticated HfApi instance.
        dataset_path: HuggingFace dataset identifier.
    """
    try:
        api.delete_file(
            path_in_repo="dataset_info.json",
            repo_id=dataset_path,
            repo_type="dataset",
            commit_message=f"Force metadata refresh for {dataset_path}",
        )
        logging.info("dataset_info.json deletado - Hub vai regenerar metadata")
    except Exception as e:
        logging.warning(f"Nao foi possivel deletar dataset_info.json: {e}")
