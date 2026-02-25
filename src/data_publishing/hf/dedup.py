"""
Deduplication via HuggingFace Dataset Viewer API.

Queries existing unique_ids for a given date to avoid uploading
duplicate records.
"""

import logging

import requests

HF_API_BASE = "https://datasets-server.huggingface.co"


def get_existing_ids_for_date(dataset_path: str, date_str: str) -> set[str]:
    """Query unique_ids already in the HuggingFace dataset for a given date.

    Uses the Dataset Viewer /filter endpoint to paginate through existing
    records without downloading the full dataset.

    Args:
        dataset_path: HuggingFace dataset identifier (e.g. "nitaibezerra/govbrnews").
        date_str: Date string in YYYY-MM-DD format.

    Returns:
        Set of unique_id strings already present in the dataset for that date.
    """
    existing_ids: set[str] = set()
    offset = 0
    max_iterations = 100  # Safety limit

    logging.info(f"Consultando IDs existentes para {date_str} via API...")

    for _ in range(max_iterations):
        try:
            url = f"{HF_API_BASE}/filter"
            params = {
                "dataset": dataset_path,
                "config": "default",
                "split": "train",
                "where": (
                    f"\"published_at\">'{date_str}T00:00:00' "
                    f"AND \"published_at\"<'{date_str}T23:59:59'"
                ),
                "offset": offset,
                "length": 100,
            }
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code != 200:
                logging.warning(f"API retornou status {resp.status_code}: {resp.text[:200]}")
                break

            data = resp.json()

            if "error" in data:
                logging.warning(f"API retornou erro: {data['error']}")
                break

            rows = data.get("rows", [])
            if not rows:
                break

            for row in rows:
                existing_ids.add(row["row"]["unique_id"])

            offset += 100
            if len(rows) < 100:
                break

        except requests.RequestException as e:
            logging.warning(f"Erro ao consultar API: {e}")
            break

    logging.info(f"Encontrados {len(existing_ids)} IDs existentes no HuggingFace para {date_str}")
    return existing_ids
