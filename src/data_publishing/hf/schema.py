"""
PyArrow schema definitions and record conversion for HuggingFace datasets.

Defines the column layout, Arrow schemas, and conversion logic for the
govbrnews dataset (full and reduced versions).
"""

from datetime import timedelta, timezone

import pyarrow as pa

# Timezone do Brasil (UTC-3) - usado nos arquivos base do dataset
BRT = timezone(timedelta(hours=-3))

# HuggingFace dataset paths
DATASET_PATH = "nitaibezerra/govbrnews"
REDUCED_DATASET_PATH = "nitaibezerra/govbrnews-reduced"

# Colunas do dataset HuggingFace (ordem importante)
HF_COLUMNS = [
    "unique_id", "agency", "published_at", "updated_datetime", "extracted_at",
    "title", "subtitle", "editorial_lead", "url", "content",
    "image", "video_url", "category", "tags",
    "theme_1_level_1", "theme_1_level_1_code", "theme_1_level_1_label",
    "theme_1_level_2_code", "theme_1_level_2_label",
    "theme_1_level_3_code", "theme_1_level_3_label",
    "most_specific_theme_code", "most_specific_theme_label",
    "summary",
]

# Colunas do dataset reduzido
REDUCED_COLUMNS = ["published_at", "agency", "title", "url"]

# SQL query para buscar noticias do dia
SQL_QUERY = """
    SELECT
        n.unique_id,
        n.agency_key as agency,
        n.published_at,
        n.updated_datetime,
        n.extracted_at,
        n.title,
        n.subtitle,
        n.editorial_lead,
        n.url,
        n.content,
        n.image_url as image,
        n.video_url,
        n.category,
        n.tags,
        t1.label as theme_1_level_1,
        t1.code as theme_1_level_1_code,
        t1.label as theme_1_level_1_label,
        t2.code as theme_1_level_2_code,
        t2.label as theme_1_level_2_label,
        t3.code as theme_1_level_3_code,
        t3.label as theme_1_level_3_label,
        tm.code as most_specific_theme_code,
        tm.label as most_specific_theme_label,
        n.summary
    FROM news n
    LEFT JOIN themes t1 ON n.theme_l1_id = t1.id
    LEFT JOIN themes t2 ON n.theme_l2_id = t2.id
    LEFT JOIN themes t3 ON n.theme_l3_id = t3.id
    LEFT JOIN themes tm ON n.most_specific_theme_id = tm.id
    WHERE n.published_at >= %s
      AND n.published_at < %s::date + INTERVAL '1 day'
    ORDER BY n.published_at DESC
"""


def build_arrow_schema() -> pa.Schema:
    """Build the full PyArrow schema matching the HuggingFace dataset.

    Timestamp types match the existing base files:
    - published_at: timestamp[us, tz=-03:00]
    - updated_datetime: timestamp[us, tz=-03:00]
    - extracted_at: timestamp[ns] (naive, no timezone)
    """
    return pa.schema([
        ("unique_id", pa.string()),
        ("agency", pa.string()),
        ("published_at", pa.timestamp('us', tz='-03:00')),
        ("updated_datetime", pa.timestamp('us', tz='-03:00')),
        ("extracted_at", pa.timestamp('ns')),
        ("title", pa.string()),
        ("subtitle", pa.string()),
        ("editorial_lead", pa.string()),
        ("url", pa.string()),
        ("content", pa.string()),
        ("image", pa.string()),
        ("video_url", pa.string()),
        ("category", pa.string()),
        ("tags", pa.list_(pa.string())),
        ("theme_1_level_1", pa.string()),
        ("theme_1_level_1_code", pa.string()),
        ("theme_1_level_1_label", pa.string()),
        ("theme_1_level_2_code", pa.string()),
        ("theme_1_level_2_label", pa.string()),
        ("theme_1_level_3_code", pa.string()),
        ("theme_1_level_3_label", pa.string()),
        ("most_specific_theme_code", pa.string()),
        ("most_specific_theme_label", pa.string()),
        ("summary", pa.string()),
    ])


def build_reduced_schema() -> pa.Schema:
    """Build the reduced PyArrow schema (4 essential columns)."""
    return pa.schema([
        ("published_at", pa.timestamp('us', tz='-03:00')),
        ("agency", pa.string()),
        ("title", pa.string()),
        ("url", pa.string()),
    ])


def records_to_arrow_table(
    records: list[tuple],
    columns: list[str],
    schema: pa.Schema,
) -> pa.Table:
    """Convert PostgreSQL records to a PyArrow Table.

    Handles timezone conversion:
    - published_at, updated_datetime → BRT (-03:00)
    - extracted_at → naive (UTC without tzinfo)

    Args:
        records: Raw rows from PostgreSQL (list of tuples).
        columns: Column names matching the record positions.
        schema: PyArrow schema for the output table.

    Returns:
        PyArrow Table ready for Parquet serialization.
    """
    data_dict = {col: [] for col in columns}

    for record in records:
        for i, col in enumerate(columns):
            value = record[i]
            if col in ('published_at', 'updated_datetime'):
                if value is not None and hasattr(value, 'astimezone'):
                    value = value.astimezone(BRT)
            elif col == 'extracted_at':
                if value is not None and hasattr(value, 'replace'):
                    if hasattr(value, 'tzinfo') and value.tzinfo is not None:
                        value = value.astimezone(timezone.utc).replace(tzinfo=None)
            data_dict[col].append(value)

    return pa.table(data_dict, schema=schema)
