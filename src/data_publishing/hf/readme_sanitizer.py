"""
README.md sanitizer for HuggingFace datasets.

Removes stale splits metadata from the YAML front matter so the Hub
auto-calculates correct values after new shards are added.
"""

import logging
import os
import re
import tempfile

from huggingface_hub import HfApi


def sanitize_readme(api: HfApi, repo_id: str) -> None:
    """Remove stale splits metadata from a dataset's README.md.

    The HuggingFace Hub may insert split size/download metadata into the
    README YAML front matter. This becomes outdated when new Parquet shards
    are added. Removing it lets the Hub recalculate automatically.

    Args:
        api: Authenticated HfApi instance.
        repo_id: HuggingFace dataset identifier.
    """
    try:
        readme_path = api.hf_hub_download(
            repo_id=repo_id,
            filename="README.md",
            repo_type="dataset",
        )
        with open(readme_path, 'r') as f:
            content = f.read()

        if '---' not in content or 'splits:' not in content:
            return

        parts = content.split('---')
        if len(parts) < 3:
            return

        yaml_section = parts[1]
        rest = '---'.join(parts[2:])

        original_yaml = yaml_section
        yaml_section = re.sub(
            r'\n\s+splits:.*?(?=\n\s*[a-z]|\n\s*$|\Z)',
            '',
            yaml_section,
            flags=re.DOTALL,
        )
        yaml_section = re.sub(r'\n\s+download_size:.*', '', yaml_section)
        yaml_section = re.sub(r'\n\s+dataset_size:.*', '', yaml_section)

        if yaml_section != original_yaml:
            new_content = f"---{yaml_section}---{rest}"
            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.md', delete=False
            ) as tmp:
                tmp.write(new_content)
                tmp_path = tmp.name

            try:
                api.upload_file(
                    path_or_fileobj=tmp_path,
                    path_in_repo="README.md",
                    repo_id=repo_id,
                    repo_type="dataset",
                    commit_message="Remove stale splits metadata",
                )
                logging.info(f"README.md sanitizado em {repo_id}")
            finally:
                os.unlink(tmp_path)

    except Exception as e:
        logging.warning(f"Nao foi possivel sanitizar README.md de {repo_id}: {e}")
