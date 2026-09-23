from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory."""

    source_dir = path_for('source_dir')
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'

    raw_dir.mkdir(parents=True, exist_ok=True)

    source_files = [
        'customers.csv',
        'products.json',
        'orders.csv',
    ]

    for filename in source_files:
        source_file = source_dir / filename

        if not source_file.exists():
            raise FileNotFoundError(f'Source file not found: {source_file}')

        shutil.copy2(source_file, raw_dir / filename)

    return raw_dir