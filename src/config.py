from pathlib import Path
import os
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load environment-specific values from the local .env file.
load_dotenv(PROJECT_ROOT / '.env')

with (PROJECT_ROOT / 'config' / 'settings.yml').open(encoding='utf-8') as f:
    SETTINGS = yaml.safe_load(f)


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f'Missing required environment variable: {name}')
    return value


DB = {
    'host': required_env('POSTGRES_HOST'),
    'port': int(required_env('POSTGRES_PORT')),
    'dbname': required_env('POSTGRES_DB'),
    'user': required_env('POSTGRES_USER'),
    'password': required_env('POSTGRES_PASSWORD'),
}


def path_for(key: str) -> Path:
    return PROJECT_ROOT / SETTINGS['pipeline'][key]