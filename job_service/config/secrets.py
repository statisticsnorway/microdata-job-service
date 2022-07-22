import json

from job_service.config import environment


def _initialize_secrets() -> dict:
    with open(environment.get('SECRETS_FILE'), encoding='utf-8') as f:
        secrets_file = json.load(f)
    return {
        'MONGODB_USER': secrets_file['MONGODB_USER'],
        'MONGODB_PASSWORD': secrets_file['MONGODB_PASSWORD'],
    }


_SECRETS = _initialize_secrets()


def get(key: str) -> str:
    return _SECRETS[key]
