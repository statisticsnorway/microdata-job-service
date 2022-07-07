import os


def _initialize_environment() -> dict:
    return {
        'INPUT_DIR': os.environ['INPUT_DIR'],
        'MONGODB_URL': os.environ['MONGODB_URL'],
        'MONGODB_USER': os.environ['MONGODB_USER'],
        'MONGODB_PASSWORD': os.environ['MONGODB_PASSWORD']
    }


_ENVIRONMENT_VARIABLES = _initialize_environment()


def get(key: str) -> str:
    return _ENVIRONMENT_VARIABLES[key]
