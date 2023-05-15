import os


def _initialize_environment() -> dict:
    return {
        "INPUT_DIR": os.environ["INPUT_DIR"],
        "MONGODB_URL": os.environ["MONGODB_URL"],
        "JWKS_URL": os.environ["JWKS_URL"],
        "SECRETS_FILE": os.environ["SECRETS_FILE"],
        "DOCKER_HOST_NAME": os.environ["DOCKER_HOST_NAME"],
        "STACK": os.environ["STACK"],
        "JWT_AUTH": (
            False if os.environ.get("JWT_AUTH", "true") == "false" else True
        ),
    }


_ENVIRONMENT_VARIABLES = _initialize_environment()


def get(key: str) -> str:
    return _ENVIRONMENT_VARIABLES[key]
