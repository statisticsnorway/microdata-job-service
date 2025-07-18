import os


def _initialize_environment() -> dict:
    return {
        "INPUT_DIR": os.environ["INPUT_DIR"],
        "SQLITE_URL": os.environ["SQLITE_URL"],
        "JWKS_URL": os.environ["JWKS_URL"],
        "DOCKER_HOST_NAME": os.environ["DOCKER_HOST_NAME"],
        "STACK": os.environ["STACK"],
        "JWT_AUTH": (
            False if os.environ.get("JWT_AUTH", "true") == "false" else True
        ),
        "BUMP_ENABLED": (
            True
            if os.environ.get("BUMP_ENABLED", "false") == "true"
            else False
        ),
        "COMMIT_ID": os.environ["COMMIT_ID"],
    }


_ENVIRONMENT_VARIABLES = _initialize_environment()


def get(key: str) -> str:
    return _ENVIRONMENT_VARIABLES[key]
