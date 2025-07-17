from fastapi import APIRouter


observability_api = APIRouter()


@observability_api.get("/health/alive")
def alive():
    return "I'm alive!"


@observability_api.get("/health/ready")
def ready():
    return "I'm ready!"
