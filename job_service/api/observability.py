from fastapi import APIRouter


router = APIRouter()


@router.get("/health/alive")
def alive():
    return "I'm alive!"


@router.get("/health/ready")
def ready():
    return "I'm ready!"
