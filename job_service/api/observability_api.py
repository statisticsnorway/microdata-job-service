from flask import Blueprint


observability_api = Blueprint("observability", __name__)


@observability_api.get("/health/alive")
def alive():
    return "I'm alive!"


@observability_api.get("/health/ready")
def ready():
    return "I'm ready!"
