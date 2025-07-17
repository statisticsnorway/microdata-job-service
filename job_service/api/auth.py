import logging
from typing import Union

import jwt
from jwt import MissingRequiredClaimError, PyJWKClient
from jwt.exceptions import (
    InvalidSignatureError,
    ExpiredSignatureError,
    InvalidAudienceError,
    DecodeError,
)

from job_service.config import environment
from job_service.exceptions import AuthError
from job_service.adapter.db.models import UserInfo


logger = logging.getLogger()
jwks_client = PyJWKClient(environment.get("JWKS_URL"), lifespan=3000)


def get_jwks_aud() -> str:
    return "datastore-qa" if environment.get("STACK") == "qa" else "datastore"


def get_signing_key(jwt_token: str):
    return jwks_client.get_signing_key_from_jwt(jwt_token).key


def authorize_user(
    authorization: Union[str, None], user_info: Union[str, None]
) -> UserInfo:
    if not environment.get("JWT_AUTH"):
        logger.warning("JWT_AUTH is turned off.")
        return UserInfo(
            user_id="1234-1234-1234-1234", first_name="Test", last_name="User"
        )
    if authorization is None:
        raise AuthError("Unauthorized. No authorization token was provided")
    if user_info is None:
        raise AuthError("Unauthorized. No user info token was provided")
    try:
        signing_key = get_signing_key(authorization)
        decoded_authorization = jwt.decode(
            authorization,
            signing_key,
            algorithms=["RS256", "RS512"],
            audience=get_jwks_aud(),
            options={
                "require": ["aud", "sub", "accreditation/role", "user/uuid"]
            },
        )
        role = decoded_authorization.get("accreditation/role")
        if role != "role/dataadministrator":
            raise AuthError(f"Can't start job with role: {role}")
        decoded_user_info = jwt.decode(
            user_info,
            signing_key,
            algorithms=["RS256", "RS512"],
            options={
                "require": ["user/uuid", "user/firstName", "user/lastName"],
                "verify_aud": False,
            },
        )
        if decoded_authorization.get("user/uuid") != decoded_user_info.get(
            "user/uuid"
        ):
            raise AuthError("Token mismatch")
        user_id = decoded_user_info.get("user/uuid")
        first_name = decoded_user_info.get("user/firstName")
        last_name = decoded_user_info.get("user/lastName")
        return UserInfo(
            user_id=str(user_id),
            first_name=str(first_name),
            last_name=str(last_name),
        )
    except AuthError as e:
        raise e
    except (
        InvalidSignatureError,
        ExpiredSignatureError,
        InvalidAudienceError,
        MissingRequiredClaimError,
        DecodeError,
        ValueError,
        AttributeError,
        KeyError,
    ) as e:
        raise AuthError(f"Unauthorized: {e}") from e
    except Exception as e:
        raise Exception(f"Internal Server Error {e}") from e
