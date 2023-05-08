import logging
import os

import jwt
from jwt import PyJWKClient
from jwt.exceptions import (
    InvalidSignatureError, ExpiredSignatureError,
    InvalidAudienceError, DecodeError
)

from job_service.config import environment
from job_service.exceptions import AuthError, NoUserError
from job_service.model.job import UserInfo


logger = logging.getLogger()
jwks_client = PyJWKClient(environment.get('JWKS_URL'), lifespan=3000)


def get_jwks_aud() -> str:
    return 'job-service-qa' if os.environ['STACK'] == 'qa' else 'job-service'


def get_signing_key(jwt_token: str):
    return jwks_client.get_signing_key_from_jwt(jwt_token).key


def authorize_user(token: str) -> UserInfo:
    if token is None:
        raise Exception('Unauthorized. No token was provided')
    try:
        signing_key = get_signing_key(token)
        decoded_jwt = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256", "RS512"]
            # audience=get_jwks_aud()
        )
        user_id = str(decoded_jwt.get('user/uuid'))
        first_name = str(decoded_jwt.get('user/firstName'))
        last_name = str(decoded_jwt.get('user/lastName'))
        if user_id in [None, '']:
            raise NoUserError('No valid userId')
        if first_name in [None, '']:
            raise NoUserError('No valid firstName')
        if last_name in [None, '']:
            raise NoUserError('No valid lastName')
        return UserInfo(
            user_id=user_id,
            first_name=first_name,
            last_name=last_name
        )
    except (
        InvalidSignatureError,
        ExpiredSignatureError,
        InvalidAudienceError,
        NoUserError,
        DecodeError,
        ValueError,
        AttributeError,
        KeyError
    ) as e:
        raise AuthError(f"Unauthorized {e}") from e
    except Exception as e:
        raise Exception(f"Internal Server Error {e}") from e
