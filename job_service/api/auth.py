import logging
from typing import Union

import jwt
from jwt import MissingRequiredClaimError, PyJWKClient
from jwt.exceptions import (
    InvalidSignatureError, ExpiredSignatureError,
    InvalidAudienceError, DecodeError
)

from job_service.config import environment
from job_service.exceptions import AuthError
from job_service.model.job import UserInfo


logger = logging.getLogger()
jwks_client = PyJWKClient(environment.get('JWKS_URL'), lifespan=3000)


def get_jwks_aud() -> str:
    return 'datastore-qa' if environment.get('STACK') == 'qa' else 'datastore'


def get_signing_key(jwt_token: str):
    return jwks_client.get_signing_key_from_jwt(jwt_token).key


def authorize_user(token: Union[str, None]) -> UserInfo:
    if token is None:
        raise AuthError('Unauthorized. No token was provided')
    try:
        signing_key = get_signing_key(token)
        decoded_jwt = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256", "RS512"],
            audience=get_jwks_aud(),
            options={
                'require': [
                    'aud',
                    'sub',
                    'accreditation/role',
                    'user/uuid',
                    'user/firstName',
                    'user/lastName'
                ]
            }
        )
        role = decoded_jwt.get('accreditation/role')
        if role != 'role/dataadministrator':
            raise AuthError(f'Can\'t start job with role: {role}')

        user_id = decoded_jwt.get('user/uuid')
        first_name = decoded_jwt.get('user/firstName')
        last_name = decoded_jwt.get('user/lastName')
        return UserInfo(
            user_id=str(user_id),
            first_name=str(first_name),
            last_name=str(last_name)
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
        KeyError
    ) as e:
        raise AuthError(f"Unauthorized: {e}") from e
    except Exception as e:
        raise Exception(f"Internal Server Error {e}") from e
