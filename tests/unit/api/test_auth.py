import pytest
from pytest import MonkeyPatch

from job_service.api import auth
from job_service.exceptions import AuthError
from job_service.model.job import UserInfo
from tests.resources import test_data
from tests.util import generate_rsa_key_pairs, encode_jwt_payload

JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_rsa_key_pairs()
JWT_INVALID_PRIVATE_KEY, _ = generate_rsa_key_pairs()

expected_user_info = UserInfo(
    user_id='1234-1234-1234-1234',
    first_name='Test',
    last_name='Brukersen'
)


@pytest.fixture(autouse=True)
def setup(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        auth,
        'get_signing_key',
        lambda *a: JWT_PUBLIC_KEY.decode('utf-8')
    )


def test_auth_valid_token():
    token = encode_jwt_payload(
        test_data.valid_jwt_payload, JWT_PRIVATE_KEY
    )
    actual_user_info = auth.authorize_user(token)
    assert actual_user_info.user_id == expected_user_info.user_id
    assert actual_user_info.first_name == expected_user_info.first_name
    assert actual_user_info.last_name == expected_user_info.last_name


def test_auth_no_user_id():
    with pytest.raises(AuthError) as e:
        token = encode_jwt_payload(
            test_data.jwt_payload_no_user_id, JWT_PRIVATE_KEY
        )
        auth.authorize_user(token)
    assert "No valid userId" in str(e)


def test_auth_no_first_name():
    with pytest.raises(AuthError) as e:
        token = encode_jwt_payload(
            test_data.jwt_payload_no_first_name, JWT_PRIVATE_KEY
        )
        auth.authorize_user(token)
    assert "No valid firstName" in str(e)


def test_auth_no_last_name():
    with pytest.raises(AuthError) as e:
        token = encode_jwt_payload(
            test_data.jwt_payload_no_last_name, JWT_PRIVATE_KEY
        )
        auth.authorize_user(token)
    assert "No valid lastName" in str(e)


def test_auth_expired_token():
    with pytest.raises(AuthError) as e:
        token = encode_jwt_payload(
            test_data.jwt_payload_expired, JWT_PRIVATE_KEY
        )
        auth.authorize_user(token)
    assert "Signature has expired" in str(e)


def test_auth_missing_token():
    with pytest.raises(AuthError) as e:
        auth.authorize_user(None)
    assert "Unauthorized. No token was provided" in str(e)
