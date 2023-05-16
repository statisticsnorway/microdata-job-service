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
    user_id="1234-1234-1234-1234", first_name="Test", last_name="Brukersen"
)


@pytest.fixture(autouse=True)
def setup(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        auth, "get_signing_key", lambda *a: JWT_PUBLIC_KEY.decode("utf-8")
    )


def test_auth_valid_tokens():
    auth_token = encode_jwt_payload(
        test_data.valid_authorization_payload, JWT_PRIVATE_KEY
    )
    user_info_token = encode_jwt_payload(
        test_data.valid_user_info_payload, JWT_PRIVATE_KEY
    )
    actual_user_info = auth.authorize_user(auth_token, user_info_token)
    assert actual_user_info.user_id == expected_user_info.user_id
    assert actual_user_info.first_name == expected_user_info.first_name
    assert actual_user_info.last_name == expected_user_info.last_name


def test_user_info_no_user_id():
    with pytest.raises(AuthError) as e:
        auth_token = encode_jwt_payload(
            test_data.authorization_payload_no_user_id, JWT_PRIVATE_KEY
        )
        user_info_token = encode_jwt_payload(
            test_data.valid_user_info_payload, JWT_PRIVATE_KEY
        )
        auth.authorize_user(auth_token, user_info_token)
    assert 'Token is missing the "user/uuid" claim' in str(e)


def test_auth_wrong_role():
    with pytest.raises(AuthError) as e:
        auth_token = encode_jwt_payload(
            test_data.authorization_payload_wrong_accreditation,
            JWT_PRIVATE_KEY,
        )
        user_info_token = encode_jwt_payload(
            test_data.valid_user_info_payload, JWT_PRIVATE_KEY
        )
        auth.authorize_user(auth_token, user_info_token)
    assert "Can't start job with role: role/researcher" in str(e)


def test_auth_expired_token():
    with pytest.raises(AuthError) as e:
        auth_token = encode_jwt_payload(
            test_data.authorization_payload_expired, JWT_PRIVATE_KEY
        )
        user_info_token = encode_jwt_payload(
            test_data.valid_user_info_payload, JWT_PRIVATE_KEY
        )
        auth.authorize_user(auth_token, user_info_token)
    assert "Signature has expired" in str(e)


def test_user_info_no_first_name():
    with pytest.raises(AuthError) as e:
        auth_token = encode_jwt_payload(
            test_data.valid_authorization_payload, JWT_PRIVATE_KEY
        )
        user_info_token = encode_jwt_payload(
            test_data.user_info_payload_no_first_name, JWT_PRIVATE_KEY
        )
        auth.authorize_user(auth_token, user_info_token)
    assert 'Token is missing the "user/firstName" claim' in str(e)


def test_auth_no_last_name():
    with pytest.raises(AuthError) as e:
        auth_token = encode_jwt_payload(
            test_data.valid_authorization_payload, JWT_PRIVATE_KEY
        )
        user_info_token = encode_jwt_payload(
            test_data.user_info_payload_no_last_name, JWT_PRIVATE_KEY
        )
        auth.authorize_user(auth_token, user_info_token)
    assert 'Token is missing the "user/lastName" claim' in str(e)


def test_auth_missing_auth_token():
    with pytest.raises(AuthError) as e:
        user_info_token = encode_jwt_payload(
            test_data.valid_user_info_payload, JWT_PRIVATE_KEY
        )
        auth.authorize_user(None, user_info_token)
    assert "Unauthorized. No authorization token was provided" in str(e)


def test_auth_missing_user_info_token():
    with pytest.raises(AuthError) as e:
        auth_token = encode_jwt_payload(
            test_data.valid_authorization_payload, JWT_PRIVATE_KEY
        )
        auth.authorize_user(auth_token, None)
    assert "Unauthorized. No user info token was provided" in str(e)
