from datetime import datetime, timedelta


valid_authorization_payload = {
    "aud": ["no.ssb.fdb", "datastore"],
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "accreditation/role": "role/dataadministrator",
    "sub": "testuser",
    "user/uuid": "1234-1234-1234-1234",
}

authorization_payload_no_user_id = {
    "aud": ["no.ssb.fdb", "datastore"],
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "accreditation/role": "role/dataadministrator",
    "sub": "testuser",
}

authorization_payload_wrong_accreditation = {
    "aud": ["no.ssb.fdb", "datastore"],
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "accreditation/role": "role/researcher",
    "sub": "testuser",
    "user/uuid": "1234-1234-1234-1234",
}

authorization_payload_expired = {
    "aud": ["no.ssb.fdb", "datastore"],
    "exp": (datetime.now() - timedelta(hours=1)).timestamp(),
    "accreditation/role": "role/dataadministrator",
    "sub": "testuser",
    "user/uuid": "1234-1234-1234-1234",
}

valid_user_info_payload = {
    "aud": ["rose"],
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "accreditation/role": "role/dataadministrator",
    "sub": "testuser",
    "user/uuid": "1234-1234-1234-1234",
    "user/firstName": "Test",
    "user/lastName": "Brukersen",
}

user_info_payload_no_first_name = {
    "aud": ["rose"],
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "accreditation/role": "role/dataadministrator",
    "sub": "testuser",
    "user/uuid": "1234-1234-1234-1234",
    "user/lastName": "Brukersen",
}


user_info_payload_no_last_name = {
    "aud": ["rose"],
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "accreditation/role": "role/dataadministrator",
    "sub": "testuser",
    "user/uuid": "1234-1234-1234-1234",
    "user/firstName": "Test",
}
