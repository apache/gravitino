"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import time
import json
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from http import HTTPStatus
import jwt

from gravitino.utils.http_client import Response

JWT_FAKE_SECRET = "fake_secret"
GENERATED_TIME = int(time.time())


@dataclass
class TestResponse:
    body: bytes
    status_code: int


@dataclass_json
@dataclass
class TestJWT:
    sub: str
    exp: int
    aud: str


def mock_authentication_with_error_authentication_type():
    return TestResponse(
        body=json.dumps(
            {
                "code": 0,
                "access_token": "1",
                "issued_token_type": "2",
                "token_type": "3",
                "expires_in": 1,
                "scope": "test",
                "refresh_token": None,
            }
        ).encode("utf-8"),
        status_code=HTTPStatus.OK.value,
    )


def mock_authentication_with_non_jwt():
    return TestResponse(
        body=json.dumps(
            {
                "code": 0,
                "access_token": "1",
                "issued_token_type": "2",
                "token_type": "bearer",
                "expires_in": 1,
                "scope": "test",
                "refresh_token": None,
            }
        ),
        status_code=HTTPStatus.OK.value,
    )


def mock_jwt(sub, exp, aud):
    return jwt.encode(
        TestJWT(sub, exp, aud).to_dict(),
        JWT_FAKE_SECRET,
        algorithm="HS256",
    )


def mock_old_new_jwt():
    return [
        mock_jwt(sub="gravitino", exp=GENERATED_TIME - 10000, aud="service1"),
        mock_jwt(sub="gravitino", exp=GENERATED_TIME + 10000, aud="service1"),
    ]


def mock_authentication_with_jwt():
    old_access_token, new_access_token = mock_old_new_jwt()
    return [
        TestResponse(
            body=json.dumps(
                {
                    "code": 0,
                    "access_token": old_access_token,
                    "issued_token_type": "2",
                    "token_type": "bearer",
                    "expires_in": 1,
                    "scope": "test",
                    "refresh_token": None,
                }
            ),
            status_code=HTTPStatus.OK.value,
        ),
        TestResponse(
            body=json.dumps(
                {
                    "code": 0,
                    "access_token": new_access_token,
                    "issued_token_type": "2",
                    "token_type": "bearer",
                    "expires_in": 1,
                    "scope": "test",
                    "refresh_token": None,
                }
            ),
            status_code=HTTPStatus.OK.value,
        ),
    ]
