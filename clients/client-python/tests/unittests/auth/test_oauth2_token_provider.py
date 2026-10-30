# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
import base64
import json
import sys
from threading import Barrier, Event, Lock, Thread
from time import monotonic
from unittest.mock import patch

from gravitino.auth.default_oauth2_token_provider import DefaultOAuth2TokenProvider
from gravitino.auth.oauth2_token_provider import OAuth2TokenProvider
from gravitino.exceptions.base import (
    BadRequestException,
    GravitinoRuntimeException,
    IllegalArgumentException,
    RESTException,
    UnauthorizedException,
)
from gravitino.utils.http_client import HTTPClient
from gravitino.dto.responses.oauth2_token_response import OAuth2TokenResponse
from tests.unittests.auth import mock_base
from tests.unittests.auth.mock_base import mock_token_response as token_response

OAUTH_PORT = 1082


class _StructuredRESTException(RESTException):
    def __init__(self, status, method, endpoint, reason):
        super().__init__("%s %s %s %s", status, method, endpoint, reason)


class _TestOAuth2TokenProvider(OAuth2TokenProvider):
    def _get_access_token(self):
        return None

    def validate(self):
        pass


class TestOAuth2TokenProvider(unittest.TestCase):
    @patch("gravitino.utils.http_client.build_opener")
    def test_close_delegates_without_request(self, build_opener):
        provider = _TestOAuth2TokenProvider("http://localhost")

        with patch.object(
            HTTPClient, "close", autospec=True, side_effect=HTTPClient.close
        ) as client_close:
            provider.close()
            provider.close()

        self.assertEqual(2, client_close.call_count)
        build_opener.assert_not_called()

    def test_provider_init_exception(self):
        with self.assertRaises(IllegalArgumentException):
            _ = DefaultOAuth2TokenProvider(uri="test")

        with self.assertRaises(IllegalArgumentException):
            _ = DefaultOAuth2TokenProvider(uri="test", credential="xx")

        with self.assertRaises(IllegalArgumentException):
            _ = DefaultOAuth2TokenProvider(uri="test", credential="xx", scope="test")

    @patch(
        "gravitino.utils.http_client.HTTPClient._make_request",
        return_value=mock_base.mock_authentication_invalid_client_error(),
    )
    def test_authentication_invalid_client_error(self, *mock_methods):
        with self.assertRaises(UnauthorizedException):
            _ = DefaultOAuth2TokenProvider(
                uri=f"http://127.0.0.1:{OAUTH_PORT}",
                credential="yy:xx",
                path="oauth/token",
                scope="test",
            )

    @patch(
        "gravitino.utils.http_client.HTTPClient._make_request",
        return_value=mock_base.mock_authentication_invalid_grant_error(),
    )
    def test_authentication_invalid_grant_error(self, *mock_methods):
        with self.assertRaises(BadRequestException):
            _ = DefaultOAuth2TokenProvider(
                uri=f"http://127.0.0.1:{OAUTH_PORT}",
                credential="yy:xx",
                path="oauth/token",
                scope="test",
            )

    @patch(
        "gravitino.utils.http_client.HTTPClient.post_form",
        return_value=mock_base.mock_authentication_with_error_authentication_type(),
    )
    def test_authentication_with_error_authentication_type(self, *mock_methods):
        with self.assertRaises(IllegalArgumentException):
            _ = DefaultOAuth2TokenProvider(
                uri=f"http://127.0.0.1:{OAUTH_PORT}",
                credential="yy:xx",
                path="oauth/token",
                scope="test",
            )

    @patch(
        "gravitino.utils.http_client.HTTPClient.post_form",
        return_value=mock_base.mock_authentication_with_non_jwt(),
    )
    def test_authentication_with_non_jwt(self, *mock_methods):
        token_provider = DefaultOAuth2TokenProvider(
            uri=f"http://127.0.0.1:{OAUTH_PORT}",
            credential="yy:xx",
            path="oauth/token",
            scope="test",
        )

        self.assertTrue(token_provider.has_token_data())
        self.assertEqual(b"Bearer 1", token_provider.get_token_data())


class TestOAuth2TokenExpiry(unittest.TestCase):
    # pylint: disable=protected-access
    def setUp(self):
        self.monotonic, self.wall = 1000.0, 2000.0
        clock_patch = patch("gravitino.auth.default_oauth2_token_provider.time")
        clock = clock_patch.start()
        self.addCleanup(clock_patch.stop)
        clock.monotonic.side_effect = lambda: self.monotonic
        clock.time.side_effect = lambda: self.wall
        fetch_patch = patch.object(HTTPClient, "post_form")
        self.fetch = fetch_patch.start()
        self.addCleanup(fetch_patch.stop)

    def provider(self, *responses):
        self.monotonic, self.wall = 1000.0, 2000.0
        self.fetch.reset_mock()
        if responses:
            self.fetch.side_effect = responses
        return DefaultOAuth2TokenProvider(
            "http://localhost", "client:secret", "test", "token"
        )

    def advance(self, seconds):
        self.monotonic += seconds
        self.wall += seconds

    @staticmethod
    def jwt(expires, **claims):
        payload = json.dumps({"exp": expires, **claims}, ensure_ascii=False).encode()
        return (
            "e30."
            + base64.urlsafe_b64encode(payload).decode().rstrip("=")
            + ".signature"
        )

    @staticmethod
    def deep_jwt():
        depth = sys.getrecursionlimit() + 10
        payload = b'{"nested":' + b"[" * depth + b"0" + b"]" * depth + b"}"
        return "e30." + base64.urlsafe_b64encode(payload).decode() + ".signature"

    def assert_lifetime(self, token, metadata, lifetime, margin):
        provider = self.provider(
            token_response(token, **metadata), token_response("renewed", expires_in=60)
        )
        self.advance(lifetime - margin - 0.01)
        self.assertEqual(b"Bearer " + token.encode(), provider.get_token_data())
        self.assertEqual(1, self.fetch.call_count)
        self.monotonic, self.wall = 1000 + lifetime - margin, 2000 + lifetime - margin
        self.assertEqual(b"Bearer renewed", provider.get_token_data())
        self.assertEqual(2, self.fetch.call_count)

    def test_expiry_sources_and_renewal_boundaries(self):
        cases = [
            ("opaque", {"expires_in": 3600}, 3600, 30),
            (self.jwt(2060), {"expires_in": 120}, 60, 6),
            (self.jwt(2120), {"expires_in": 60}, 60, 6),
            (self.jwt(2060), {}, 60, 6),
            (self.jwt(2060), {"expires_in": None}, 60, 6),
            ("opaque", {"expires_in": 1}, 1, 0.1),
            (self.jwt(2000.5), {}, 0.5, 0.05),
            (self.jwt(2060, extra="\uffff\uffff"), {}, 60, 6),
        ]
        for index, case in enumerate(cases):
            with self.subTest(case=index):
                self.assert_lifetime(*case)

    def test_invalid_metadata_falls_back_to_valid_source(self):
        cases = [
            (self.jwt(2120), {"expires_in": value}, 120, 12)
            for value in [False, "60", 1.5, {}, 10**1000]
        ]
        malformed = ["a.%%%.c", "a._w.c", "a.eA.c", "a.W10.c", "e30.e30.signature"]
        malformed.append(self.deep_jwt())
        malformed += [
            self.jwt(value)
            for value in [True, "2060", float("nan"), float("inf"), 10**1000]
        ]
        cases += [(token, {"expires_in": 60}, 60, 6) for token in malformed]
        for index, case in enumerate(cases):
            with self.subTest(case=index):
                self.assert_lifetime(*case)

    def test_unusable_or_expired_metadata_is_rejected(self):
        cases = [
            ("opaque", {}),
            (self.deep_jwt(), {}),
            ("opaque", {"expires_in": "60"}),
            ("opaque", {"expires_in": True}),
            (self.jwt(2100), {"expires_in": 0}),
            (self.jwt(2100), {"expires_in": -1}),
            (self.jwt(2000), {"expires_in": 60}),
            (self.jwt(1999), {"expires_in": 60}),
        ]
        for index, (token, metadata) in enumerate(cases):
            with self.subTest(case=index):
                with self.assertRaises(GravitinoRuntimeException):
                    self.provider(token_response(token, **metadata))

    def test_response_validation_is_safe_and_preserves_exception_types(self):
        invalid = {
            "access_token": [None, "", " ", 123],
            "token_type": [None, False, "client:secret"],
        }
        cases = [
            (
                token_response("old", expires_in=60, **{field: value}),
                IllegalArgumentException,
            )
            for field, values in invalid.items()
            for value in values
        ]
        cases += [
            (mock_base.TestResponse(body, 200), GravitinoRuntimeException)
            for body in [b"client:secret", b"[]", b"null"]
        ]
        cases.append(
            (_StructuredRESTException(502, "POST", "auth", "secret"), RESTException)
        )
        for index, (response, error_type) in enumerate(cases):
            with self.subTest(case=index):
                with self.assertRaises(error_type) as caught:
                    self.provider(response)
                self.assertIs(type(caught.exception), error_type)
                self.assertIsNone(caught.exception.__context__)
                self.assertIsNone(caught.exception.__cause__)
                self.assertNotIn("secret", str(caught.exception))

    def test_renewal_failure_preserves_cache_and_recovers(self):
        cases = [
            (UnauthorizedException("client:secret"), UnauthorizedException, 54),
            (UnauthorizedException("client:secret"), UnauthorizedException, 60),
            (BadRequestException("client:secret"), BadRequestException, 54),
            (
                _StructuredRESTException(502, "POST", "auth", "secret"),
                RESTException,
                54,
            ),
            (
                token_response("old", token_type="client:secret"),
                IllegalArgumentException,
                54,
            ),
            (OSError("client:secret"), GravitinoRuntimeException, 54),
            (token_response("invalid"), GravitinoRuntimeException, 60),
            (
                token_response(self.jwt(1999), expires_in=60),
                GravitinoRuntimeException,
                54,
            ),
        ]
        for index, (failure, error_type, elapsed) in enumerate(cases):
            with self.subTest(case=index):
                provider = self.provider(
                    token_response("old", expires_in=60),
                    failure,
                    token_response("recovered", expires_in=60),
                )
                old = provider._token
                self.advance(elapsed)
                with self.assertRaises(error_type) as caught:
                    provider.get_token_data()
                self.assertIs(type(caught.exception), error_type)
                self.assertIs(old, provider._token)
                self.assertIsNone(caught.exception.__context__)
                self.assertIsNone(caught.exception.__cause__)
                self.assertNotIn("secret", str(caught.exception))
                self.assertEqual(b"Bearer recovered", provider.get_token_data())
                self.assertEqual(3, self.fetch.call_count)

    def test_token_values_are_hidden_in_repr(self):
        response = token_response("secret", expires_in=60, refresh_token="secret")
        provider = self.provider(response)
        decoded = OAuth2TokenResponse.from_json(response.body, infer_missing=True)
        for state in [provider._token, decoded]:
            self.assertNotIn("secret", repr(state))

    def test_clock_changes(self):
        cases = [
            (self.jwt(2060), 60, 60, -440, True),
            (self.jwt(2060), 120, 0, 54, True),
            ("opaque", 60, 0, 10000, False),
        ]
        for token, lifetime, monotonic_delta, wall_delta, renew in cases:
            with self.subTest(monotonic=monotonic_delta, wall=wall_delta):
                provider = self.provider(
                    token_response(token, expires_in=lifetime),
                    token_response("renewed", expires_in=60),
                )
                self.monotonic += monotonic_delta
                self.wall += wall_delta
                expected = b"Bearer renewed" if renew else b"Bearer " + token.encode()
                self.assertEqual(expected, provider.get_token_data())
                self.assertEqual(1 + renew, self.fetch.call_count)

    def test_slow_issuance_does_not_consume_new_token_lifetime(self):
        def issue(*_args, **_kwargs):
            self.advance(2.05)
            return token_response(self.jwt(2003), expires_in=1)

        self.fetch.side_effect = issue
        provider = self.provider()
        self.advance(0.8)
        self.assertEqual(
            b"Bearer " + self.jwt(2003).encode(), provider.get_token_data()
        )
        self.assertEqual(1, self.fetch.call_count)

    def test_expiry_during_parsing_is_rejected(self):
        decode = OAuth2TokenResponse.from_dict

        def delayed_decode(*args, **kwargs):
            self.advance(2)
            return decode(*args, **kwargs)

        with patch.object(OAuth2TokenResponse, "from_dict", side_effect=delayed_decode):
            with self.assertRaises(GravitinoRuntimeException):
                self.provider(token_response("opaque", expires_in=1))

    def test_concurrent_renewal_success(self):
        self._assert_concurrent_renewal(False)

    def test_concurrent_renewal_failure(self):
        self._assert_concurrent_renewal(True)

    def _assert_concurrent_renewal(self, fails):
        provider = self.provider(token_response("old", expires_in=60))
        self.advance(54)
        start, entered, release = Barrier(5), Event(), Event()
        active = Lock()
        results = [None] * 4

        def renew(*_args, **_kwargs):
            # pylint: disable-next=consider-using-with  # Detect overlap without blocking.
            self.assertTrue(active.acquire(blocking=False), "Overlapping requests")
            try:
                entered.set()
                self.assertTrue(release.wait(5))
                if fails:
                    raise UnauthorizedException("secret")
                return token_response("renewed", expires_in=60)
            finally:
                active.release()

        def access(index):
            try:
                start.wait(5)
                results[index] = provider.get_token_data()
            except Exception as error:  # pylint: disable=broad-exception-caught
                results[index] = type(error)

        self.fetch.side_effect = renew
        workers = []
        barrier_passed = False
        try:
            for index in range(start.parties - 1):
                worker = Thread(
                    target=access,
                    args=(index,),
                    name=f"oauth2-renewal-worker-{index}",
                    daemon=True,
                )
                worker.start()
                workers.append(worker)
            start.wait(5)
            barrier_passed = True
            self.assertTrue(entered.wait(5))
        finally:
            release.set()
            if not barrier_passed:
                start.abort()
            deadline = monotonic() + 5
            for worker in workers:
                worker.join(timeout=max(0, deadline - monotonic()))
            self.assertFalse(
                [worker.name for worker in workers if worker.is_alive()],
                "OAuth2 worker threads did not finish",
            )
        self.assertEqual(
            [UnauthorizedException if fails else b"Bearer renewed"] * 4, results
        )
        self.assertEqual(5 if fails else 2, self.fetch.call_count)
        if fails:
            self.fetch.side_effect = [token_response("recovered", expires_in=60)]
            self.assertEqual(b"Bearer recovered", provider.get_token_data())
