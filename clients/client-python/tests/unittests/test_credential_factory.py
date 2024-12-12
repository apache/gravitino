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

# pylint: disable=protected-access,too-many-lines,too-many-locals

import unittest

from gravitino.api.credential.gcs_token_credential import GCSTokenCredential
from gravitino.api.credential.oss_token_credential import OSSTokenCredential
from gravitino.api.credential.s3_secret_key_credential import S3SecretKeyCredential
from gravitino.api.credential.s3_token_credential import S3TokenCredential
from gravitino.utils.credential_factory import CredentialFactory


class TestCredentialFactory(unittest.TestCase):

    def test_s3_token_credential(self):
        s3_credential_info = {
            S3TokenCredential._GRAVITINO_S3_SESSION_ACCESS_KEY_ID: "access_key",
            S3TokenCredential._GRAVITINO_S3_SESSION_SECRET_ACCESS_KEY: "secret_key",
            S3TokenCredential._GRAVITINO_S3_TOKEN: "session_token",
        }
        s3_credential = S3TokenCredential(s3_credential_info, 1000)
        credential_info = s3_credential.credential_info()
        expire_time = s3_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            s3_credential.S3_TOKEN_CREDENTIAL_TYPE, credential_info, expire_time
        )
        self.assertEqual("access_key", check_credential.access_key_id())
        self.assertEqual("secret_key", check_credential.secret_access_key())
        self.assertEqual("session_token", check_credential.session_token())
        self.assertEqual(1000, check_credential.expire_time_in_ms())

    def test_s3_secret_key_credential(self):
        s3_credential_info = {
            S3SecretKeyCredential._GRAVITINO_S3_STATIC_ACCESS_KEY_ID: "access_key",
            S3SecretKeyCredential._GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY: "secret_key",
        }
        s3_credential = S3SecretKeyCredential(s3_credential_info, 0)
        credential_info = s3_credential.credential_info()
        expire_time = s3_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            s3_credential.S3_SECRET_KEY_CREDENTIAL_TYPE, credential_info, expire_time
        )
        self.assertEqual("access_key", check_credential.access_key_id())
        self.assertEqual("secret_key", check_credential.secret_access_key())
        self.assertEqual(0, check_credential.expire_time_in_ms())

    def test_gcs_token_credential(self):
        credential_info = {GCSTokenCredential._GCS_TOKEN_NAME: "token"}
        credential = GCSTokenCredential(credential_info, 1000)
        credential_info = credential.credential_info()
        expire_time = credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            credential.credential_type(), credential_info, expire_time
        )
        self.assertEqual(
            GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )
        self.assertEqual("token", check_credential.token())
        self.assertEqual(1000, check_credential.expire_time_in_ms())

    def test_oss_token_credential(self):
        credential_info = {
            OSSTokenCredential._GRAVITINO_OSS_TOKEN: "token",
            OSSTokenCredential._GRAVITINO_OSS_SESSION_ACCESS_KEY_ID: "access_id",
            OSSTokenCredential._GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY: "secret_key",
        }
        credential = OSSTokenCredential(credential_info, 1000)
        credential_info = credential.credential_info()
        expire_time = credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            credential.credential_type(), credential_info, expire_time
        )
        self.assertEqual(
            OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )
        self.assertEqual("token", check_credential.security_token())
        self.assertEqual("access_id", check_credential.access_key_id())
        self.assertEqual("secret_key", check_credential.secret_access_key())
        self.assertEqual(1000, check_credential.expire_time_in_ms())
