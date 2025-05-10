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
from gravitino.api.credential.oss_secret_key_credential import OSSSecretKeyCredential
from gravitino.api.credential.adls_token_credential import ADLSTokenCredential
from gravitino.api.credential.azure_account_key_credential import (
    AzureAccountKeyCredential,
)


class TestCredentialFactory(unittest.TestCase):
    def test_s3_token_credential(self):
        s3_credential_info = {
            S3TokenCredential._SESSION_ACCESS_KEY_ID: "access_key",
            S3TokenCredential._SESSION_SECRET_ACCESS_KEY: "secret_key",
            S3TokenCredential._SESSION_TOKEN: "session_token",
        }
        s3_credential = S3TokenCredential(s3_credential_info, 1000)
        credential_info = s3_credential.credential_info()
        expire_time = s3_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            s3_credential.S3_TOKEN_CREDENTIAL_TYPE, credential_info, expire_time
        )
        self.assertEqual(
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )

        self.assertIsInstance(check_credential, S3TokenCredential)
        self.assertEqual("access_key", check_credential.access_key_id())
        self.assertEqual("secret_key", check_credential.secret_access_key())
        self.assertEqual("session_token", check_credential.session_token())
        self.assertEqual(1000, check_credential.expire_time_in_ms())

    def test_s3_secret_key_credential(self):
        s3_credential_info = {
            S3SecretKeyCredential._STATIC_ACCESS_KEY_ID: "access_key",
            S3SecretKeyCredential._STATIC_SECRET_ACCESS_KEY: "secret_key",
        }
        s3_credential = S3SecretKeyCredential(s3_credential_info, 0)
        credential_info = s3_credential.credential_info()
        expire_time = s3_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            s3_credential.S3_SECRET_KEY_CREDENTIAL_TYPE, credential_info, expire_time
        )
        self.assertEqual(
            S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )

        self.assertIsInstance(check_credential, S3SecretKeyCredential)
        self.assertEqual("access_key", check_credential.access_key_id())
        self.assertEqual("secret_key", check_credential.secret_access_key())
        self.assertEqual(0, check_credential.expire_time_in_ms())

    def test_gcs_token_credential(self):
        gcs_credential_info = {GCSTokenCredential._GCS_TOKEN_NAME: "token"}
        gcs_credential = GCSTokenCredential(gcs_credential_info, 1000)
        credential_info = gcs_credential.credential_info()
        expire_time = gcs_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            gcs_credential.credential_type(), credential_info, expire_time
        )
        self.assertEqual(
            GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )

        self.assertIsInstance(check_credential, GCSTokenCredential)
        self.assertEqual("token", check_credential.token())
        self.assertEqual(1000, check_credential.expire_time_in_ms())

    def test_oss_token_credential(self):
        oss_credential_info = {
            OSSTokenCredential._STATIC_ACCESS_KEY_ID: "access_id",
            OSSTokenCredential._STATIC_SECRET_ACCESS_KEY: "secret_key",
            OSSTokenCredential._OSS_TOKEN: "token",
        }
        oss_credential = OSSTokenCredential(oss_credential_info, 1000)
        credential_info = oss_credential.credential_info()
        expire_time = oss_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            oss_credential.credential_type(), credential_info, expire_time
        )
        self.assertEqual(
            OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )

        self.assertIsInstance(check_credential, OSSTokenCredential)
        self.assertEqual("token", check_credential.security_token())
        self.assertEqual("access_id", check_credential.access_key_id())
        self.assertEqual("secret_key", check_credential.secret_access_key())
        self.assertEqual(1000, check_credential.expire_time_in_ms())

    def test_oss_secret_key_credential(self):
        oss_credential_info = {
            OSSSecretKeyCredential._STATIC_ACCESS_KEY_ID: "access_key",
            OSSSecretKeyCredential._STATIC_SECRET_ACCESS_KEY: "secret_key",
        }
        oss_credential = OSSSecretKeyCredential(oss_credential_info, 0)
        credential_info = oss_credential.credential_info()
        expire_time = oss_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            oss_credential.OSS_SECRET_KEY_CREDENTIAL_TYPE, credential_info, expire_time
        )
        self.assertEqual(
            OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )

        self.assertIsInstance(check_credential, OSSSecretKeyCredential)
        self.assertEqual("access_key", check_credential.access_key_id())
        self.assertEqual("secret_key", check_credential.secret_access_key())
        self.assertEqual(0, check_credential.expire_time_in_ms())

    def test_adls_token_credential(self):
        adls_credential_info = {
            ADLSTokenCredential._STORAGE_ACCOUNT_NAME: "account_name",
            ADLSTokenCredential._SAS_TOKEN: "sas_token",
        }
        adls_credential = ADLSTokenCredential(adls_credential_info, 1000)
        credential_info = adls_credential.credential_info()
        expire_time = adls_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            adls_credential.credential_type(), credential_info, expire_time
        )
        self.assertEqual(
            ADLSTokenCredential.ADLS_TOKEN_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )

        self.assertIsInstance(check_credential, ADLSTokenCredential)
        self.assertEqual("account_name", check_credential.account_name())
        self.assertEqual("sas_token", check_credential.sas_token())
        self.assertEqual(1000, check_credential.expire_time_in_ms())

    def test_azure_account_key_credential(self):
        azure_credential_info = {
            AzureAccountKeyCredential._STORAGE_ACCOUNT_NAME: "account_name",
            AzureAccountKeyCredential._STORAGE_ACCOUNT_KEY: "account_key",
        }
        azure_credential = AzureAccountKeyCredential(azure_credential_info, 0)
        credential_info = azure_credential.credential_info()
        expire_time = azure_credential.expire_time_in_ms()

        check_credential = CredentialFactory.create(
            azure_credential.credential_type(), credential_info, expire_time
        )
        self.assertEqual(
            AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE,
            check_credential.credential_type(),
        )

        self.assertIsInstance(check_credential, AzureAccountKeyCredential)
        self.assertEqual("account_name", check_credential.account_name())
        self.assertEqual("account_key", check_credential.account_key())
        self.assertEqual(0, check_credential.expire_time_in_ms())
