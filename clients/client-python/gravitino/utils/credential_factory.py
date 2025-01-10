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

from typing import Dict

from gravitino.api.credential.credential import Credential
from gravitino.api.credential.gcs_token_credential import GCSTokenCredential
from gravitino.api.credential.oss_token_credential import OSSTokenCredential
from gravitino.api.credential.s3_secret_key_credential import S3SecretKeyCredential
from gravitino.api.credential.s3_token_credential import S3TokenCredential
from gravitino.api.credential.oss_secret_key_credential import OSSSecretKeyCredential
from gravitino.api.credential.adls_token_credential import ADLSTokenCredential
from gravitino.api.credential.azure_account_key_credential import (
    AzureAccountKeyCredential,
)


class CredentialFactory:
    @staticmethod
    def create(
        credential_type: str, credential_info: Dict[str, str], expire_time_in_ms: int
    ) -> Credential:
        credential = None

        if credential_type == S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE:
            credential = S3TokenCredential(credential_info, expire_time_in_ms)
        elif credential_type == S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE:
            credential = S3SecretKeyCredential(credential_info, expire_time_in_ms)
        elif credential_type == GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE:
            credential = GCSTokenCredential(credential_info, expire_time_in_ms)
        elif credential_type == OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE:
            credential = OSSTokenCredential(credential_info, expire_time_in_ms)
        elif credential_type == OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE:
            credential = OSSSecretKeyCredential(credential_info, expire_time_in_ms)
        elif credential_type == ADLSTokenCredential.ADLS_TOKEN_CREDENTIAL_TYPE:
            credential = ADLSTokenCredential(credential_info, expire_time_in_ms)
        elif (
            credential_type
            == AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE
        ):
            credential = AzureAccountKeyCredential(credential_info, expire_time_in_ms)
        else:
            raise NotImplementedError(
                f"Credential type {credential_type} is not supported"
            )

        return credential
