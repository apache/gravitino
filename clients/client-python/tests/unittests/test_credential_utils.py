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
import base64
import os

# pylint: disable=protected-access,too-many-lines,too-many-locals

import unittest

from gravitino.api.credential.s3_token_credential import S3TokenCredential
from gravitino.utils.credential_utils import CredentialUtils


class TestCredentialUtils(unittest.TestCase):

  def test_s3_token_credential(self):
    s3_credential_info = {
      S3TokenCredential._GRAVITINO_S3_SESSION_ACCESS_KEY_ID: "access_key",
      S3TokenCredential._GRAVITINO_S3_SESSION_SECRET_ACCESS_KEY: "secret_key",
      S3TokenCredential._GRAVITINO_S3_TOKEN: "session_token"}
    s3_credential = S3TokenCredential(s3_credential_info, 1000)
    credential_info = s3_credential.credential_info()
    expire_time = s3_credential.expire_time_in_ms()

    check_credential = CredentialUtils.to_credential(
      s3_credential.S3_TOKEN_CREDENTIAL_TYPE, credential_info, expire_time)
    self.assertEqual("access_key", check_credential.access_key_id())
    self.assertEqual("secret_key", check_credential.secret_access_key())
    self.assertEqual("session_token", check_credential.session_token())
