/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.s3.credential;

import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCredentialProvider {

  @Test
  void testCredentialProviderType() {
    S3TokenProvider s3TokenProvider = new S3TokenProvider();
    Assertions.assertEquals(
        S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE, s3TokenProvider.credentialType());

    S3SecretKeyProvider s3SecretKeyProvider = new S3SecretKeyProvider();
    Assertions.assertEquals(
        S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE, s3SecretKeyProvider.credentialType());
  }
}
