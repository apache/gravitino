/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.secret;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.storage.CloudStorageCredentialPropertyKeys;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;

public class TestStaticCredentialPropertyKeys {

  @Test
  void testKeysForCredentialType() {
    assertEquals(
        Set.of(
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY),
        StaticCredentialPropertyKeys.keysForCredentialType(
            S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE));
    assertEquals(
        Set.of(
            CloudStorageCredentialPropertyKeys.JDBC_USER,
            CloudStorageCredentialPropertyKeys.JDBC_PASSWORD),
        StaticCredentialPropertyKeys.keysForCredentialType(JdbcCredential.JDBC_CREDENTIAL_TYPE));
    assertTrue(
        StaticCredentialPropertyKeys.keysForCredentialType(
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)
            .isEmpty());
    assertTrue(
        StaticCredentialPropertyKeys.keysForCredentialProviders(
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE
                    + ","
                    + JdbcCredential.JDBC_CREDENTIAL_TYPE)
            .contains(CloudStorageCredentialPropertyKeys.JDBC_PASSWORD));
    assertFalse(
        StaticCredentialPropertyKeys.keysForCredentialProviders(
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)
            .contains(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
  }
}
