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
package org.apache.gravitino.credential;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCredentialPropertyKeys {

  @Test
  void testKnownCredentialKeys() {
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("s3-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("s3-secret-access-key"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("s3-session-token"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("oss-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("oss-secret-access-key"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("cos-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("jdbc-password"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("jdbc-user"));
    Assertions.assertTrue(
        CredentialPropertyKeys.isCredentialPropertyKey("azure-storage-account-key"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("aws-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("aws-secret-access-key"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("dlf-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("dlf-access-key-secret"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("dlf-security-token"));
  }

  @Test
  void testNonCredentialKeys() {
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey(null));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("custom-token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("credential-providers"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("access-key-id"));
  }
}
