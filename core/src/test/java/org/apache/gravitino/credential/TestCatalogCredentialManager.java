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

package org.apache.gravitino.credential;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogCredentialManager {

  @Test
  void testSelectsStorageCredentialWhenJdbcProviderIsAlsoConfigured() {
    String s3Path = "s3://bucket/warehouse/table";
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            CredentialConstants.CREDENTIAL_PROVIDERS,
            String.join(
                ",", DummyCredentialProvider.CREDENTIAL_TYPE, JdbcCredential.JDBC_CREDENTIAL_TYPE),
            JdbcCredential.GRAVITINO_JDBC_USER,
            "test-user",
            JdbcCredential.GRAVITINO_JDBC_PASSWORD,
            "test-password");

    try (CatalogCredentialManager credentialManager =
        new CatalogCredentialManager("test-catalog", catalogProperties)) {
      PathBasedCredentialContext context =
          new PathBasedCredentialContext("test-user", ImmutableSet.of(), ImmutableSet.of(s3Path));

      Credential credential =
          credentialManager
              .getCredentialByPath(s3Path, context)
              .orElseThrow(() -> new AssertionError("Expected a storage credential"));

      Assertions.assertInstanceOf(DummyCredentialProvider.DummyCredential.class, credential);
      Assertions.assertTrue(
          credentialManager.getCredentialProvider(JdbcCredential.JDBC_CREDENTIAL_TYPE).isPresent());
    }
  }
}
