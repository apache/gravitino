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
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcCredentialProvider {

  @Test
  void testJdbcCredentialProvider() {
    String jdbcUser = "test-user";
    String jdbcPassword = "test-password";
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            JdbcCredential.GRAVITINO_JDBC_USER,
            jdbcUser,
            JdbcCredential.GRAVITINO_JDBC_PASSWORD,
            jdbcPassword);

    CredentialProvider credentialProvider =
        CredentialProviderFactory.create(JdbcCredential.JDBC_CREDENTIAL_TYPE, catalogProperties);

    Assertions.assertEquals(
        JdbcCredential.JDBC_CREDENTIAL_TYPE, credentialProvider.credentialType());
    Assertions.assertInstanceOf(JdbcCredentialProvider.class, credentialProvider);

    CatalogCredentialContext context = new CatalogCredentialContext("test-user");
    Credential credential = credentialProvider.getCredential(context);

    Assertions.assertNotNull(credential);
    Assertions.assertInstanceOf(JdbcCredential.class, credential);
    JdbcCredential jdbcCredential = (JdbcCredential) credential;

    Assertions.assertEquals(jdbcUser, jdbcCredential.jdbcUser());
    Assertions.assertEquals(jdbcPassword, jdbcCredential.jdbcPassword());
    Assertions.assertEquals(0, jdbcCredential.expireTimeInMs());
  }

  @Test
  void testJdbcCredentialProviderWithMissingProperties() {
    Map<String, String> catalogProperties = ImmutableMap.of();

    CredentialProvider credentialProvider =
        CredentialProviderFactory.create(JdbcCredential.JDBC_CREDENTIAL_TYPE, catalogProperties);

    CatalogCredentialContext context = new CatalogCredentialContext("test-user");
    Credential credential = credentialProvider.getCredential(context);

    Assertions.assertNull(credential);
  }

  @Test
  void testJdbcCredentialInfo() {
    String jdbcUser = "test-user";
    String jdbcPassword = "test-password";
    JdbcCredential jdbcCredential = new JdbcCredential(jdbcUser, jdbcPassword);

    Map<String, String> credentialInfo = jdbcCredential.credentialInfo();
    Assertions.assertEquals(2, credentialInfo.size());
    Assertions.assertEquals(jdbcUser, credentialInfo.get(JdbcCredential.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(
        jdbcPassword, credentialInfo.get(JdbcCredential.GRAVITINO_JDBC_PASSWORD));
  }

  @Test
  void testJdbcCredentialInitialize() {
    String jdbcUser = "test-user";
    String jdbcPassword = "test-password";
    Map<String, String> credentialInfo =
        ImmutableMap.of(
            JdbcCredential.GRAVITINO_JDBC_USER,
            jdbcUser,
            JdbcCredential.GRAVITINO_JDBC_PASSWORD,
            jdbcPassword);

    JdbcCredential jdbcCredential = new JdbcCredential();
    jdbcCredential.initialize(credentialInfo, 0);

    Assertions.assertEquals(jdbcUser, jdbcCredential.jdbcUser());
    Assertions.assertEquals(jdbcPassword, jdbcCredential.jdbcPassword());
  }
}
