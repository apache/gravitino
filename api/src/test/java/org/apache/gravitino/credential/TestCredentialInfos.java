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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestCredentialInfos {

  @Test
  void testSkipsExpiringAndNulls() {
    Credential staticCred = new JdbcCredential("user", "pass");
    Credential expiring =
        new S3TokenCredential("ak", "sk", "session", System.currentTimeMillis() + 60_000L);

    Map<String, String> merged =
        CredentialInfos.nonExpiringCredentialInfo(
            new Credential[] {null, expiring, staticCred, null});

    assertEquals("user", merged.get(JdbcCredential.GRAVITINO_JDBC_USER));
    assertEquals("pass", merged.get(JdbcCredential.GRAVITINO_JDBC_PASSWORD));
    assertFalse(merged.containsKey(S3TokenCredential.GRAVITINO_S3_TOKEN));
    assertFalse(merged.containsKey(S3TokenCredential.GRAVITINO_S3_SESSION_ACCESS_KEY_ID));
  }

  @Test
  void testNullOrEmpty() {
    assertTrue(CredentialInfos.nonExpiringCredentialInfo(null).isEmpty());
    assertTrue(CredentialInfos.nonExpiringCredentialInfo(new Credential[0]).isEmpty());
  }

  @Test
  void testLaterStaticOverwritesEarlier() {
    Credential first = new JdbcCredential("u1", "p1");
    Credential second = new JdbcCredential("u2", "p2");
    Map<String, String> merged =
        CredentialInfos.nonExpiringCredentialInfo(new Credential[] {first, second});
    assertEquals("u2", merged.get(JdbcCredential.GRAVITINO_JDBC_USER));
    assertEquals("p2", merged.get(JdbcCredential.GRAVITINO_JDBC_PASSWORD));
  }
}
