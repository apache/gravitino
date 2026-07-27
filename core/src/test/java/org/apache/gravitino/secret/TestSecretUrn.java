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

import static org.apache.gravitino.secret.SecretConstants.URN_PREFIX;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretUrn {

  @Test
  public void testBuildAndParseWriteThroughUrn() {
    String urn = SecretUrn.buildWriteThrough("memory", "catalog", 42L, "jdbc-password");
    Assertions.assertEquals("urn:gravitino-secret:memory:catalog:42:jdbc-password", urn);

    SecretUrn.ParsedUrn parsed = SecretUrn.parse(urn);
    Assertions.assertEquals("memory", parsed.providerName());
    Assertions.assertEquals("catalog:42:jdbc-password", parsed.identifier());
    Assertions.assertEquals(
        java.util.List.of("catalog", "42", "jdbc-password"), parsed.identifierSegments());
  }

  @Test
  public void testIsWriteThroughForEntity() {
    String urn = SecretUrn.buildWriteThrough("memory", "schema", 7L, "token");
    Assertions.assertTrue(SecretUrn.isWriteThroughForEntity(urn, "schema", 7L));
    Assertions.assertFalse(SecretUrn.isWriteThroughForEntity(urn, "catalog", 7L));
    Assertions.assertFalse(SecretUrn.isWriteThroughForEntity(urn, "schema", 8L));
    Assertions.assertFalse(
        SecretUrn.isWriteThroughForEntity(URN_PREFIX + "memory:external:ref", "schema", 7L));
  }

  @Test
  public void testDottedPropertyKeyInUrn() {
    String urn = SecretUrn.buildWriteThrough("local", "catalog", 1L, "authentication.password");
    Assertions.assertEquals("urn:gravitino-secret:local:catalog:1:authentication.password", urn);
    Assertions.assertTrue(SecretUrn.isWriteThroughForEntity(urn, "catalog", 1L));
  }

  @Test
  public void testInvalidUrn() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> SecretUrn.parse("not-a-urn"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SecretUrn.buildWriteThrough("memory", "catalog", 1L, "bad key"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SecretUrn.validateSegment("bad/key"));
  }
}
