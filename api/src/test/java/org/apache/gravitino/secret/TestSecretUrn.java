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

import static org.apache.gravitino.secret.SecretConstants.ATTR_ENTITY_ID;
import static org.apache.gravitino.secret.SecretConstants.ATTR_ENTITY_TYPE;
import static org.apache.gravitino.secret.SecretConstants.ATTR_PROPERTY_KEY;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretUrn {

  private static Map<String, String> attributes(String entityType, String entityId, String key) {
    return Map.of(
        ATTR_ENTITY_TYPE, entityType,
        ATTR_ENTITY_ID, entityId,
        ATTR_PROPERTY_KEY, key);
  }

  @Test
  public void testBuildAndParseWriteThroughUrn() {
    SecretUrn urn =
        SecretUrn.buildWriteThrough("memory", attributes("catalog", "42", "jdbc-password"));
    Assertions.assertEquals("urn:gravitino-secret:memory:catalog:42:jdbc-password", urn.toString());
    Assertions.assertEquals("memory", urn.providerName());
    Assertions.assertEquals(List.of("catalog", "42", "jdbc-password"), urn.identifierSegments());
    Assertions.assertEquals("jdbc-password", urn.propertyKey());

    SecretUrn parsed = SecretUrn.parse(urn.toString());
    Assertions.assertEquals(urn, parsed);
  }

  @Test
  public void testDottedPropertyKeyInUrn() {
    SecretUrn urn =
        SecretUrn.buildWriteThrough("local", attributes("catalog", "1", "authentication.password"));
    Assertions.assertEquals(
        "urn:gravitino-secret:local:catalog:1:authentication.password", urn.toString());
    Assertions.assertEquals("local", urn.providerName());
    Assertions.assertEquals(
        List.of("catalog", "1", "authentication.password"), urn.identifierSegments());
    Assertions.assertEquals("authentication.password", urn.propertyKey());
  }

  @Test
  public void testInvalidUrn() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> SecretUrn.parse("not-a-urn"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SecretUrn.buildWriteThrough("memory", attributes("catalog", "1", "bad key")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SecretUrn.buildWriteThrough("memory", attributes("catalog", "abc", "password")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SecretUrn.buildWriteThrough("memory", null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SecretUrn.validateSegment("bad/key"));
  }
}
