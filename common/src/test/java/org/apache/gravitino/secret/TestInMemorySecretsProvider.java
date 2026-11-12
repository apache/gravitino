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

import java.util.Map;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestInMemorySecretsProvider {

  private static Map<String, String> writeAttributes() {
    return Map.of(
        ATTR_ENTITY_TYPE, "catalog",
        ATTR_ENTITY_ID, "10",
        ATTR_PROPERTY_KEY, "password");
  }

  @Test
  public void testWriteReadDelete() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    Assertions.assertEquals("memory", provider.type());

    provider.initialize("memory", Map.of());
    SecretUrn urn = provider.writeSecret("s3cr3t", writeAttributes());
    Assertions.assertEquals("urn:gravitino-secret:memory:catalog:10:password", urn.toString());
    Assertions.assertEquals("s3cr3t", provider.readSecret(urn));

    provider.deleteSecret(urn);
    Assertions.assertThrows(IllegalArgumentException.class, () -> provider.readSecret(urn));
    provider.close();
  }

  @Test
  public void testWriteSecretRejectsNullArguments() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.writeSecret(null, writeAttributes()));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.writeSecret("s3cr3t", null));
  }

  @Test
  public void testWriteSecretRejectsMissingAttributes() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.writeSecret("s3cr3t", Map.of()));
  }

  @Test
  public void testReadDeleteRejectNullUrn() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    Assertions.assertThrows(IllegalArgumentException.class, () -> provider.readSecret(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> provider.deleteSecret(null));
  }

  @Test
  public void testCloseClearsStoredSecrets() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretUrn urn = provider.writeSecret("s3cr3t", writeAttributes());
    Assertions.assertEquals("s3cr3t", provider.readSecret(urn));

    provider.close();
    Assertions.assertThrows(IllegalArgumentException.class, () -> provider.readSecret(urn));
  }

  @Test
  public void testWrittenVersionStaysPendingUntilPromoted() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretUrn urn = provider.writeSecret("old", writeAttributes());
    String oldVersion = provider.currentSecretVersion(urn);
    Assertions.assertNotNull(oldVersion);

    String newVersion = provider.writeSecretVersion(urn, "new");
    Assertions.assertNotEquals(oldVersion, newVersion);
    Assertions.assertEquals(oldVersion, provider.currentSecretVersion(urn));
    Assertions.assertEquals("old", provider.readSecret(urn));

    provider.promoteSecretVersion(urn, newVersion);
    Assertions.assertEquals(newVersion, provider.currentSecretVersion(urn));
    Assertions.assertEquals("new", provider.readSecret(urn));

    provider.deleteSecretVersion(urn, oldVersion);
    Assertions.assertEquals("new", provider.readSecret(urn));
  }

  @Test
  public void testWrittenVersionBecomesCurrentWhenUrnHasNone() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretUrn urn = SecretUrn.buildWriteThrough("memory", writeAttributes());
    Assertions.assertNull(provider.currentSecretVersion(urn));

    String version = provider.writeSecretVersion(urn, "first");
    Assertions.assertEquals(version, provider.currentSecretVersion(urn));
    Assertions.assertEquals("first", provider.readSecret(urn));
  }

  @Test
  public void testDeletingCurrentVersionLeavesNoReadableValue() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretUrn urn = provider.writeSecret("old", writeAttributes());
    String oldVersion = provider.currentSecretVersion(urn);
    String newVersion = provider.writeSecretVersion(urn, "new");

    provider.deleteSecretVersion(urn, oldVersion);
    Assertions.assertNull(provider.currentSecretVersion(urn));
    Assertions.assertThrows(IllegalArgumentException.class, () -> provider.readSecret(urn));

    provider.promoteSecretVersion(urn, newVersion);
    Assertions.assertEquals("new", provider.readSecret(urn));

    provider.deleteSecretVersion(urn, newVersion);
    Assertions.assertNull(provider.currentSecretVersion(urn));
    Assertions.assertDoesNotThrow(() -> provider.deleteSecretVersion(urn, newVersion));
  }

  @Test
  public void testPromoteRejectsUnknownVersion() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretUrn urn = SecretUrn.buildWriteThrough("memory", writeAttributes());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.promoteSecretVersion(urn, "1"));

    String version = provider.writeSecretVersion(urn, "first");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.promoteSecretVersion(urn, version + "0"));
    Assertions.assertEquals("first", provider.readSecret(urn));
  }

  @Test
  public void testDeleteSecretRemovesEveryVersion() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretUrn urn = provider.writeSecret("old", writeAttributes());
    String newVersion = provider.writeSecretVersion(urn, "new");

    provider.deleteSecret(urn);
    Assertions.assertNull(provider.currentSecretVersion(urn));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.promoteSecretVersion(urn, newVersion));
  }

  @Test
  public void testWriteSecretReplacesEveryVersion() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretUrn urn = provider.writeSecret("old", writeAttributes());
    String pending = provider.writeSecretVersion(urn, "pending");

    provider.writeSecret("new", writeAttributes());
    Assertions.assertEquals("new", provider.readSecret(urn));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.promoteSecretVersion(urn, pending));
  }

  @Test
  public void testVersionMethodsRejectInvalidArguments() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretUrn urn = SecretUrn.buildWriteThrough("memory", writeAttributes());
    SecretUrn otherProvider = SecretUrn.buildWriteThrough("vault", writeAttributes());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.writeSecretVersion(null, "s3cr3t"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.writeSecretVersion(urn, null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.writeSecretVersion(otherProvider, "s3cr3t"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.promoteSecretVersion(urn, null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.deleteSecretVersion(null, "1"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.currentSecretVersion(null));
  }
}
