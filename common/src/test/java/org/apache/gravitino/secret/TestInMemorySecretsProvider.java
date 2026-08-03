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
}
