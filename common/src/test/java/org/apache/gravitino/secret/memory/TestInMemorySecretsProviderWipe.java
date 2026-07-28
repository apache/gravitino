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

package org.apache.gravitino.secret.memory;

import java.util.Map;
import org.apache.gravitino.secret.SecretWriteContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestInMemorySecretsProviderWipe {

  @Test
  public void testDeleteSecretZerosStoredBytes() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    SecretWriteContext context = new SecretWriteContext("memory", "catalog", 10L, "password");
    String urn = provider.writeSecret("s3cr3t", context);

    byte[] stored = provider.storedBytes(urn);
    Assertions.assertNotNull(stored);
    Assertions.assertTrue(stored.length > 0);

    provider.deleteSecret(urn);
    Assertions.assertNull(provider.storedBytes(urn));
    for (byte b : stored) {
      Assertions.assertEquals((byte) 0, b);
    }
  }

  @Test
  public void testCloseZerosAllStoredBytes() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    provider.initialize("memory", Map.of());
    String urn1 =
        provider.writeSecret("one", new SecretWriteContext("memory", "catalog", 1L, "password"));
    String urn2 =
        provider.writeSecret("two", new SecretWriteContext("memory", "catalog", 2L, "token"));

    byte[] first = provider.storedBytes(urn1);
    byte[] second = provider.storedBytes(urn2);
    Assertions.assertNotNull(first);
    Assertions.assertNotNull(second);

    provider.close();
    Assertions.assertNull(provider.storedBytes(urn1));
    Assertions.assertNull(provider.storedBytes(urn2));
    for (byte b : first) {
      Assertions.assertEquals((byte) 0, b);
    }
    for (byte b : second) {
      Assertions.assertEquals((byte) 0, b);
    }
  }
}
