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

import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestInMemorySecretsProvider {

  @Test
  public void testWriteReadDelete() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    Assertions.assertEquals("memory", provider.type());

    SecretWriteContext context = new SecretWriteContext("memory", "catalog", 10L, "password");
    String urn = provider.writeSecret("s3cr3t", context);
    Assertions.assertEquals("urn:gravitino-secret:memory:catalog:10:password", urn);
    Assertions.assertEquals("s3cr3t", provider.readSecret(urn));

    provider.deleteSecret(urn);
    Assertions.assertThrows(IllegalArgumentException.class, () -> provider.readSecret(urn));
  }

  @Test
  public void testExternalReferenceUnsupported() {
    InMemorySecretsProvider provider = new InMemorySecretsProvider();
    SecretReferenceLocator locator = new SecretReferenceLocator("memory", "mount", "path");
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> provider.buildExternalReferenceUrn("password", locator));
    Assertions.assertTrue(exception.getMessage().contains("memory"));
  }
}
