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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.gravitino.secret.GravitinoSecretProvider;
import org.apache.gravitino.secret.SecretReferenceLocator;
import org.apache.gravitino.secret.SecretUrn;
import org.apache.gravitino.secret.SecretWriteContext;

/** In-memory secret provider for development and testing. */
public class InMemorySecretsProvider implements GravitinoSecretProvider {

  private final ConcurrentHashMap<String, String> secrets = new ConcurrentHashMap<>();

  @Override
  public String type() {
    return "memory";
  }

  @Override
  public String writeSecret(String plaintext, SecretWriteContext context) {
    String urn =
        SecretUrn.buildWriteThrough(
            context.providerName(),
            context.entityType(),
            context.entityId(),
            context.propertyKey());
    secrets.put(
        urn, Base64.getEncoder().encodeToString(plaintext.getBytes(StandardCharsets.UTF_8)));
    return urn;
  }

  @Override
  public String readSecret(String urn) {
    String encoded = secrets.get(urn);
    if (encoded == null) {
      throw new IllegalArgumentException("Secret not found for URN: " + urn);
    }
    return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
  }

  @Override
  public void deleteSecret(String urn) {
    secrets.remove(urn);
  }

  @Override
  public String buildExternalReferenceUrn(String propertyKey, SecretReferenceLocator locator) {
    throw new UnsupportedOperationException(
        String.format("Provider %s does not support external secret references", type()));
  }
}
