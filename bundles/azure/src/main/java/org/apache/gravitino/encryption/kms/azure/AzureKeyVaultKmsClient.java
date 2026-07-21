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
package org.apache.gravitino.encryption.kms.azure;

import com.azure.core.exception.AzureException;
import com.azure.core.exception.ClientAuthenticationException;
import com.azure.core.exception.ResourceNotFoundException;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.azure.security.keyvault.keys.models.KeyProperties;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsAuthenticationException;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.exceptions.ConnectionFailedException;

final class AzureKeyVaultKmsClient implements KmsClient {

  private final String source;
  private final URI vaultUri;
  private final KeyClient keyClient;
  private final Clock clock;

  AzureKeyVaultKmsClient(String source, URI vaultUri, KeyClient keyClient) {
    this(source, vaultUri, keyClient, Clock.systemUTC());
  }

  AzureKeyVaultKmsClient(String source, URI vaultUri, KeyClient keyClient, Clock clock) {
    if (source == null || source.trim().isEmpty()) {
      throw new IllegalArgumentException("Azure Key Vault source cannot be blank");
    }
    if (keyClient == null || clock == null) {
      throw new IllegalArgumentException("Azure Key Vault SDK client and clock cannot be null");
    }

    this.source = source;
    this.vaultUri = AzureKeyVaultKeyIdentifier.validateVaultUri(vaultUri);
    this.keyClient = keyClient;
    this.clock = clock;
  }

  @Override
  public KmsKeyProperties getKeyProperties(KmsReference reference) {
    validateReference(reference);
    AzureKeyVaultKeyIdentifier identifier =
        AzureKeyVaultKeyIdentifier.parse(reference.keyId(), vaultUri);

    try {
      KeyVaultKey key = keyClient.getKey(identifier.name(), identifier.version());
      return normalize(reference, key);
    } catch (ResourceNotFoundException e) {
      return new AzureKeyVaultKmsKeyProperties(reference, false, false, false, false);
    } catch (ConnectionFailedException e) {
      throw e;
    } catch (ClientAuthenticationException e) {
      throw new KmsAuthenticationException(
          e, "Azure Key Vault rejected credentials for source '%s'", source);
    } catch (AzureException e) {
      throw connectionFailure(reference, e);
    } catch (RuntimeException e) {
      throw connectionFailure(reference, e);
    }
  }

  private void validateReference(KmsReference reference) {
    if (reference == null) {
      throw new IllegalArgumentException("Azure Key Vault reference cannot be null");
    }
    if (reference.api() != KmsApi.AZURE_KEY_VAULT) {
      throw new IllegalArgumentException(
          String.format("KMS reference API must be '%s'", KmsApi.AZURE_KEY_VAULT.wireValue()));
    }
    if (!source.equals(reference.source())) {
      throw new IllegalArgumentException(
          String.format("KMS reference source must be '%s'", source));
    }
  }

  private KmsKeyProperties normalize(KmsReference reference, KeyVaultKey key) {
    if (key == null || key.getProperties() == null) {
      throw new ConnectionFailedException(
          "Azure Key Vault returned incomplete properties for key '%s'", reference.keyId());
    }

    KeyProperties properties = key.getProperties();
    List<KeyOperation> operations = key.getKeyOperations();
    boolean supportsWrapping = operations != null && operations.contains(KeyOperation.WRAP_KEY);
    boolean supportsUnwrapping = operations != null && operations.contains(KeyOperation.UNWRAP_KEY);
    return new AzureKeyVaultKmsKeyProperties(
        reference,
        true,
        isEffectivelyEnabled(properties, clock.instant()),
        supportsWrapping,
        supportsUnwrapping);
  }

  private static boolean isEffectivelyEnabled(KeyProperties properties, Instant now) {
    if (!Boolean.TRUE.equals(properties.isEnabled())) {
      return false;
    }

    OffsetDateTime notBefore = properties.getNotBefore();
    if (notBefore != null && now.isBefore(notBefore.toInstant())) {
      return false;
    }

    OffsetDateTime expiresOn = properties.getExpiresOn();
    return expiresOn == null || now.isBefore(expiresOn.toInstant());
  }

  private ConnectionFailedException connectionFailure(KmsReference reference, RuntimeException e) {
    return new ConnectionFailedException(
        e, "Failed to inspect Azure Key Vault key '%s' for source '%s'", reference.keyId(), source);
  }
}
