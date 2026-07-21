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

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.KmsConfigurationException;

/** Creates key-inspection clients for Microsoft Azure Key Vault. */
@DeveloperApi
public final class AzureKeyVaultKmsClientFactory implements KmsClientFactory {

  static final String VAULT_URL = "endpoint.vaultUrl";
  static final String CREDENTIAL_METHOD = "credential.method";
  static final String MANAGED_IDENTITY_CLIENT_ID = "credential.managedIdentityClientId";

  private static final Set<String> SUPPORTED_PROPERTIES =
      ImmutableSet.of(VAULT_URL, CREDENTIAL_METHOD, MANAGED_IDENTITY_CLIENT_ID);

  /** Creates an Azure Key Vault KMS client factory. */
  public AzureKeyVaultKmsClientFactory() {}

  /**
   * Returns the Azure Key Vault API.
   *
   * @return the Azure Key Vault API
   */
  @Override
  public KmsApi api() {
    return KmsApi.AZURE_KEY_VAULT;
  }

  /**
   * Creates a client using Azure's default credential chain.
   *
   * <p>The required {@code endpoint.vaultUrl} property is the vault HTTPS origin. The {@code
   * credential.method} must be {@code default}. The optional {@code
   * credential.managedIdentityClientId} selects a user-assigned managed identity.
   *
   * @param source logical name of the configured Azure Key Vault source
   * @param properties Azure Key Vault client properties
   * @return the configured key-inspection client
   * @throws IllegalArgumentException if the source or properties are invalid
   */
  @Override
  public KmsClient create(String source, Map<String, String> properties) {
    validateSource(source);
    if (properties == null) {
      throw new KmsConfigurationException("Azure Key Vault properties cannot be null");
    }

    for (String property : properties.keySet()) {
      if (!SUPPORTED_PROPERTIES.contains(property)) {
        throw new KmsConfigurationException("Unsupported Azure Key Vault property '%s'", property);
      }
    }

    URI vaultUri = parseVaultUri(requiredProperty(properties, VAULT_URL));
    String credentialMethod = requiredProperty(properties, CREDENTIAL_METHOD);
    if (!"default".equals(credentialMethod)) {
      throw new KmsConfigurationException(
          "Unsupported Azure Key Vault credential method: %s", credentialMethod);
    }
    DefaultAzureCredentialBuilder credentialBuilder = new DefaultAzureCredentialBuilder();
    String managedIdentityClientId = properties.get(MANAGED_IDENTITY_CLIENT_ID);
    if (managedIdentityClientId != null) {
      if (managedIdentityClientId.trim().isEmpty()) {
        throw new KmsConfigurationException(
            "Azure Key Vault property '%s' cannot be blank", MANAGED_IDENTITY_CLIENT_ID);
      }
      credentialBuilder.managedIdentityClientId(managedIdentityClientId.trim());
    }

    KeyClient keyClient =
        new KeyClientBuilder()
            .vaultUrl(vaultUri.toString())
            .credential(credentialBuilder.build())
            .buildClient();
    return new AzureKeyVaultKmsClient(source, vaultUri, keyClient);
  }

  private static void validateSource(String source) {
    if (source == null || source.trim().isEmpty()) {
      throw new KmsConfigurationException("Azure Key Vault source cannot be blank");
    }
  }

  private static String requiredProperty(Map<String, String> properties, String property) {
    String value = properties.get(property);
    if (value == null || value.trim().isEmpty()) {
      throw new KmsConfigurationException("Azure Key Vault property '%s' is required", property);
    }
    return value.trim();
  }

  private static URI parseVaultUri(String value) {
    URI vaultUri;
    try {
      vaultUri = new URI(value);
    } catch (URISyntaxException e) {
      throw new KmsConfigurationException(e, "Azure Key Vault URL must be a valid URI");
    }
    return AzureKeyVaultKeyIdentifier.validateVaultUri(vaultUri);
  }
}
