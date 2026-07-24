/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.config.CredentialConfig;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

public class CredentialUtils {

  public static Map<String, CredentialProvider> loadCredentialProviders(
      Map<String, String> catalogProperties) {
    CredentialConfig credentialConfig = new CredentialConfig(catalogProperties);
    List<String> credentialProviders = credentialConfig.get(CredentialConfig.CREDENTIAL_PROVIDERS);

    return credentialProviders.stream()
        .collect(
            Collectors.toMap(
                String::toString,
                credentialType ->
                    CredentialProviderFactory.create(credentialType, catalogProperties)));
  }

  /**
   * Get Credential providers from properties supplier.
   *
   * <p>If there are multiple properties suppliers, will try to get the credential providers in the
   * input order.
   *
   * @param propertiesSuppliers The properties suppliers.
   * @return A set of credential providers.
   */
  public static Set<String> getCredentialProvidersByOrder(
      Supplier<Map<String, String>>... propertiesSuppliers) {

    for (Supplier<Map<String, String>> supplier : propertiesSuppliers) {
      Map<String, String> properties = supplier.get();
      Set<String> providers = getCredentialProvidersFromProperties(properties);
      if (!providers.isEmpty()) {
        return providers;
      }
    }

    // No explicit `credential-providers` is configured; infer the providers from static storage
    // credentials (e.g. s3-access-key-id/s3-secret-access-key) instead. This keeps fileset-level
    // (path-based) credential vending consistent with catalog-level vending, so a catalog
    // configured with only static credentials can still vend them without explicitly setting
    // `credential-providers`.
    for (Supplier<Map<String, String>> supplier : propertiesSuppliers) {
      Set<String> providers = getStorageCredentialProviders(supplier.get());
      if (!providers.isEmpty()) {
        return providers;
      }
    }

    return Collections.emptySet();
  }

  /**
   * Infers the storage credential provider types from static cloud storage credentials present in
   * the properties (S3/OSS/Azure/GCS). This is used to enable credential vending when only static
   * credentials are configured, without an explicit {@code credential-providers} setting.
   *
   * @param properties The properties that may contain static storage credentials.
   * @return The inferred credential provider types, or an empty set if none is found.
   */
  public static Set<String> getStorageCredentialProviders(Map<String, String> properties) {
    Set<String> providers = new LinkedHashSet<>();
    if (properties == null) {
      return providers;
    }

    String s3AccessKeyId = properties.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID);
    String s3SecretAccessKey = properties.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
    if (StringUtils.isNotBlank(s3AccessKeyId) && StringUtils.isNotBlank(s3SecretAccessKey)) {
      providers.add(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE);
    }

    String ossAccessKeyId = properties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID);
    String ossSecretAccessKey = properties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET);
    if (StringUtils.isNotBlank(ossAccessKeyId) && StringUtils.isNotBlank(ossSecretAccessKey)) {
      providers.add(OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE);
    }

    String azureAccountName = properties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
    String azureAccountKey = properties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    if (StringUtils.isNotBlank(azureAccountName) && StringUtils.isNotBlank(azureAccountKey)) {
      providers.add(AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE);
    }

    String gcsServiceAccountFile = properties.get(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);
    if (StringUtils.isNotBlank(gcsServiceAccountFile)) {
      providers.add(GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE);
    }

    return providers;
  }

  private static Set<String> getCredentialProvidersFromProperties(Map<String, String> properties) {
    if (properties == null) {
      return Collections.emptySet();
    }

    CredentialConfig credentialConfig = new CredentialConfig(properties);
    return credentialConfig.get(CredentialConfig.CREDENTIAL_PROVIDERS).stream()
        .collect(Collectors.toSet());
  }
}
