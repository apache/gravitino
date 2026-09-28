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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.config.CredentialConfig;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.COSProperties;
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
  @SafeVarargs
  public static Set<String> getCredentialProvidersByOrder(
      Supplier<Map<String, String>>... propertiesSuppliers) {

    for (Supplier<Map<String, String>> supplier : propertiesSuppliers) {
      Map<String, String> properties = supplier.get();
      Set<String> providers = getCredentialProvidersFromProperties(properties);
      if (!providers.isEmpty()) {
        return providers;
      }
    }

    return Collections.emptySet();
  }

  /**
   * Like {@link #getCredentialProvidersByOrder}, but when no supplier sets {@code
   * credential-providers}, infers storage providers from merged properties (same rules as catalog
   * auto-detect). Suppliers are typically fileset → schema → catalog; for inference, earlier
   * suppliers win when the same key appears at multiple levels.
   *
   * @param propertiesSuppliers properties in precedence order for explicit {@code
   *     credential-providers}; for inference, earlier entries override later keys
   * @return selected or inferred credential provider types
   */
  @SafeVarargs
  public static Set<String> getCredentialProvidersByOrderOrInfer(
      Supplier<Map<String, String>>... propertiesSuppliers) {
    Set<String> providers = getCredentialProvidersByOrder(propertiesSuppliers);
    if (!providers.isEmpty()) {
      return providers;
    }
    Map<String, String> merged = new HashMap<>();
    // Suppliers are typically fileset → schema → catalog. Merge so earlier (fileset) wins.
    for (int i = propertiesSuppliers.length - 1; i >= 0; i--) {
      Map<String, String> properties = propertiesSuppliers[i].get();
      if (properties != null) {
        merged.putAll(properties);
      }
    }
    return inferStorageCredentialProviders(merged);
  }

  /**
   * Infers storage credential provider types from plaintext properties when {@code
   * credential-providers} is omitted (catalog and fileset-path fallback).
   *
   * @param properties merged properties (may be null)
   * @return inferred provider type names (possibly empty)
   */
  public static Set<String> inferStorageCredentialProviders(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptySet();
    }
    List<String> credentialProviders = new ArrayList<>();
    addStorageCredentialProviders(properties, credentialProviders);
    return new HashSet<>(credentialProviders);
  }

  /**
   * Appends inferred storage credential provider names for S3/OSS/Azure/GCS/COS key pairs present
   * in {@code properties}. No-op when {@code properties} or {@code credentialProviders} is null or
   * when {@code properties} is empty.
   *
   * @param properties catalog or merged plaintext properties
   * @param credentialProviders list to append to
   */
  public static void addStorageCredentialProviders(
      Map<String, String> properties, List<String> credentialProviders) {
    if (properties == null || properties.isEmpty() || credentialProviders == null) {
      return;
    }
    String s3AccessKeyId = properties.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID);
    String s3SecretAccessKey = properties.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
    if (StringUtils.isNotBlank(s3AccessKeyId) && StringUtils.isNotBlank(s3SecretAccessKey)) {
      credentialProviders.add(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE);
    }

    String ossAccessKeyId = properties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID);
    String ossSecretAccessKey = properties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET);
    if (StringUtils.isNotBlank(ossAccessKeyId) && StringUtils.isNotBlank(ossSecretAccessKey)) {
      credentialProviders.add(OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE);
    }

    String azureAccountName = properties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
    String azureAccountKey = properties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    if (StringUtils.isNotBlank(azureAccountName) && StringUtils.isNotBlank(azureAccountKey)) {
      credentialProviders.add(AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE);
    }

    String gcsServiceAccountFile = properties.get(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);
    if (StringUtils.isNotBlank(gcsServiceAccountFile)) {
      credentialProviders.add(GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE);
    }

    String cosAccessKeyId = properties.get(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID);
    String cosSecretAccessKey = properties.get(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET);
    if (StringUtils.isNotBlank(cosAccessKeyId) && StringUtils.isNotBlank(cosSecretAccessKey)) {
      credentialProviders.add(COSSecretKeyCredential.COS_SECRET_KEY_CREDENTIAL_TYPE);
    }
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
