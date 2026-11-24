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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.credential.AwsSecretKeyCredential;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.COSSecretKeyCredential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.DlfSecretKeyCredential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.storage.AWSProperties;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.COSProperties;
import org.apache.gravitino.storage.CloudStorageCredentialPropertyKeys;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

/**
 * Maps static credential types to the property keys that {@code getSecrets} must treat as
 * credential secrets ({@code INCLUDE_CREDENTIAL_SECRETS} required). Lives in {@code core} so it can
 * use {@code api} credential-type constants without pulling {@code api} into {@code catalog-common}
 * (used by GVFS).
 *
 * <p>Token providers are intentionally absent. The flattened property-key set used by GVFS stays in
 * {@link CloudStorageCredentialPropertyKeys}.
 */
public final class StaticCredentialPropertyKeys {

  /**
   * Static credential type → property keys. Map keys are {@link
   * org.apache.gravitino.credential.Credential#credentialType()} values for static providers.
   */
  private static final Map<String, Set<String>> KEYS_BY_CREDENTIAL_TYPE =
      ImmutableMap.<String, Set<String>>builder()
          .put(
              S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
                  S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY))
          .put(
              OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
                  OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET))
          .put(
              COSSecretKeyCredential.COS_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  COSProperties.GRAVITINO_COS_ACCESS_KEY_ID,
                  COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET))
          .put(
              AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  AWSProperties.GRAVITINO_AWS_ACCESS_KEY_ID,
                  AWSProperties.GRAVITINO_AWS_SECRET_ACCESS_KEY))
          .put(
              AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY))
          .put(
              DlfSecretKeyCredential.DLF_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID,
                  PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET,
                  PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN))
          .put(
              JdbcCredential.JDBC_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  CloudStorageCredentialPropertyKeys.JDBC_USER,
                  CloudStorageCredentialPropertyKeys.JDBC_PASSWORD))
          .build();

  private StaticCredentialPropertyKeys() {}

  /**
   * Returns property keys for a static credential type, or an empty set when the type is unknown or
   * is a token provider.
   *
   * @param credentialType credential-provider type (for example {@code s3-secret-key})
   * @return property keys for that type; never null
   */
  public static Set<String> keysForCredentialType(@Nullable String credentialType) {
    if (credentialType == null || credentialType.isEmpty()) {
      return ImmutableSet.of();
    }
    Set<String> keys = KEYS_BY_CREDENTIAL_TYPE.get(credentialType.toLowerCase(Locale.ROOT));
    return keys == null ? ImmutableSet.of() : keys;
  }

  /**
   * Returns static credential property keys implied by a comma-separated {@code
   * credential-providers} value. Token providers are ignored.
   *
   * @param credentialProviders raw {@code credential-providers} property value, or null
   * @return union of keys for enabled static credential types; never null
   */
  public static Set<String> keysForCredentialProviders(@Nullable String credentialProviders) {
    if (StringUtils.isBlank(credentialProviders)) {
      return ImmutableSet.of();
    }
    Set<String> keys = new HashSet<>();
    for (String provider : credentialProviders.split(",")) {
      keys.addAll(keysForCredentialType(provider.trim()));
    }
    return keys;
  }

  /**
   * Returns static credential property keys implied by {@code credential-providers} on {@code
   * properties}.
   *
   * @param properties entity properties, or null
   * @return union of keys for enabled static credential types; never null
   */
  public static Set<String> keysForCredentialProviders(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return ImmutableSet.of();
    }
    return keysForCredentialProviders(properties.get(CredentialConstants.CREDENTIAL_PROVIDERS));
  }
}
