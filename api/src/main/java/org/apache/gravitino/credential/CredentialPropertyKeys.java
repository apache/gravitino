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
package org.apache.gravitino.credential;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Catalog property keys that appear in {@link Credential#credentialInfo()}.
 *
 * <p>These keys are delivered to clients via {@link SupportsCredentials#getCredentials()}, not via
 * {@code getSecrets()}. {@code getSecrets()} must not recover them as inline plaintext or secret
 * URNs so the two APIs do not overlap.
 *
 * <p>Excludes IRSA session field names ({@code access-key-id}, {@code secret-access-key}, {@code
 * session-token}) and the GCS credential field {@code token}, which are not catalog entity property
 * keys.
 */
public final class CredentialPropertyKeys {

  private static final Set<String> KEYS;

  static {
    Set<String> keys = new HashSet<>();
    // S3 secret-key / token
    keys.add(S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID);
    keys.add(S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY);
    keys.add(S3TokenCredential.GRAVITINO_S3_TOKEN);
    // OSS secret-key / token
    keys.add(OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_ACCESS_KEY_ID);
    keys.add(OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_SECRET_ACCESS_KEY);
    keys.add(OSSTokenCredential.GRAVITINO_OSS_TOKEN);
    // COS secret-key / token
    keys.add(COSSecretKeyCredential.GRAVITINO_COS_STATIC_ACCESS_KEY_ID);
    keys.add(COSSecretKeyCredential.GRAVITINO_COS_STATIC_SECRET_ACCESS_KEY);
    keys.add(COSTokenCredential.GRAVITINO_COS_SESSION_TOKEN);
    // Azure account key / ADLS token
    keys.add(AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
    keys.add(AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    keys.add(ADLSTokenCredential.GRAVITINO_ADLS_SAS_TOKEN);
    // JDBC
    keys.add(JdbcCredential.GRAVITINO_JDBC_USER);
    keys.add(JdbcCredential.GRAVITINO_JDBC_PASSWORD);
    // Glue AWS API credentials
    keys.add(AwsSecretKeyCredential.GRAVITINO_AWS_ACCESS_KEY_ID);
    keys.add(AwsSecretKeyCredential.GRAVITINO_AWS_SECRET_ACCESS_KEY);
    // Paimon DLF credentials
    keys.add(DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_ID);
    keys.add(DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_SECRET);
    keys.add(DlfSecretKeyCredential.GRAVITINO_DLF_SECURITY_TOKEN);
    KEYS = Collections.unmodifiableSet(keys);
  }

  private CredentialPropertyKeys() {}

  /**
   * Returns whether {@code key} is a credential-info property key.
   *
   * @param key the property key
   * @return true when the key is delivered via {@link SupportsCredentials}
   */
  public static boolean isCredentialPropertyKey(@Nullable String key) {
    return key != null && KEYS.contains(key);
  }

  /**
   * Returns the immutable set of credential property keys.
   *
   * @return credential property keys
   */
  public static Set<String> keys() {
    return KEYS;
  }
}
