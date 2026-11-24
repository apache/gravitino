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

import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Classifies credential-provider types for catalog {@code getCredentials} authorization.
 *
 * <p>Short-lived token providers may be returned with metadata access alone. Static (long-lived)
 * providers require {@code USE_SECRETS} and {@code INCLUDE_CREDENTIAL_SECRETS} (or metalake owner).
 */
public final class CredentialProviderKinds {

  private static final Set<String> TOKEN_CREDENTIAL_TYPES =
      ImmutableSet.of(
          S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE,
          OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE,
          COSTokenCredential.COS_TOKEN_CREDENTIAL_TYPE,
          ADLSTokenCredential.ADLS_TOKEN_CREDENTIAL_TYPE,
          GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE,
          AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE);

  private CredentialProviderKinds() {}

  /**
   * Returns whether {@code credentialType} is a short-lived token / scoped vending type.
   *
   * @param credentialType credential type string, or null
   * @return true for known token providers
   */
  public static boolean isTokenCredentialType(@Nullable String credentialType) {
    if (credentialType == null || credentialType.isEmpty()) {
      return false;
    }
    return TOKEN_CREDENTIAL_TYPES.contains(credentialType.toLowerCase(Locale.ROOT));
  }
}
