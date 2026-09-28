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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Builds static cloud secret-key credentials from merged plaintext properties (fileset → schema →
 * catalog). Used so fileset/schema AK/SK overrides are returned by {@code getCredentials} when that
 * static provider type was already selected for the request.
 */
final class StaticSecretKeyCredentialFactory {

  private StaticSecretKeyCredentialFactory() {}

  /**
   * Materializes static secret-key credentials present in {@code properties}. Later list entries
   * replace earlier ones of the same {@link Credential#credentialType()}.
   *
   * @param properties merged plaintext properties
   * @return static credentials that can be built from the map (may be empty)
   */
  static List<Credential> fromProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return List.of();
    }
    List<Credential> built = new ArrayList<>();
    buildS3(properties).ifPresent(built::add);
    buildOss(properties).ifPresent(built::add);
    buildCos(properties).ifPresent(built::add);
    buildAzure(properties).ifPresent(built::add);
    return built;
  }

  /**
   * Replaces or appends static secret-key credentials derived from {@code mergedProperties} onto
   * {@code existing}, but only for types in {@code allowedTypes} (the providers already selected
   * for this request). Token / other credentials are left unchanged; static types not in the
   * allowlist are not introduced.
   *
   * @param existing credentials already vended by catalog providers
   * @param mergedProperties fileset→schema→catalog plaintext properties
   * @param allowedTypes credential types selected for this request (e.g. path-context providers)
   * @return combined list with allowed static secret-key types reflecting merged properties
   */
  static List<Credential> overlay(
      List<Credential> existing, Map<String, String> mergedProperties, Set<String> allowedTypes) {
    if (allowedTypes == null || allowedTypes.isEmpty()) {
      return existing;
    }
    List<Credential> fromMerged =
        fromProperties(mergedProperties).stream()
            .filter(c -> allowedTypes.contains(c.credentialType()))
            .collect(Collectors.toList());
    if (fromMerged.isEmpty()) {
      return existing;
    }
    Map<String, Credential> byType = new HashMap<>();
    if (existing != null) {
      for (Credential credential : existing) {
        if (credential != null) {
          byType.put(credential.credentialType(), credential);
        }
      }
    }
    for (Credential credential : fromMerged) {
      byType.put(credential.credentialType(), credential);
    }
    return new ArrayList<>(byType.values());
  }

  private static Optional<Credential> buildS3(Map<String, String> properties) {
    String accessKeyId = properties.get(S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID);
    String secretAccessKey =
        properties.get(S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY);
    if (StringUtils.isAnyBlank(accessKeyId, secretAccessKey)) {
      return Optional.empty();
    }
    return Optional.of(new S3SecretKeyCredential(accessKeyId, secretAccessKey));
  }

  private static Optional<Credential> buildOss(Map<String, String> properties) {
    String accessKeyId = properties.get(OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_ACCESS_KEY_ID);
    String secretAccessKey =
        properties.get(OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_SECRET_ACCESS_KEY);
    if (StringUtils.isAnyBlank(accessKeyId, secretAccessKey)) {
      return Optional.empty();
    }
    return Optional.of(new OSSSecretKeyCredential(accessKeyId, secretAccessKey));
  }

  private static Optional<Credential> buildCos(Map<String, String> properties) {
    String accessKeyId = properties.get(COSSecretKeyCredential.GRAVITINO_COS_STATIC_ACCESS_KEY_ID);
    String secretAccessKey =
        properties.get(COSSecretKeyCredential.GRAVITINO_COS_STATIC_SECRET_ACCESS_KEY);
    if (StringUtils.isAnyBlank(accessKeyId, secretAccessKey)) {
      return Optional.empty();
    }
    return Optional.of(new COSSecretKeyCredential(accessKeyId, secretAccessKey));
  }

  private static Optional<Credential> buildAzure(Map<String, String> properties) {
    String accountName =
        properties.get(AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
    String accountKey =
        properties.get(AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    if (StringUtils.isAnyBlank(accountName, accountKey)) {
      return Optional.empty();
    }
    return Optional.of(new AzureAccountKeyCredential(accountName, accountKey));
  }
}
