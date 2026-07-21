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
package org.apache.gravitino.kms.gcp;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;

final class GoogleCloudKmsClient implements KmsClient {

  private static final Pattern CRYPTO_KEY_NAME =
      Pattern.compile("projects/([^/]+)/locations/([^/]+)/keyRings/[^/]+/cryptoKeys/[^/]+");

  private final String source;
  private final String projectId;
  private final String location;
  private final GoogleCloudKmsMetadataService metadataService;
  private final AtomicBoolean closed = new AtomicBoolean();

  GoogleCloudKmsClient(
      String source,
      String projectId,
      String location,
      GoogleCloudKmsMetadataService metadataService) {
    if (source == null || source.trim().isEmpty()) {
      throw new IllegalArgumentException("Google Cloud KMS source cannot be blank");
    }
    if (metadataService == null) {
      throw new IllegalArgumentException("Google Cloud KMS metadata service cannot be null");
    }

    this.source = source;
    this.projectId = projectId;
    this.location = location;
    this.metadataService = metadataService;
  }

  /** {@inheritDoc} */
  @Override
  public KmsKeyProperties getKeyProperties(KmsReference reference) {
    validateReference(reference);

    Optional<GoogleCloudKmsKeyMetadata> metadata = metadataService.getKey(reference.keyId());
    if (metadata == null) {
      throw new IllegalStateException("Google Cloud KMS metadata service returned null");
    }
    if (!metadata.isPresent()) {
      return GoogleCloudKmsKeyProperties.missing(reference);
    }

    GoogleCloudKmsKeyMetadata key = metadata.get();
    return new GoogleCloudKmsKeyProperties(
        reference,
        true,
        key.primaryVersionEnabled(),
        key.encryptDecryptPurpose(),
        key.encryptDecryptPurpose());
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      metadataService.close();
    }
  }

  private void validateReference(KmsReference reference) {
    if (reference == null) {
      throw new IllegalArgumentException("Google Cloud KMS reference cannot be null");
    }
    if (reference.api() != KmsApi.GOOGLE_CLOUD_KMS) {
      throw new IllegalArgumentException(
          String.format("KMS API %s does not match Google Cloud KMS", reference.api()));
    }
    if (!source.equals(reference.source())) {
      throw new IllegalArgumentException(
          String.format(
              "Google Cloud KMS source %s does not match configured source %s",
              reference.source(), source));
    }
    String providerKeyId = reference.keyId();
    Matcher matcher = CRYPTO_KEY_NAME.matcher(providerKeyId);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format("Invalid Google Cloud KMS CryptoKey resource name: %s", providerKeyId));
    }
    if (!projectId.equals(matcher.group(1)) || !location.equals(matcher.group(2))) {
      throw new IllegalArgumentException(
          String.format(
              "Google Cloud KMS key %s is outside configured project %s and location %s",
              providerKeyId, projectId, location));
    }
  }
}
