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
package org.apache.gravitino.kms.transit;

import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.exceptions.ConnectionFailedException;

final class OpenBaoTransitKmsClient implements KmsClient {

  private final String source;
  private final TransitApiClient apiClient;

  OpenBaoTransitKmsClient(String source, URI serviceAddress, String transitMount, Path tokenFile) {
    this.source = source;
    this.apiClient =
        new TransitApiClient("OpenBao Transit", serviceAddress, transitMount, tokenFile);
  }

  /** {@inheritDoc} */
  @Override
  public KmsKeyProperties getKeyProperties(KmsReference reference) {
    validateReference(reference);

    Optional<TransitReadKeyResponse> response =
        apiClient.readKey(new TransitReadKeyRequest(reference.keyId()));
    if (!response.isPresent()) {
      return missingProperties(reference);
    }

    TransitKeyData data = response.get().data();
    if (data == null || data.supportsEncryption() == null || data.supportsDecryption() == null) {
      throw malformedResponse();
    }
    if (Boolean.TRUE.equals(data.softDeleted())) {
      return missingProperties(reference);
    }

    return new OpenBaoTransitKmsKeyProperties(
        reference, true, true, data.supportsEncryption(), data.supportsDecryption());
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    apiClient.close();
  }

  private void validateReference(KmsReference reference) {
    if (reference == null) {
      throw new IllegalArgumentException("OpenBao Transit key reference cannot be null");
    }
    if (reference.api() != KmsApi.OPENBAO_TRANSIT) {
      throw new IllegalArgumentException(
          String.format("KMS API %s does not match OpenBao Transit", reference.api()));
    }
    if (!source.equals(reference.source())) {
      throw new IllegalArgumentException(
          String.format(
              "OpenBao Transit source %s does not match configured source %s",
              reference.source(), source));
    }
    validateKeyId(reference.keyId());
  }

  private static void validateKeyId(String keyId) {
    if (keyId == null || keyId.trim().isEmpty()) {
      throw new IllegalArgumentException("OpenBao Transit key ID cannot be blank");
    }
    if (keyId.contains("/") || keyId.contains("\\") || ".".equals(keyId) || "..".equals(keyId)) {
      throw new IllegalArgumentException(
          String.format("Invalid OpenBao Transit key ID: %s", keyId));
    }
  }

  private static OpenBaoTransitKmsKeyProperties missingProperties(KmsReference reference) {
    return new OpenBaoTransitKmsKeyProperties(reference, false, false, false, false);
  }

  private static ConnectionFailedException malformedResponse() {
    return new ConnectionFailedException("OpenBao Transit returned a malformed response");
  }
}
