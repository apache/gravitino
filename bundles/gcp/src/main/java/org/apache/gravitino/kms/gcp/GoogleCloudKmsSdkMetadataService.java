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

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose;
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import java.util.Optional;
import org.apache.gravitino.encryption.kms.KmsAuthenticationException;
import org.apache.gravitino.exceptions.ConnectionFailedException;

final class GoogleCloudKmsSdkMetadataService implements GoogleCloudKmsMetadataService {

  private final String source;
  private final KeyManagementServiceClient client;

  GoogleCloudKmsSdkMetadataService(String source, KeyManagementServiceClient client) {
    if (client == null) {
      throw new IllegalArgumentException("Google Cloud KMS SDK client cannot be null");
    }
    this.source = source;
    this.client = client;
  }

  @Override
  public Optional<GoogleCloudKmsKeyMetadata> getKey(String resourceName) {
    try {
      CryptoKey key = client.getCryptoKey(resourceName);
      if (key == null) {
        throw new ConnectionFailedException(
            "Google Cloud KMS returned an empty response for source %s", source);
      }
      boolean encryptDecryptPurpose = key.getPurpose() == CryptoKeyPurpose.ENCRYPT_DECRYPT;
      boolean primaryVersionEnabled =
          key.hasPrimary() && key.getPrimary().getState() == CryptoKeyVersionState.ENABLED;
      return Optional.of(
          new GoogleCloudKmsKeyMetadata(encryptDecryptPurpose, primaryVersionEnabled));
    } catch (NotFoundException e) {
      return Optional.empty();
    } catch (ConnectionFailedException e) {
      throw e;
    } catch (ApiException e) {
      switch (e.getStatusCode().getCode()) {
        case UNAUTHENTICATED:
        case PERMISSION_DENIED:
          throw new KmsAuthenticationException(
              e, "Google Cloud KMS rejected credentials for source %s", source);
        default:
          break;
      }
      throw unavailable(e);
    } catch (RuntimeException e) {
      throw unavailable(e);
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private ConnectionFailedException unavailable(RuntimeException cause) {
    return new ConnectionFailedException(
        cause, "Google Cloud KMS could not read key properties for source %s", source);
  }
}
