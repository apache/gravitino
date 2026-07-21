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
package org.apache.gravitino.kms.aws;

import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsAuthenticationException;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.InvalidArnException;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KeyState;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.awssdk.services.kms.model.NotFoundException;

final class AwsKmsClient implements KmsClient {

  private final String source;
  private final software.amazon.awssdk.services.kms.KmsClient delegate;

  AwsKmsClient(String source, software.amazon.awssdk.services.kms.KmsClient delegate) {
    this.source = source;
    this.delegate = delegate;
  }

  @Override
  public KmsKeyProperties getKeyProperties(KmsReference reference) {
    validateReference(reference);
    String providerKeyId = reference.keyId();

    try {
      DescribeKeyResponse response =
          delegate.describeKey(DescribeKeyRequest.builder().keyId(providerKeyId).build());
      if (response == null || response.keyMetadata() == null) {
        throw new ConnectionFailedException(
            "AWS KMS returned no metadata for key '%s' from source '%s'", providerKeyId, source);
      }

      KeyMetadata metadata = response.keyMetadata();
      boolean enabled =
          Boolean.TRUE.equals(metadata.enabled()) && metadata.keyState() == KeyState.ENABLED;
      boolean supportsEncryption = metadata.keyUsage() == KeyUsageType.ENCRYPT_DECRYPT;
      return new AwsKmsKeyProperties(
          reference, true, enabled, supportsEncryption, supportsEncryption);
    } catch (NotFoundException e) {
      return new AwsKmsKeyProperties(reference, false, false, false, false);
    } catch (InvalidArnException e) {
      throw new IllegalArgumentException(
          String.format("Invalid AWS KMS key ID '%s'", providerKeyId), e);
    } catch (KmsException e) {
      if (e.statusCode() == 401 || e.statusCode() == 403) {
        throw new KmsAuthenticationException(
            e, "AWS KMS rejected credentials for source '%s'", source);
      }
      throw new ConnectionFailedException(
          e, "Failed to inspect AWS KMS key '%s' from source '%s'", providerKeyId, source);
    } catch (SdkClientException e) {
      throw new ConnectionFailedException(
          e, "Failed to inspect AWS KMS key '%s' from source '%s'", providerKeyId, source);
    }
  }

  @Override
  public void close() {
    delegate.close();
  }

  private void validateReference(KmsReference reference) {
    if (reference == null) {
      throw new IllegalArgumentException("KMS reference cannot be null");
    }
    if (reference.api() != KmsApi.AWS_KMS) {
      throw new IllegalArgumentException(
          String.format(
              "KMS source '%s' requires API '%s', not '%s'",
              source, KmsApi.AWS_KMS.wireValue(), reference.api().wireValue()));
    }
    if (!source.equals(reference.source())) {
      throw new IllegalArgumentException(
          String.format(
              "KMS reference source '%s' does not match AWS KMS source '%s'",
              reference.source(), source));
    }
  }
}
