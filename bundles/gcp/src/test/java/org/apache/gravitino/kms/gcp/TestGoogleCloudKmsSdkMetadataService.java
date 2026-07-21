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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import java.util.Optional;
import org.apache.gravitino.encryption.kms.KmsAuthenticationException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestGoogleCloudKmsSdkMetadataService {

  private static final String SOURCE = "analytics";
  private static final String KEY_NAME =
      "projects/data-project/locations/us/keyRings/tables/cryptoKeys/customer";

  private KeyManagementServiceClient sdkClient;
  private GoogleCloudKmsSdkMetadataService metadataService;

  @BeforeEach
  void setUp() {
    sdkClient = mock(KeyManagementServiceClient.class);
    metadataService = new GoogleCloudKmsSdkMetadataService(SOURCE, sdkClient);
  }

  @Test
  void mapsEncryptDecryptKeyWithEnabledPrimary() {
    when(sdkClient.getCryptoKey(KEY_NAME))
        .thenReturn(key(CryptoKeyPurpose.ENCRYPT_DECRYPT, CryptoKeyVersionState.ENABLED));

    GoogleCloudKmsKeyMetadata metadata =
        metadataService.getKey(KEY_NAME).orElseThrow(IllegalStateException::new);

    assertTrue(metadata.encryptDecryptPurpose());
    assertTrue(metadata.primaryVersionEnabled());
    verify(sdkClient).getCryptoKey(KEY_NAME);
  }

  @Test
  void mapsKeyWithoutPrimaryVersionAsDisabled() {
    when(sdkClient.getCryptoKey(KEY_NAME))
        .thenReturn(CryptoKey.newBuilder().setPurpose(CryptoKeyPurpose.ENCRYPT_DECRYPT).build());

    GoogleCloudKmsKeyMetadata metadata =
        metadataService.getKey(KEY_NAME).orElseThrow(IllegalStateException::new);

    assertTrue(metadata.encryptDecryptPurpose());
    assertFalse(metadata.primaryVersionEnabled());
  }

  @ParameterizedTest
  @EnumSource(
      value = CryptoKeyVersionState.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"ENABLED", "UNRECOGNIZED"})
  void mapsNonEnabledPrimaryVersionsAsDisabled(CryptoKeyVersionState state) {
    when(sdkClient.getCryptoKey(KEY_NAME)).thenReturn(key(CryptoKeyPurpose.ENCRYPT_DECRYPT, state));

    assertFalse(
        metadataService
            .getKey(KEY_NAME)
            .orElseThrow(IllegalStateException::new)
            .primaryVersionEnabled());
  }

  @ParameterizedTest
  @EnumSource(
      value = CryptoKeyPurpose.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"ENCRYPT_DECRYPT", "UNRECOGNIZED"})
  void mapsOtherPurposesAsNotSupportingEnvelopeEncryption(CryptoKeyPurpose purpose) {
    when(sdkClient.getCryptoKey(KEY_NAME)).thenReturn(key(purpose, CryptoKeyVersionState.ENABLED));

    assertFalse(
        metadataService
            .getKey(KEY_NAME)
            .orElseThrow(IllegalStateException::new)
            .encryptDecryptPurpose());
  }

  @Test
  void mapsUnrecognizedPurposeAndPrimaryStateAsUnsupportedAndDisabled() {
    CryptoKey key =
        CryptoKey.newBuilder()
            .setPurposeValue(Integer.MAX_VALUE)
            .setPrimary(CryptoKeyVersion.newBuilder().setStateValue(Integer.MAX_VALUE).build())
            .build();
    when(sdkClient.getCryptoKey(KEY_NAME)).thenReturn(key);

    GoogleCloudKmsKeyMetadata metadata =
        metadataService.getKey(KEY_NAME).orElseThrow(IllegalStateException::new);

    assertFalse(metadata.encryptDecryptPurpose());
    assertFalse(metadata.primaryVersionEnabled());
  }

  @Test
  void mapsNotFoundToEmptyMetadata() {
    ApiException failure = apiException(StatusCode.Code.NOT_FOUND);
    when(sdkClient.getCryptoKey(KEY_NAME)).thenThrow(failure);

    assertEquals(Optional.empty(), metadataService.getKey(KEY_NAME));
  }

  @ParameterizedTest
  @EnumSource(
      value = StatusCode.Code.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"OK", "NOT_FOUND", "UNAUTHENTICATED", "PERMISSION_DENIED"})
  void mapsProviderFailuresToConnectionFailures(StatusCode.Code code) {
    ApiException failure = apiException(code);
    when(sdkClient.getCryptoKey(KEY_NAME)).thenThrow(failure);

    assertThrows(ConnectionFailedException.class, () -> metadataService.getKey(KEY_NAME));
  }

  @ParameterizedTest
  @EnumSource(
      value = StatusCode.Code.class,
      names = {"UNAUTHENTICATED", "PERMISSION_DENIED"})
  void mapsRejectedCredentialsToAuthenticationFailures(StatusCode.Code code) {
    ApiException failure = apiException(code);
    when(sdkClient.getCryptoKey(KEY_NAME)).thenThrow(failure);

    assertThrows(KmsAuthenticationException.class, () -> metadataService.getKey(KEY_NAME));
  }

  @Test
  void mapsEmptyAndUnexpectedResponsesToConnectionFailures() {
    when(sdkClient.getCryptoKey(KEY_NAME)).thenReturn(null);
    assertThrows(ConnectionFailedException.class, () -> metadataService.getKey(KEY_NAME));

    when(sdkClient.getCryptoKey(KEY_NAME)).thenThrow(new IllegalStateException("closed transport"));
    assertThrows(ConnectionFailedException.class, () -> metadataService.getKey(KEY_NAME));
  }

  @Test
  void closesSdkClient() {
    metadataService.close();

    verify(sdkClient).close();
  }

  @Test
  void rejectsNullSdkClient() {
    assertThrows(
        IllegalArgumentException.class, () -> new GoogleCloudKmsSdkMetadataService(SOURCE, null));
  }

  private static CryptoKey key(CryptoKeyPurpose purpose, CryptoKeyVersionState state) {
    return CryptoKey.newBuilder()
        .setPurpose(purpose)
        .setPrimary(CryptoKeyVersion.newBuilder().setState(state).build())
        .build();
  }

  private static ApiException apiException(StatusCode.Code code) {
    StatusCode statusCode = mock(StatusCode.class);
    when(statusCode.getCode()).thenReturn(code);
    return ApiExceptionFactory.createException(
        new RuntimeException(code.name()), statusCode, false);
  }
}
