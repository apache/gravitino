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

import java.util.stream.Stream;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsAuthenticationException;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.encryption.kms.TestKmsClientContract;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kms.model.DependencyTimeoutException;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.InvalidArnException;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyState;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.awssdk.services.kms.model.KmsInternalException;
import software.amazon.awssdk.services.kms.model.NotFoundException;

/** Tests AWS KMS key inspection and normalized property mapping. */
public class TestAwsKmsClient extends TestKmsClientContract {

  private static final String SOURCE = "primary-aws";
  private static final String USABLE_KEY_ID = "1234abcd-12ab-34cd-56ef-1234567890ab";
  private static final String MISSING_KEY_ID = "missing-key";

  private software.amazon.awssdk.services.kms.KmsClient delegate;
  private AwsKmsClient client;

  @BeforeEach
  void setUp() {
    delegate = Mockito.mock(software.amazon.awssdk.services.kms.KmsClient.class);
    client = new AwsKmsClient(SOURCE, delegate);
    Mockito.when(delegate.describeKey(Mockito.any(DescribeKeyRequest.class)))
        .thenAnswer(
            invocation -> {
              DescribeKeyRequest request = invocation.getArgument(0);
              if (MISSING_KEY_ID.equals(request.keyId())) {
                throw NotFoundException.builder().message("Key does not exist").build();
              }
              return response(
                  KeyMetadata.builder()
                      .enabled(true)
                      .keyState(KeyState.ENABLED)
                      .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
                      .build());
            });
  }

  @Override
  protected KmsClient client() {
    return client;
  }

  @Override
  protected KmsReference usableKey() {
    return reference(USABLE_KEY_ID);
  }

  @Override
  protected KmsReference missingKey() {
    return reference(MISSING_KEY_ID);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1234abcd-12ab-34cd-56ef-1234567890ab",
        "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
        "alias/application-key",
        "arn:aws:kms:us-west-2:111122223333:alias/application-key"
      })
  void testPassesProviderNativeKeyIdentifierWithoutModification(String keyId) {
    client.getKeyProperties(reference(keyId));

    ArgumentCaptor<DescribeKeyRequest> requestCaptor =
        ArgumentCaptor.forClass(DescribeKeyRequest.class);
    Mockito.verify(delegate).describeKey(requestCaptor.capture());
    Assertions.assertEquals(keyId, requestCaptor.getValue().keyId());
  }

  @ParameterizedTest
  @EnumSource(
      value = KeySpec.class,
      names = {"SYMMETRIC_DEFAULT", "RSA_2048"})
  void testSupportsSymmetricAndAsymmetricEncryptionKeys(KeySpec keySpec) {
    stubMetadata(
        KeyMetadata.builder()
            .enabled(true)
            .keyState(KeyState.ENABLED)
            .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
            .keySpec(keySpec)
            .build());

    KmsKeyProperties properties = usableKeyProperties();

    Assertions.assertTrue(properties.enabled());
    Assertions.assertTrue(properties.supportsWrapping());
    Assertions.assertTrue(properties.supportsUnwrapping());
  }

  @ParameterizedTest
  @EnumSource(
      value = KeyState.class,
      names = {
        "CREATING",
        "DISABLED",
        "PENDING_DELETION",
        "PENDING_IMPORT",
        "PENDING_REPLICA_DELETION",
        "UNAVAILABLE",
        "UPDATING",
        "UNKNOWN_TO_SDK_VERSION"
      })
  void testOnlyEnabledStateIsEnabled(KeyState keyState) {
    stubMetadata(
        KeyMetadata.builder()
            .enabled(true)
            .keyState(keyState)
            .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
            .build());

    KmsKeyProperties properties = usableKeyProperties();

    Assertions.assertTrue(properties.present());
    Assertions.assertFalse(properties.enabled());
    Assertions.assertTrue(properties.supportsWrapping());
    Assertions.assertTrue(properties.supportsUnwrapping());
  }

  @Test
  void testEnabledFlagMustAlsoBeTrue() {
    stubMetadata(
        KeyMetadata.builder()
            .enabled(false)
            .keyState(KeyState.ENABLED)
            .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
            .build());

    Assertions.assertFalse(usableKeyProperties().enabled());
  }

  @Test
  void testMissingStateIsNotEnabled() {
    stubMetadata(
        KeyMetadata.builder().enabled(true).keyUsage(KeyUsageType.ENCRYPT_DECRYPT).build());

    Assertions.assertFalse(usableKeyProperties().enabled());
  }

  @ParameterizedTest
  @EnumSource(
      value = KeyUsageType.class,
      names = {"SIGN_VERIFY", "GENERATE_VERIFY_MAC", "KEY_AGREEMENT", "UNKNOWN_TO_SDK_VERSION"})
  void testNonEncryptionUsageDoesNotSupportWrapping(KeyUsageType keyUsage) {
    stubMetadata(
        KeyMetadata.builder().enabled(true).keyState(KeyState.ENABLED).keyUsage(keyUsage).build());

    KmsKeyProperties properties = usableKeyProperties();

    Assertions.assertTrue(properties.enabled());
    Assertions.assertFalse(properties.supportsWrapping());
    Assertions.assertFalse(properties.supportsUnwrapping());
  }

  @Test
  void testMissingUsageDoesNotSupportWrapping() {
    stubMetadata(KeyMetadata.builder().enabled(true).keyState(KeyState.ENABLED).build());

    KmsKeyProperties properties = usableKeyProperties();

    Assertions.assertFalse(properties.supportsWrapping());
    Assertions.assertFalse(properties.supportsUnwrapping());
  }

  @Test
  void testUnknownProviderStateAndUsageAreConservative() {
    stubMetadata(
        KeyMetadata.builder()
            .enabled(true)
            .keyState("A_NEW_STATE")
            .keyUsage("A_NEW_USAGE")
            .build());

    KmsKeyProperties properties = usableKeyProperties();

    Assertions.assertFalse(properties.enabled());
    Assertions.assertFalse(properties.supportsWrapping());
    Assertions.assertFalse(properties.supportsUnwrapping());
  }

  @Test
  void testMissingKeyReturnsAllFalseProperties() {
    KmsReference reference = missingKey();

    KmsKeyProperties properties = client.getKeyProperties(reference);

    Assertions.assertEquals(reference, properties.reference());
    Assertions.assertFalse(properties.present());
    Assertions.assertFalse(properties.enabled());
    Assertions.assertFalse(properties.supportsWrapping());
    Assertions.assertFalse(properties.supportsUnwrapping());
  }

  @Test
  void testInvalidArnIsRejectedAsInvalidInput() {
    InvalidArnException failure = InvalidArnException.builder().message("Invalid ARN").build();
    Mockito.when(delegate.describeKey(Mockito.any(DescribeKeyRequest.class))).thenThrow(failure);

    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> client.getKeyProperties(usableKey()));

    Assertions.assertSame(failure, thrown.getCause());
  }

  @Test
  void testRejectedCredentialsAreAuthenticationFailures() {
    KmsException failure =
        (KmsException) KmsException.builder().message("access denied").statusCode(403).build();
    Mockito.when(delegate.describeKey(Mockito.any(DescribeKeyRequest.class))).thenThrow(failure);

    KmsAuthenticationException thrown =
        Assertions.assertThrows(
            KmsAuthenticationException.class, () -> client.getKeyProperties(usableKey()));

    Assertions.assertSame(failure, thrown.getCause());
  }

  @ParameterizedTest
  @MethodSource("connectionFailures")
  void testProviderAndTransportFailuresAreConnectionFailures(RuntimeException failure) {
    Mockito.when(delegate.describeKey(Mockito.any(DescribeKeyRequest.class))).thenThrow(failure);

    ConnectionFailedException thrown =
        Assertions.assertThrows(
            ConnectionFailedException.class, () -> client.getKeyProperties(usableKey()));

    Assertions.assertSame(failure, thrown.getCause());
  }

  @Test
  void testMissingResponseIsConnectionFailure() {
    Mockito.when(delegate.describeKey(Mockito.any(DescribeKeyRequest.class))).thenReturn(null);

    Assertions.assertThrows(
        ConnectionFailedException.class, () -> client.getKeyProperties(usableKey()));
  }

  @Test
  void testMissingMetadataIsConnectionFailure() {
    Mockito.when(delegate.describeKey(Mockito.any(DescribeKeyRequest.class)))
        .thenReturn(DescribeKeyResponse.builder().build());

    Assertions.assertThrows(
        ConnectionFailedException.class, () -> client.getKeyProperties(usableKey()));
  }

  @Test
  void testCloseDelegatesToAwsClient() {
    client.close();

    Mockito.verify(delegate).close();
  }

  private static Stream<Arguments> connectionFailures() {
    return Stream.of(
        Arguments.of(SdkClientException.create("transport unavailable")),
        Arguments.of(DependencyTimeoutException.builder().message("dependency timeout").build()),
        Arguments.of(KmsInternalException.builder().message("internal failure").build()),
        Arguments.of(KmsException.builder().message("throttled").statusCode(429).build()));
  }

  private static DescribeKeyResponse response(KeyMetadata metadata) {
    return DescribeKeyResponse.builder().keyMetadata(metadata).build();
  }

  private KmsReference reference(String keyId) {
    return new KmsReference(KmsApi.AWS_KMS, SOURCE, keyId);
  }

  private KmsKeyProperties usableKeyProperties() {
    return client.getKeyProperties(usableKey());
  }

  private void stubMetadata(KeyMetadata metadata) {
    Mockito.when(delegate.describeKey(Mockito.any(DescribeKeyRequest.class)))
        .thenReturn(response(metadata));
  }
}
