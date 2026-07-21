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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.encryption.kms.TestKmsClientContract;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestGoogleCloudKmsClient extends TestKmsClientContract {

  private static final String SOURCE = "analytics";
  private static final String PROJECT_ID = "data-project";
  private static final String LOCATION = "us-west1";
  private static final String USABLE_KEY =
      "projects/data-project/locations/us-west1/keyRings/tables/cryptoKeys/customer";
  private static final String MISSING_KEY =
      "projects/data-project/locations/us-west1/keyRings/tables/cryptoKeys/missing";

  private final FakeMetadataService metadataService = new FakeMetadataService();

  private GoogleCloudKmsClient client;

  @BeforeEach
  void setUp() {
    metadataService.keys.put(USABLE_KEY, new GoogleCloudKmsKeyMetadata(true, true));
    client = new GoogleCloudKmsClient(SOURCE, PROJECT_ID, LOCATION, metadataService);
  }

  @Override
  protected KmsClient client() {
    return client;
  }

  @Override
  protected KmsReference usableKey() {
    return reference(USABLE_KEY);
  }

  @Override
  protected KmsReference missingKey() {
    return reference(MISSING_KEY);
  }

  @Test
  void readsExactlyOneCryptoKeyResource() {
    client.getKeyProperties(usableKey());

    assertEquals(1, metadataService.readCount.get());
    assertEquals(USABLE_KEY, metadataService.lastResourceName.get());
  }

  @Test
  void reportsDisabledPrimaryVersion() {
    metadataService.keys.put(USABLE_KEY, new GoogleCloudKmsKeyMetadata(true, false));

    KmsKeyProperties properties = client.getKeyProperties(usableKey());

    assertTrue(properties.present());
    assertFalse(properties.enabled());
    assertTrue(properties.supportsWrapping());
    assertTrue(properties.supportsUnwrapping());
  }

  @Test
  void reportsNonEncryptionPurpose() {
    metadataService.keys.put(USABLE_KEY, new GoogleCloudKmsKeyMetadata(false, true));

    KmsKeyProperties properties = client.getKeyProperties(usableKey());

    assertTrue(properties.present());
    assertTrue(properties.enabled());
    assertFalse(properties.supportsWrapping());
    assertFalse(properties.supportsUnwrapping());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "customer",
        "projects/p/locations/l/keyRings/r/cryptoKeys",
        "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1",
        "projects//locations/l/keyRings/r/cryptoKeys/k",
        "projects/p/locations//keyRings/r/cryptoKeys/k",
        "projects/p/locations/l/keyRings//cryptoKeys/k",
        "projects/p/locations/l/keyRings/r/cryptoKeys/",
        "https://cloudkms.googleapis.com/v1/projects/p/locations/l/keyRings/r/cryptoKeys/k"
      })
  void rejectsInvalidCryptoKeyResourceNames(String keyId) {
    assertThrows(IllegalArgumentException.class, () -> client.getKeyProperties(reference(keyId)));
    assertEquals(0, metadataService.readCount.get());
  }

  @Test
  void preservesConnectionFailures() {
    ConnectionFailedException failure =
        new ConnectionFailedException("Google Cloud KMS is unavailable");
    metadataService.failure = failure;

    ConnectionFailedException exception =
        assertThrows(ConnectionFailedException.class, () -> client.getKeyProperties(usableKey()));

    assertEquals(failure, exception);
  }

  @Test
  void rejectsBrokenMetadataServiceContract() {
    metadataService.returnNull = true;

    assertThrows(IllegalStateException.class, () -> client.getKeyProperties(usableKey()));
  }

  @Test
  void closesMetadataServiceOnce() {
    client.close();
    client.close();

    assertEquals(1, metadataService.closeCount.get());
  }

  @Test
  void rejectsInvalidConstruction() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudKmsClient(" ", PROJECT_ID, LOCATION, metadataService));
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudKmsClient(SOURCE, PROJECT_ID, LOCATION, null));
  }

  private KmsReference reference(String keyId) {
    return new KmsReference(KmsApi.GOOGLE_CLOUD_KMS, SOURCE, keyId);
  }

  private static final class FakeMetadataService implements GoogleCloudKmsMetadataService {
    private final Map<String, GoogleCloudKmsKeyMetadata> keys = new HashMap<>();
    private final AtomicInteger readCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private final AtomicReference<String> lastResourceName = new AtomicReference<>();

    private ConnectionFailedException failure;
    private boolean returnNull;

    @Override
    public Optional<GoogleCloudKmsKeyMetadata> getKey(String resourceName) {
      readCount.incrementAndGet();
      lastResourceName.set(resourceName);
      if (failure != null) {
        throw failure;
      }
      if (returnNull) {
        return null;
      }
      return Optional.ofNullable(keys.get(resourceName));
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }
  }
}
