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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.encryption.kms.TestKmsClientFactoryContract;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestGoogleCloudKmsClientFactory extends TestKmsClientFactoryContract {

  private static final String SOURCE = "analytics";
  private static final String KEY_NAME =
      "projects/data-project/locations/us/keyRings/tables/cryptoKeys/customer";

  private final AtomicReference<String> createdSource = new AtomicReference<>();
  private final GoogleCloudKmsClientFactory factory =
      new GoogleCloudKmsClientFactory(
          source -> {
            createdSource.set(source);
            return resourceName -> Optional.of(new GoogleCloudKmsKeyMetadata(true, true));
          });

  @Override
  protected KmsClientFactory factory() {
    return factory;
  }

  @Override
  protected KmsApi expectedApi() {
    return KmsApi.GOOGLE_CLOUD_KMS;
  }

  @Test
  void createsWorkingClientWithDefaultCredentials() {
    KmsClient client = factory.create(SOURCE, validProperties());
    KmsReference reference = new KmsReference(KmsApi.GOOGLE_CLOUD_KMS, SOURCE, KEY_NAME);

    KmsKeyProperties properties = client.getKeyProperties(reference);

    assertEquals(reference, properties.reference());
    assertTrue(properties.present());
    assertEquals(SOURCE, createdSource.get());
  }

  @Test
  void rejectsMissingOrUnknownConfiguration() {
    assertThrows(IllegalArgumentException.class, () -> factory.create(SOURCE, null));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> factory.create(SOURCE, Map.of("credentialsFile", "/tmp/credentials.json")));
    assertTrue(exception.getMessage().contains("credentialsFile"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\t"})
  void rejectsBlankSource(String source) {
    assertThrows(IllegalArgumentException.class, () -> factory.create(source, validProperties()));
  }

  @Test
  void preservesInitializationFailure() {
    ConnectionFailedException failure =
        new ConnectionFailedException("Application Default Credentials are unavailable");
    GoogleCloudKmsClientFactory failingFactory =
        new GoogleCloudKmsClientFactory(
            source -> {
              throw failure;
            });

    ConnectionFailedException exception =
        assertThrows(
            ConnectionFailedException.class,
            () -> failingFactory.create(SOURCE, validProperties()));

    assertEquals(failure, exception);
  }

  @Test
  void rejectsBrokenMetadataServiceFactoryContract() {
    GoogleCloudKmsClientFactory brokenFactory = new GoogleCloudKmsClientFactory(source -> null);

    assertThrows(
        IllegalStateException.class, () -> brokenFactory.create(SOURCE, validProperties()));
    assertThrows(IllegalArgumentException.class, () -> new GoogleCloudKmsClientFactory(null));
  }

  @Test
  void serviceLoaderDiscoversFactory() {
    KmsClientFactory discovered = null;
    for (KmsClientFactory candidate : ServiceLoader.load(KmsClientFactory.class)) {
      if (candidate.api() == KmsApi.GOOGLE_CLOUD_KMS) {
        discovered = candidate;
        break;
      }
    }

    assertInstanceOf(GoogleCloudKmsClientFactory.class, discovered);
  }

  private static Map<String, String> validProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(GoogleCloudKmsClientFactory.PROJECT_ID, "data-project");
    properties.put(GoogleCloudKmsClientFactory.LOCATION, "us");
    properties.put(GoogleCloudKmsClientFactory.CREDENTIAL_METHOD, "default");
    return properties;
  }
}
