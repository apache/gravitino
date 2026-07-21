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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.TestKmsClientFactoryContract;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests AWS KMS factory validation, initialization, and service discovery. */
public class TestAwsKmsClientFactory extends TestKmsClientFactoryContract {

  private static final String SOURCE = "primary-aws";

  private final AwsKmsClientFactory factory = new AwsKmsClientFactory();

  @Override
  protected KmsClientFactory factory() {
    return factory;
  }

  @Override
  protected KmsApi expectedApi() {
    return KmsApi.AWS_KMS;
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void testRejectsBlankSource(String source) {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(source, validProperties()));
  }

  @Test
  void testRejectsNullProperties() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create(SOURCE, null));
  }

  @Test
  void testRejectsMissingRegion() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> factory.create(SOURCE, Collections.<String, String>emptyMap()));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void testRejectsBlankRegion(String region) {
    Map<String, String> properties = new HashMap<>();
    properties.put(AwsKmsClientFactory.REGION, region);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(SOURCE, properties));
  }

  @Test
  void testRejectsUnknownProperty() {
    Map<String, String> properties = validProperties();
    properties.put("accessKeyId", "must-not-be-configured-here");

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(SOURCE, properties));
  }

  @Test
  void testRejectsNullPropertyName() {
    Map<String, String> properties = validProperties();
    properties.put(null, "value");

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(SOURCE, properties));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "ftp://localhost:4566", "http:///missing-host", "not a uri"})
  void testRejectsInvalidServiceAddress(String serviceAddress) {
    Map<String, String> properties = validProperties();
    properties.put(AwsKmsClientFactory.SERVICE_ADDRESS, serviceAddress);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(SOURCE, properties));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://user@localhost:4566",
        "http://localhost:4566/path",
        "http://localhost:4566?query=value",
        "http://localhost:4566#fragment"
      })
  void testRejectsServiceAddressThatIsNotAnOrigin(String serviceAddress) {
    Map<String, String> properties = validProperties();
    properties.put(AwsKmsClientFactory.SERVICE_ADDRESS, serviceAddress);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(SOURCE, properties));
  }

  @Test
  void testCreatesClientWithoutResolvingCredentialsOrContactingAws() {
    try (KmsClient client = factory.create(SOURCE, validProperties())) {
      Assertions.assertNotNull(client);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://localhost:4566",
        "http://localhost:4566/",
        "https://kms.us-west-2.amazonaws.com"
      })
  void testCreatesClientWithServiceAddress(String serviceAddress) {
    Map<String, String> properties = validProperties();
    properties.put(AwsKmsClientFactory.SERVICE_ADDRESS, serviceAddress);

    try (KmsClient client = factory.create(SOURCE, properties)) {
      Assertions.assertNotNull(client);
    }
  }

  @Test
  void testFactoryIsDiscoverableWithServiceLoader() {
    boolean discovered = false;
    for (KmsClientFactory candidate : ServiceLoader.load(KmsClientFactory.class)) {
      if (candidate.getClass() == AwsKmsClientFactory.class) {
        discovered = true;
      }
    }

    Assertions.assertTrue(discovered);
  }

  private static Map<String, String> validProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(AwsKmsClientFactory.REGION, "us-west-2");
    properties.put(AwsKmsClientFactory.CREDENTIAL_METHOD, "default");
    return properties;
  }
}
