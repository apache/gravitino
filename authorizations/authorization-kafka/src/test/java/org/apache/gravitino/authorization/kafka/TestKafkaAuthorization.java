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
package org.apache.gravitino.authorization.kafka;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.AuthorizationProvider;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKafkaAuthorization {

  @Test
  public void testServiceLoaderDiscoveryAndProviderValidation() throws IOException {
    ServiceLoader<AuthorizationProvider> loader = ServiceLoader.load(AuthorizationProvider.class);
    AuthorizationProvider provider =
        StreamSupport.stream(loader.spliterator(), false)
            .filter(p -> KafkaAuthorization.SHORT_NAME.equals(p.shortName()))
            .findFirst()
            .orElse(null);

    Assertions.assertNotNull(provider, "Expected KafkaAuthorization to be registered via SPI");
    Assertions.assertInstanceOf(KafkaAuthorization.class, provider);
    BaseAuthorization<?> kafkaAuthorization = (BaseAuthorization<?>) provider;

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            kafkaAuthorization.newPlugin(
                "test_metalake", "hive", ImmutableMap.of("bootstrap.servers", "localhost:9092")));

    try (AuthorizationPlugin plugin =
        kafkaAuthorization.newPlugin(
            "test_metalake", "kafka", ImmutableMap.of("bootstrap.servers", "localhost:19092"))) {
      Assertions.assertInstanceOf(KafkaAuthorizationPlugin.class, plugin);
    }
  }

  @Test
  public void testValidatePropertiesRejectsInvalidKafkaAndAdminConfig() {
    BaseAuthorization<?> kafkaAuthorization = new KafkaAuthorization();

    kafkaAuthorization.validateProperties(
        "test_metalake", "kafka", ImmutableMap.of("bootstrap.servers", "localhost:9092"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            kafkaAuthorization.validateProperties(
                "test_metalake",
                "kafka",
                ImmutableMap.of(
                    "bootstrap.servers",
                    "localhost:9092",
                    KafkaAuthorizationProperties.USER_PRINCIPAL_TYPE,
                    "*")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            kafkaAuthorization.validateProperties(
                "test_metalake",
                "kafka",
                ImmutableMap.of(
                    "bootstrap.servers",
                    "localhost:9092",
                    KafkaAuthorizationProperties.ADMIN_CLIENT_PREFIX + "config.providers",
                    "file")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            kafkaAuthorization.validateProperties(
                "test_metalake",
                "kafka",
                ImmutableMap.of(
                    "bootstrap.servers",
                    "localhost:9092",
                    KafkaAuthorizationProperties.ADMIN_CLIENT_PREFIX + "security.protocol",
                    "INVALID_PROTOCOL")));
  }
}
