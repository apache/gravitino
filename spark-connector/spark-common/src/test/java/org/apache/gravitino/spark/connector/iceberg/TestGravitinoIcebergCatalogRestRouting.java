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

package org.apache.gravitino.spark.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests Iceberg REST endpoint selection for Spark catalogs. */
public class TestGravitinoIcebergCatalogRestRouting {

  @Test
  void testRoutingDisabledSkipsDiscovery() {
    Map<String, String> sessionConfig =
        ImmutableMap.of(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED, "false");
    AtomicBoolean discoveryCalled = new AtomicBoolean();

    Optional<String> result =
        GravitinoIcebergCatalog.resolveIcebergRestUri(
            hiveProperties(),
            sessionConfig::get,
            () -> {
              discoveryCalled.set(true);
              return Optional.of("http://discovered/iceberg");
            });

    Assertions.assertFalse(result.isPresent());
    Assertions.assertFalse(discoveryCalled.get());
  }

  @Test
  void testManualUriTakesPrecedenceOverDiscovery() {
    Map<String, String> sessionConfig =
        ImmutableMap.of(
            GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI, "http://configured/iceberg");
    AtomicBoolean discoveryCalled = new AtomicBoolean();

    Optional<String> result =
        GravitinoIcebergCatalog.resolveIcebergRestUri(
            hiveProperties(),
            sessionConfig::get,
            () -> {
              discoveryCalled.set(true);
              return Optional.of("http://discovered/iceberg");
            });

    Assertions.assertEquals("http://configured/iceberg", result.get());
    Assertions.assertFalse(discoveryCalled.get());
  }

  @Test
  void testRoutingEnabledRequiresEndpoint() {
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                GravitinoIcebergCatalog.resolveIcebergRestUri(
                    hiveProperties(), key -> null, Optional::empty));

    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED));
  }

  @Test
  void testInvalidRoutingSettingFails() {
    Map<String, String> sessionConfig =
        ImmutableMap.of(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED, "yes");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoIcebergCatalog.resolveIcebergRestUri(
                    hiveProperties(), sessionConfig::get, Optional::empty));

    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED));
  }

  @Test
  void testDiscoveryFailureIncludesEscapeHatches() {
    RuntimeException discoveryFailure = new RuntimeException("connection refused");

    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                GravitinoIcebergCatalog.resolveIcebergRestUri(
                    hiveProperties(),
                    key -> null,
                    () -> {
                      throw discoveryFailure;
                    }));

    Assertions.assertSame(discoveryFailure, exception.getCause());
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED));
    Assertions.assertTrue(
        exception.getMessage().contains(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI));
  }

  @Test
  void testNonRoutableBackendSkipsDiscovery() {
    AtomicBoolean discoveryCalled = new AtomicBoolean();

    Optional<String> result =
        GravitinoIcebergCatalog.resolveIcebergRestUri(
            ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "rest"),
            key -> null,
            () -> {
              discoveryCalled.set(true);
              return Optional.of("http://discovered/iceberg");
            });

    Assertions.assertFalse(result.isPresent());
    Assertions.assertFalse(discoveryCalled.get());
  }

  private static ImmutableMap<String, String> hiveProperties() {
    return ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "hive");
  }
}
