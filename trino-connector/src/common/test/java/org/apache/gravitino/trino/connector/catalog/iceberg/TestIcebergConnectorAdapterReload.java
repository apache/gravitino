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
package org.apache.gravitino.trino.connector.catalog.iceberg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;

public class TestIcebergConnectorAdapterReload {

  private static GravitinoConfig configFor(String metalake, String discoveredUri) {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090", "gravitino.metalake", metalake));
    if (discoveredUri != null) {
      config.setDiscoveredIcebergRestUri(metalake, discoveredUri);
    }
    return config;
  }

  private static GravitinoCatalog icebergCatalog(String embeddedUri) {
    ImmutableMap<String, String> properties =
        embeddedUri == null
            ? ImmutableMap.of()
            : ImmutableMap.of("__gravitino.iceberg.rest-uri", embeddedUri);
    return new GravitinoCatalog("test", "lakehouse-iceberg", "iceberg_catalog", properties, 5L);
  }

  @Test
  public void testUnchangedWhenDiscoveredUriMatchesEmbeddedUri() {
    GravitinoConfig config = configFor("test", "http://irc-host:9001/iceberg");
    GravitinoCatalog freshCatalog = icebergCatalog(null);
    GravitinoCatalog registeredCatalog = icebergCatalog("http://irc-host:9001/iceberg");

    assertFalse(
        IcebergConnectorAdapter.hasDiscoveredIcebergRestUriChanged(
            freshCatalog, registeredCatalog, config));
  }

  @Test
  public void testChangedWhenIcebergRestBecomesAvailable() {
    GravitinoConfig config = configFor("test", "http://irc-host:9001/iceberg");
    GravitinoCatalog freshCatalog = icebergCatalog(null);
    GravitinoCatalog registeredCatalog = icebergCatalog(null);

    assertTrue(
        IcebergConnectorAdapter.hasDiscoveredIcebergRestUriChanged(
            freshCatalog, registeredCatalog, config));
  }

  @Test
  public void testChangedWhenIcebergRestBecomesUnavailable() {
    GravitinoConfig config = configFor("test", null);
    GravitinoCatalog freshCatalog = icebergCatalog(null);
    GravitinoCatalog registeredCatalog = icebergCatalog("http://irc-host:9001/iceberg");

    assertTrue(
        IcebergConnectorAdapter.hasDiscoveredIcebergRestUriChanged(
            freshCatalog, registeredCatalog, config));
  }

  @Test
  public void testNonIcebergCatalogNeverReportsChanged() {
    GravitinoConfig config = configFor("test", "http://irc-host:9001/iceberg");
    GravitinoCatalog freshCatalog =
        new GravitinoCatalog("test", "hive", "hive_catalog", ImmutableMap.of(), 5L);
    GravitinoCatalog registeredCatalog =
        new GravitinoCatalog("test", "hive", "hive_catalog", ImmutableMap.of(), 5L);

    assertFalse(
        IcebergConnectorAdapter.hasDiscoveredIcebergRestUriChanged(
            freshCatalog, registeredCatalog, config));
  }
}
