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

package org.apache.gravitino.catalog.fluss;

import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.apache.gravitino.connector.BaseCatalogPropertiesMetadata.PROPERTY_METALAKE_IN_USE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.junit.jupiter.api.Test;

class TestFlussPropertiesMetadata {

  @Test
  void testCatalogPropertiesMetadata() {
    PropertiesMetadata metadata = new FlussCatalogPropertiesMetadata();

    assertTrue(metadata.containsProperty(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS));
    assertTrue(metadata.isRequiredProperty(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS));
    assertFalse(metadata.isImmutableProperty(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS));
    assertFalse(metadata.isHiddenProperty(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS));

    assertTrue(metadata.containsProperty(ID_KEY));
    assertTrue(metadata.isReservedProperty(Catalog.PROPERTY_IN_USE));
    assertTrue(metadata.isHiddenProperty(PROPERTY_METALAKE_IN_USE));

    assertThrows(
        IllegalArgumentException.class, () -> validatePropertyForCreate(metadata, Map.of()));
    assertDoesNotThrow(
        () ->
            validatePropertyForCreate(
                metadata,
                Map.of(
                    FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS,
                    "localhost:9123",
                    CATALOG_BYPASS_PREFIX + "client.request.timeout",
                    "1 min")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            validatePropertyForCreate(
                metadata,
                Map.of(
                    FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS,
                    "localhost:9123",
                    ID_KEY,
                    "1")));
  }

  @Test
  void testSchemaPropertiesMetadata() {
    PropertiesMetadata metadata = new FlussSchemaPropertiesMetadata();

    assertFalse(metadata.containsProperty("schema.custom"));
    assertTrue(metadata.containsProperty(ID_KEY));
    assertTrue(metadata.isReservedProperty(ID_KEY));
    assertDoesNotThrow(() -> validatePropertyForCreate(metadata, Map.of("schema.custom", "v")));
    assertThrows(
        IllegalArgumentException.class,
        () -> validatePropertyForCreate(metadata, Map.of(ID_KEY, "1")));
  }

  @Test
  void testTablePropertiesMetadata() {
    PropertiesMetadata metadata = new FlussTablePropertiesMetadata();

    assertTrue(metadata.containsProperty(FlussTablePropertiesMetadata.TABLE_PROPERTY_PREFIX));
    assertTrue(metadata.containsProperty("table.log.ttl"));
    assertFalse(metadata.isRequiredProperty("table.log.ttl"));
    assertFalse(metadata.isReservedProperty("table.log.ttl"));
    assertTrue(metadata.containsProperty(ID_KEY));
    assertTrue(metadata.isReservedProperty(ID_KEY));

    assertDoesNotThrow(() -> validatePropertyForCreate(metadata, Map.of("table.log.ttl", "1d")));
    assertThrows(
        IllegalArgumentException.class,
        () -> validatePropertyForCreate(metadata, Map.of(ID_KEY, "1")));
  }
}
