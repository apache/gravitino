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

package org.apache.gravitino.catalog.doris;

import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.BLOOM_FILTER_COLUMNS;
import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.COMPRESSION;
import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE;
import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.LIGHT_SCHEMA_CHANGE;
import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.REPLICATION_FACTOR;
import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.STORAGE_POLICY;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDorisCatalog {

  @Test
  void testDorisTablePropertiesMetadata() {
    DorisTablePropertiesMetadata dorisTablePropertiesMetadata = new DorisTablePropertiesMetadata();
    Map<String, PropertyEntry<?>> propertyEntryMap =
        dorisTablePropertiesMetadata.specificPropertyEntries();

    // Verify the total number of registered properties.
    // 6 = 1 existing (replication_num) + 5 new (compression, bloom_filter_columns,
    // storage_policy, light_schema_change, enable_unique_key_merge_on_write).
    Assertions.assertEquals(6, propertyEntryMap.size());

    // ---- replication_num (integerOptional) ----
    Assertions.assertTrue(propertyEntryMap.containsKey(REPLICATION_FACTOR));
    PropertyEntry<?> replication = propertyEntryMap.get(REPLICATION_FACTOR);
    Assertions.assertEquals(REPLICATION_FACTOR, replication.getName());
    Assertions.assertFalse(replication.isRequired());
    Assertions.assertFalse(replication.isImmutable());
    Assertions.assertFalse(replication.isReserved());
    Assertions.assertFalse(replication.isHidden());
    Assertions.assertEquals(Integer.class, replication.getJavaType());
    Assertions.assertEquals(
        DorisTablePropertiesMetadata.DEFAULT_REPLICATION_FACTOR, replication.getDefaultValue());

    // ---- compression (stringOptional) ----
    Assertions.assertTrue(propertyEntryMap.containsKey(COMPRESSION));
    PropertyEntry<?> compression = propertyEntryMap.get(COMPRESSION);
    Assertions.assertEquals(COMPRESSION, compression.getName());
    Assertions.assertFalse(compression.isRequired());
    Assertions.assertFalse(compression.isImmutable());
    Assertions.assertFalse(compression.isReserved());
    Assertions.assertFalse(compression.isHidden());
    Assertions.assertEquals(String.class, compression.getJavaType());
    Assertions.assertNull(compression.getDefaultValue());

    // ---- bloom_filter_columns (stringOptional) ----
    Assertions.assertTrue(propertyEntryMap.containsKey(BLOOM_FILTER_COLUMNS));
    PropertyEntry<?> bloomFilter = propertyEntryMap.get(BLOOM_FILTER_COLUMNS);
    Assertions.assertEquals(BLOOM_FILTER_COLUMNS, bloomFilter.getName());
    Assertions.assertFalse(bloomFilter.isRequired());
    Assertions.assertFalse(bloomFilter.isImmutable());
    Assertions.assertFalse(bloomFilter.isReserved());
    Assertions.assertFalse(bloomFilter.isHidden());
    Assertions.assertEquals(String.class, bloomFilter.getJavaType());
    Assertions.assertNull(bloomFilter.getDefaultValue());

    // ---- storage_policy (stringOptional) ----
    Assertions.assertTrue(propertyEntryMap.containsKey(STORAGE_POLICY));
    PropertyEntry<?> storagePolicy = propertyEntryMap.get(STORAGE_POLICY);
    Assertions.assertEquals(STORAGE_POLICY, storagePolicy.getName());
    Assertions.assertFalse(storagePolicy.isRequired());
    Assertions.assertFalse(storagePolicy.isImmutable());
    Assertions.assertFalse(storagePolicy.isReserved());
    Assertions.assertFalse(storagePolicy.isHidden());
    Assertions.assertEquals(String.class, storagePolicy.getJavaType());
    Assertions.assertNull(storagePolicy.getDefaultValue());

    // ---- light_schema_change (stringReserved) ----
    Assertions.assertTrue(propertyEntryMap.containsKey(LIGHT_SCHEMA_CHANGE));
    PropertyEntry<?> lightSchemaChange = propertyEntryMap.get(LIGHT_SCHEMA_CHANGE);
    Assertions.assertEquals(LIGHT_SCHEMA_CHANGE, lightSchemaChange.getName());
    Assertions.assertFalse(lightSchemaChange.isRequired());
    Assertions.assertTrue(lightSchemaChange.isImmutable());
    Assertions.assertTrue(lightSchemaChange.isReserved());
    Assertions.assertFalse(lightSchemaChange.isHidden());
    Assertions.assertEquals(String.class, lightSchemaChange.getJavaType());

    // ---- enable_unique_key_merge_on_write (stringReserved) ----
    Assertions.assertTrue(propertyEntryMap.containsKey(ENABLE_UNIQUE_KEY_MERGE_ON_WRITE));
    PropertyEntry<?> mergeOnWrite = propertyEntryMap.get(ENABLE_UNIQUE_KEY_MERGE_ON_WRITE);
    Assertions.assertEquals(ENABLE_UNIQUE_KEY_MERGE_ON_WRITE, mergeOnWrite.getName());
    Assertions.assertFalse(mergeOnWrite.isRequired());
    Assertions.assertTrue(mergeOnWrite.isImmutable());
    Assertions.assertTrue(mergeOnWrite.isReserved());
    Assertions.assertFalse(mergeOnWrite.isHidden());
    Assertions.assertEquals(String.class, mergeOnWrite.getJavaType());
  }

  @Test
  void testReservedPropertiesRejectedOnCreate() {
    // Verify that reserved (read-only) properties are rejected when passed to createTable.
    DorisTablePropertiesMetadata metadata = new DorisTablePropertiesMetadata();

    // light_schema_change is a reserved property — setting it should be rejected.
    Map<String, String> props = new HashMap<>();
    props.put(LIGHT_SCHEMA_CHANGE, "false");
    assertThrows(
        IllegalArgumentException.class,
        () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, props));

    // enable_unique_key_merge_on_write is also reserved.
    Map<String, String> props2 = new HashMap<>();
    props2.put(ENABLE_UNIQUE_KEY_MERGE_ON_WRITE, "false");
    assertThrows(
        IllegalArgumentException.class,
        () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, props2));

    // Reserved properties mixed with writable properties should also be rejected.
    Map<String, String> props3 = new HashMap<>();
    props3.put(BLOOM_FILTER_COLUMNS, "col1,col2");
    props3.put(LIGHT_SCHEMA_CHANGE, "true");
    assertThrows(
        IllegalArgumentException.class,
        () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, props3));

    // Writable properties alone should pass validation.
    Map<String, String> props4 = new HashMap<>();
    props4.put(BLOOM_FILTER_COLUMNS, "col1,col2");
    props4.put(COMPRESSION, "ZSTD");
    // Writable properties alone must pass validation.
    PropertiesMetadataHelpers.validatePropertyForCreate(metadata, props4);
  }
}
