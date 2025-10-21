/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.common.cache;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestLocalMetadataCache {
  private LocalTableMetadataCache cache;
  private TableIdentifier testIdentifier;
  private TableMetadata testMetadata;
  private SupportsMetadataLocation supportsMetadataLocation;

  @BeforeEach
  void setUp() {
    cache = new LocalTableMetadataCache();
    testIdentifier = TableIdentifier.of("test", "table");
    testMetadata = mock(TableMetadata.class);
    when(testMetadata.metadataFileLocation()).thenReturn("test_location_" + testIdentifier.name());
    supportsMetadataLocation = mock(SupportsMetadataLocation.class);
    when(supportsMetadataLocation.metadataLocation(testIdentifier))
        .thenReturn("test_location_" + testIdentifier.name());

    cache.initialize(10, 60, Collections.emptyMap(), supportsMetadataLocation);
  }

  @AfterEach
  void tearDown() throws IOException {
    cache.close();
  }

  @Test
  void testUpdateAndGetTableMetadata() {
    Assertions.assertNull(cache.getTableMetadata(testIdentifier).orElse(null));
    cache.updateTableMetadata(testIdentifier, testMetadata);
    Assertions.assertEquals(testMetadata, cache.getTableMetadata(testIdentifier).orElse(null));

    TableMetadata oldMetadata = mock(TableMetadata.class);
    cache.updateTableMetadata(testIdentifier, oldMetadata);
    Assertions.assertEquals(oldMetadata, cache.doGetTableMetadata(testIdentifier).orElse(null));
    Assertions.assertNull(cache.getTableMetadata(testIdentifier).orElse(null));
  }

  @Test
  void testInvalidate() {
    cache.updateTableMetadata(testIdentifier, testMetadata);
    Assertions.assertEquals(testMetadata, cache.getTableMetadata(testIdentifier).orElse(null));

    cache.invalidate(testIdentifier);
    Assertions.assertNull(cache.doGetTableMetadata(testIdentifier).orElse(null));
    Assertions.assertNull(cache.getTableMetadata(testIdentifier).orElse(null));
  }

  @Test
  void testClose() throws IOException {
    cache.updateTableMetadata(testIdentifier, testMetadata);
    cache.close();
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testCacheCapacity() {
    for (int i = 0; i < 15; i++) {
      TableIdentifier id = TableIdentifier.of("test", "table" + i);
      TableMetadata metadata = mock(TableMetadata.class);
      cache.updateTableMetadata(id, metadata);
    }
    Assertions.assertEquals(10, cache.size());
  }

  @Test
  void testCloseWithNullCache() throws IOException {
    LocalTableMetadataCache emptyCache = new LocalTableMetadataCache();
    emptyCache.close(); // Should not throw exception
  }
}
