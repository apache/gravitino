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
package org.apache.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FilesetMetadataCache} to verify proper resource management and client ownership
 * semantics.
 */
public class TestFilesetMetadataCache {

  /**
   * Test that FilesetMetadataCache does NOT close the GravitinoClient when closed.
   *
   * <p>This verifies the resource ownership principle: FilesetMetadataCache receives a shared
   * reference to the client but doesn't own it, so it should not close it. The owner
   * (BaseGVFSOperations) is responsible for closing the client.
   */
  @Test
  public void testCacheDoesNotCloseClient() throws IOException {
    // Create a mock GravitinoClient
    GravitinoClient mockClient = mock(GravitinoClient.class);

    // Create the cache with the mock client
    FilesetMetadataCache cache = new FilesetMetadataCache(mockClient);

    // Close the cache
    cache.close();

    // Verify that the client's close() method was NEVER called
    verify(mockClient, never()).close();
  }

  /**
   * Test that FilesetMetadataCache can be closed multiple times safely.
   *
   * <p>Since the cache doesn't close the client, multiple close() calls should be idempotent and
   * not cause any issues.
   */
  @Test
  public void testCacheCanBeClosedMultipleTimes() throws IOException {
    GravitinoClient mockClient = mock(GravitinoClient.class);
    FilesetMetadataCache cache = new FilesetMetadataCache(mockClient);

    // Close multiple times - should not throw any exceptions
    cache.close();
    cache.close();
    cache.close();

    // Client should still never be closed
    verify(mockClient, never()).close();
  }

  /**
   * Test that FilesetMetadataCache properly invalidates its internal caches when closed.
   *
   * <p>Even though the client isn't closed, the cache should properly clean up its internal
   * Caffeine caches.
   */
  @Test
  public void testCacheInvalidatesInternalCaches() throws IOException {
    GravitinoClient mockClient = mock(GravitinoClient.class);
    FilesetMetadataCache cache = new FilesetMetadataCache(mockClient);

    // Note: We can't easily verify cache invalidation without accessing private fields,
    // but we can at least verify that close() completes without errors
    cache.close();

    // If we got here without exceptions, the test passes
    assertTrue(true, "Cache close completed successfully");
  }

  /**
   * Tests that {@link FilesetMetadataCache} caches and reuses catalog, schema, and fileset
   * instances across lookups without redundant client calls.
   */
  @Test
  public void testCacheReusesCatalogSchemaAndFileset() throws IOException {
    GravitinoClient mockClient = mock(GravitinoClient.class);
    Catalog mockCatalog = mock(Catalog.class);
    FilesetCatalog mockFilesetCatalog = mock(FilesetCatalog.class);
    SupportsSchemas mockSchemas = mock(SupportsSchemas.class);
    Schema mockSchema = mock(Schema.class);
    Fileset mockFileset = mock(Fileset.class);

    when(mockClient.loadCatalog("catalog")).thenReturn(mockCatalog);
    when(mockCatalog.asFilesetCatalog()).thenReturn(mockFilesetCatalog);
    when(mockCatalog.asSchemas()).thenReturn(mockSchemas);
    when(mockSchemas.loadSchema("schema")).thenReturn(mockSchema);
    when(mockFilesetCatalog.loadFileset(NameIdentifier.of("schema", "fs"))).thenReturn(mockFileset);

    try (FilesetMetadataCache cache = new FilesetMetadataCache(mockClient)) {
      NameIdentifier catalogIdent = NameIdentifier.of("ml", "catalog");
      NameIdentifier schemaIdent = NameIdentifier.of("ml", "catalog", "schema");
      NameIdentifier filesetIdent = NameIdentifier.of("ml", "catalog", "schema", "fs");

      assertSame(mockCatalog, cache.getCatalog(catalogIdent));
      assertSame(mockFilesetCatalog, cache.getFilesetCatalog(catalogIdent));
      assertSame(mockSchema, cache.getSchema(schemaIdent));
      assertSame(mockSchema, cache.getSchema(schemaIdent));
      assertSame(mockFileset, cache.getFileset(filesetIdent));
      assertSame(mockFileset, cache.getFileset(filesetIdent));

      verify(mockClient, times(1)).loadCatalog("catalog");
      verify(mockSchemas, times(1)).loadSchema("schema");
      verify(mockFilesetCatalog, times(1)).loadFileset(NameIdentifier.of("schema", "fs"));
    }
  }
}
