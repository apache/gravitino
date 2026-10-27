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
package org.apache.gravitino.iceberg.common.ops;

import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogWrapperFetcher {

  @AfterEach
  void tearDown() {
    IcebergCatalogWrapperFetcher.unbindAuxLookup();
  }

  @Test
  void createUsesLocalWhenUnbound() {
    IcebergCatalogWrapperFetcher fetcher = IcebergCatalogWrapperFetcher.create();
    Assertions.assertFalse(fetcher.isInternal());
    IcebergCatalogWrapper loaded = fetcher.load("c1", new IcebergConfig());
    Assertions.assertNotNull(loaded);
  }

  @Test
  void createUsesInternalWhenBound() {
    IcebergCatalogWrapper shared = new IcebergCatalogWrapper(new IcebergConfig());
    IcebergCatalogWrapperFetcher.bindAuxLookup(
        name -> {
          if ("c1".equals(name)) {
            return shared;
          }
          throw new NoSuchCatalogException("missing %s", name);
        });

    IcebergCatalogWrapperFetcher fetcher = IcebergCatalogWrapperFetcher.create();
    Assertions.assertTrue(fetcher.isInternal());
    Assertions.assertSame(shared, fetcher.load("c1", new IcebergConfig()));
  }

  @Test
  void internalFallsBackToLocalOnMissingCatalog() {
    IcebergCatalogWrapperFetcher.bindAuxLookup(
        name -> {
          throw new NoSuchCatalogException("missing %s", name);
        });
    IcebergCatalogWrapperFetcher fetcher = IcebergCatalogWrapperFetcher.create();
    Assertions.assertTrue(fetcher.isInternal());
    IcebergCatalogWrapper loaded = fetcher.load("unknown", new IcebergConfig());
    Assertions.assertNotNull(loaded);
  }

  @Test
  void unbindReturnsToLocal() {
    IcebergCatalogWrapperFetcher.bindAuxLookup(
        name -> new IcebergCatalogWrapper(new IcebergConfig()));
    IcebergCatalogWrapperFetcher.unbindAuxLookup();
    Assertions.assertFalse(IcebergCatalogWrapperFetcher.create().isInternal());
  }
}
