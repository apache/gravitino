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

package org.apache.gravitino.flink.connector.iceberg;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.junit.jupiter.api.Test;

class TestGravitinoIcebergCatalog {

  @Test
  void testRequireAbstractCatalogAcceptsAbstractCatalog() {
    AbstractCatalog catalog = mock(AbstractCatalog.class);

    assertSame(catalog, GravitinoIcebergCatalog.requireAbstractCatalog(catalog));
  }

  @Test
  void testRequireAbstractCatalogRejectsNonAbstractCatalog() {
    Catalog catalog = mock(Catalog.class);

    assertThrows(
        IllegalStateException.class, () -> GravitinoIcebergCatalog.requireAbstractCatalog(catalog));
  }
}
