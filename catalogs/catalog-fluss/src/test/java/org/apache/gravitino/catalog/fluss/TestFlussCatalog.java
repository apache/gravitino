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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;
import org.apache.gravitino.connector.CatalogOperations;
import org.junit.jupiter.api.Test;

class TestFlussCatalog {

  @Test
  void testShortNameAndPropertiesMetadata() {
    FlussCatalog catalog = new FlussCatalog();

    assertEquals("fluss", catalog.shortName());
    assertInstanceOf(FlussCatalogCapability.class, catalog.capability());
    assertSame(FlussCatalog.CATALOG_PROPERTIES_METADATA, catalog.catalogPropertiesMetadata());
    assertSame(FlussCatalog.SCHEMA_PROPERTIES_METADATA, catalog.schemaPropertiesMetadata());
    assertSame(FlussCatalog.TABLE_PROPERTIES_METADATA, catalog.tablePropertiesMetadata());
  }

  @Test
  void testNewOpsCreatesFlussCatalogOperations() {
    TestableFlussCatalog catalog = new TestableFlussCatalog();

    assertInstanceOf(FlussCatalogOperations.class, catalog.newOpsForTest(Map.of()));
  }

  private static class TestableFlussCatalog extends FlussCatalog {
    private CatalogOperations newOpsForTest(Map<String, String> config) {
      return newOps(config);
    }
  }
}
