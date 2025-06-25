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
package org.apache.gravitino.catalog.lakehouse.hudi;

import static org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata.CATALOG_BACKEND;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.hudi.backend.hms.HudiHMSBackendOps;
import org.apache.gravitino.catalog.lakehouse.hudi.ops.InMemoryBackendOps;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHudiCatalogOperations {

  private static final HasPropertyMetadata HUDI_PROPERTIES_METADATA =
      new HasPropertyMetadata() {
        @Override
        public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
          return HudiCatalog.TABLE_PROPERTIES_METADATA;
        }

        @Override
        public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
          return HudiCatalog.CATALOG_PROPERTIES_METADATA;
        }

        @Override
        public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
          return HudiCatalog.SCHEMA_PROPERTIES_METADATA;
        }

        @Override
        public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException();
        }

        @Override
        public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException();
        }

        @Override
        public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException();
        }

        @Override
        public PropertiesMetadata modelVersionPropertiesMetadata()
            throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Does not support model version properties");
        }
      };

  @Test
  public void testInitialize() {
    HudiCatalogOperations ops = new HudiCatalogOperations();
    ops.initialize(ImmutableMap.of(CATALOG_BACKEND, "hms"), null, HUDI_PROPERTIES_METADATA);
    Assertions.assertInstanceOf(HudiHMSBackendOps.class, ops.hudiCatalogBackendOps);
    ops.close();
  }

  @Test
  public void testTestConnection() throws Exception {
    try (HudiCatalogOperations ops = new HudiCatalogOperations();
        InMemoryBackendOps inMemoryBackendOps = new InMemoryBackendOps()) {
      ops.hudiCatalogBackendOps = inMemoryBackendOps;

      Assertions.assertDoesNotThrow(
          () ->
              ops.testConnection(NameIdentifier.of("metalake", "catalog"), null, null, null, null));
    }
  }

  @Test
  public void testListSchemas() throws Exception {
    try (HudiCatalogOperations ops = new HudiCatalogOperations();
        InMemoryBackendOps inMemoryBackendOps = new InMemoryBackendOps()) {
      ops.hudiCatalogBackendOps = inMemoryBackendOps;

      NameIdentifier[] schemas = ops.listSchemas(null);
      Assertions.assertEquals(0, schemas.length);
    }
  }

  @Test
  public void testLoadSchema() throws Exception {
    try (HudiCatalogOperations ops = new HudiCatalogOperations();
        InMemoryBackendOps inMemoryBackendOps = new InMemoryBackendOps()) {
      ops.hudiCatalogBackendOps = inMemoryBackendOps;

      Assertions.assertThrows(
          NoSuchSchemaException.class,
          () -> ops.loadSchema(NameIdentifier.of("metalake", "catalog", "schema")));
    }
  }

  @Test
  public void testListTables() throws Exception {
    try (HudiCatalogOperations ops = new HudiCatalogOperations();
        InMemoryBackendOps inMemoryBackendOps = new InMemoryBackendOps()) {
      ops.hudiCatalogBackendOps = inMemoryBackendOps;

      NameIdentifier[] tables = ops.listTables(null);
      Assertions.assertEquals(0, tables.length);
    }
  }

  @Test
  public void testLoadTable() throws Exception {
    try (HudiCatalogOperations ops = new HudiCatalogOperations();
        InMemoryBackendOps inMemoryBackendOps = new InMemoryBackendOps()) {
      ops.hudiCatalogBackendOps = inMemoryBackendOps;

      Assertions.assertThrows(
          NoSuchTableException.class,
          () -> ops.loadTable(NameIdentifier.of("metalake", "catalog", "table")));
    }
  }
}
