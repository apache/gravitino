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
package org.apache.gravitino.catalog.lakehouse.paimon;

import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalog.CATALOG_PROPERTIES_META;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalog.SCHEMA_PROPERTIES_META;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalog.TABLE_PROPERTIES_META;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonCatalog {

  static final HasPropertyMetadata PAIMON_PROPERTIES_METADATA =
      new HasPropertyMetadata() {

        @Override
        public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
          return TABLE_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
          return CATALOG_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
          return SCHEMA_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Fileset properties are not supported");
        }

        @Override
        public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Topic properties are not supported");
        }

        @Override
        public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Model properties are not supported");
        }

        @Override
        public PropertiesMetadata modelVersionPropertiesMetadata()
            throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Does not support model version properties");
        }
      };

  private static String tempDir =
      String.join(File.separator, System.getProperty("java.io.tmpdir"), "paimon_catalog_warehouse");

  @AfterAll
  public static void clean() {
    try {
      FileUtils.deleteDirectory(new File(tempDir));
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  public void testCatalogOperation() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    conf.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
    conf.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, tempDir);
    PaimonCatalog paimonCatalog =
        new PaimonCatalog().withCatalogConf(conf).withCatalogEntity(entity);
    CatalogOperations catalogOperations = paimonCatalog.ops();
    Assertions.assertInstanceOf(PaimonCatalogOperations.class, catalogOperations);

    PaimonCatalogOperations paimonCatalogOperations = (PaimonCatalogOperations) catalogOperations;
    PaimonCatalogOps paimonCatalogOps = paimonCatalogOperations.paimonCatalogOps;
    Assertions.assertEquals(
        paimonCatalogOperations.listSchemas(Namespace.empty()).length,
        paimonCatalogOps.listDatabases().size());

    // test testConnection
    Assertions.assertDoesNotThrow(
        () ->
            paimonCatalogOperations.testConnection(
                NameIdentifier.of("metalake", "catalog"),
                Catalog.Type.RELATIONAL,
                "paimon",
                "comment",
                ImmutableMap.of()));
  }

  @Test
  void testCatalogProperty() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    conf.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
    conf.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, tempDir);
    try (PaimonCatalogOperations ops = new PaimonCatalogOperations()) {
      ops.initialize(conf, entity.toCatalogInfo(), PAIMON_PROPERTIES_METADATA);
      Map<String, String> map1 = Maps.newHashMap();
      map1.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "test");
      PropertiesMetadata metadata = PAIMON_PROPERTIES_METADATA.catalogPropertiesMetadata();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map1));

      Map<String, String> map2 = Maps.newHashMap();
      map2.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
      map2.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, "test");
      map2.put(PaimonCatalogPropertiesMetadata.URI, "127.0.0.1");
      Assertions.assertDoesNotThrow(
          () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map2));

      Map<String, String> map3 = Maps.newHashMap();
      Throwable throwable =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map3));

      Assertions.assertTrue(
          throwable
              .getMessage()
              .contains(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND));
    }
  }
}
