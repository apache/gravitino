/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalog.CATALOG_PROPERTIES_META;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalog.SCHEMA_PROPERTIES_META;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.PropertiesMetadataHelpers;
import com.datastrato.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.HasPropertyMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonCatalog {

  static final HasPropertyMetadata PAIMON_PROPERTIES_METADATA =
      new HasPropertyMetadata() {

        @Override
        public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Table properties are not supported");
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
      };

  private String tempDir =
      String.join(File.separator, System.getProperty("java.io.tmpdir"), "paimon_catalog_warehouse");

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
