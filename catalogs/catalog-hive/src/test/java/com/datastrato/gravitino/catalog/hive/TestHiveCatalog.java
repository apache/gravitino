/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalog.CATALOG_PROPERTIES_METADATA;
import static com.datastrato.gravitino.catalog.hive.HiveCatalog.SCHEMA_PROPERTIES_METADATA;
import static com.datastrato.gravitino.catalog.hive.HiveCatalog.TABLE_PROPERTIES_METADATA;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.PropertiesMetadataHelpers;
import com.datastrato.gravitino.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.gravitino.connector.HasPropertyMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveCatalog extends MiniHiveMetastoreService {
  public static final HasPropertyMetadata HIVE_PROPERTIES_METADATA =
      new HasPropertyMetadata() {
        @Override
        public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
          return TABLE_PROPERTIES_METADATA;
        }

        @Override
        public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
          return CATALOG_PROPERTIES_METADATA;
        }

        @Override
        public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
          return SCHEMA_PROPERTIES_METADATA;
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

  @Test
  public void testListDatabases() throws TException, InterruptedException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(HiveCatalog.Type.RELATIONAL)
            .withProvider("hive")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    metastore.hiveConf().forEach(e -> conf.put(e.getKey(), e.getValue()));

    try (HiveCatalogOperations ops = new HiveCatalogOperations()) {
      ops.initialize(conf, entity.toCatalogInfo(), HIVE_PROPERTIES_METADATA);
      List<String> dbs = ops.clientPool.run(IMetaStoreClient::getAllDatabases);
      Assertions.assertEquals(2, dbs.size());
      Assertions.assertTrue(dbs.contains("default"));
      Assertions.assertTrue(dbs.contains(DB_NAME));
    }
  }

  @Test
  void testCatalogProperty() {
    Map<String, String> properties = Maps.newHashMap();
    metastore.hiveConf().forEach(e -> properties.put(e.getKey(), e.getValue()));
    PropertiesMetadata catalogPropertiesMetadata =
        HIVE_PROPERTIES_METADATA.catalogPropertiesMetadata();

    Assertions.assertDoesNotThrow(
        () -> {
          Map<String, String> map = Maps.newHashMap();
          map.put(METASTORE_URIS, "/tmp");
          PropertiesMetadataHelpers.validatePropertyForCreate(catalogPropertiesMetadata, map);
        });

    Throwable throwable =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForCreate(
                    catalogPropertiesMetadata, properties));

    Assertions.assertTrue(
        throwable
            .getMessage()
            .contains(
                String.format("Properties are required and must be set: [%s]", METASTORE_URIS)));
  }
}
