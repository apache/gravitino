/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalog {
  @Test
  public void testListDatabases() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    IcebergCatalog icebergCatalog =
        new IcebergCatalog().withCatalogConf(conf).withCatalogEntity(entity);
    CatalogOperations catalogOperations = icebergCatalog.newOps(Maps.newHashMap());
    Assertions.assertTrue(catalogOperations instanceof IcebergCatalogOperations);

    IcebergTableOps icebergTableOps =
        ((IcebergCatalogOperations) catalogOperations).icebergTableOps;
    ListNamespacesResponse listNamespacesResponse =
        icebergTableOps.listNamespace(org.apache.iceberg.catalog.Namespace.empty());
    Assertions.assertTrue(listNamespacesResponse.namespaces().isEmpty());
  }

  @Test
  void testCatalogProperty() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();

    try (IcebergCatalogOperations ops = new IcebergCatalogOperations(entity)) {
      ops.initialize(conf);
      Map<String, String> map1 = Maps.newHashMap();
      map1.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND_NAME, "test");
      PropertiesMetadata metadata = ops.catalogPropertiesMetadata();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> {
            metadata.validatePropertyForCreate(map1);
          });

      Map<String, String> map2 = Maps.newHashMap();
      map2.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND_NAME, "hive");
      map2.put(IcebergCatalogPropertiesMetadata.URI, "127.0.0.1");
      map2.put(IcebergCatalogPropertiesMetadata.WAREHOUSE, "test");
      Assertions.assertDoesNotThrow(
          () -> {
            metadata.validatePropertyForCreate(map2);
          });

      Map<String, String> map3 = Maps.newHashMap();
      Throwable throwable =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> metadata.validatePropertyForCreate(map3));

      Assertions.assertTrue(
          throwable.getMessage().contains(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND_NAME));
    }
  }
}
