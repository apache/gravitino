/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
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
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        new CatalogEntity.Builder()
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
}
