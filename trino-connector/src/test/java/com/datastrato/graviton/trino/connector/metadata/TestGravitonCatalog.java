/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.CatalogDTO;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitonCatalog {

  @Test
  void testGravitonCatalog() {
    String catalogName = "mock";
    String provider = "hive";
    CatalogDTO mockCatalog =
        new CatalogDTO.Builder()
            .withName(catalogName)
            .withComment("comment")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider(provider)
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    GravitonCatalog catalog = new GravitonCatalog(mockCatalog);
    Assertions.assertEquals(catalogName, catalog.getName());
    Assertions.assertEquals(provider, catalog.getProvider());
  }
}
