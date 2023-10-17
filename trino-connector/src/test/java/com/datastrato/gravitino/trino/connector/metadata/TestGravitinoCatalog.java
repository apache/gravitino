/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoCatalog {

  @Test
  void testGravitinoCatalog() {
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
    GravitinoCatalog catalog = new GravitinoCatalog(mockCatalog);
    Assertions.assertEquals(catalogName, catalog.getName());
    Assertions.assertEquals(provider, catalog.getProvider());
  }
}
