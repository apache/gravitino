/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Catalog;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.Test;

public class TestGravitinoCatalog {

  @Test
  public void testGravitinoCatalog() {
    String catalogName = "mock";
    String provider = "hive";
    Catalog mockCatalog =
        mockCatalog(
            catalogName, provider, "test catalog", Catalog.Type.RELATIONAL, Collections.emptyMap());
    GravitinoCatalog catalog = new GravitinoCatalog(mockCatalog);
    assertEquals(catalogName, catalog.getName());
    assertEquals(provider, catalog.getProvider());
  }

  public static Catalog mockCatalog(
      String name,
      String provider,
      String comments,
      Catalog.Type type,
      Map<String, String> properties) {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn(name);
    when(mockCatalog.provider()).thenReturn(provider);
    when(mockCatalog.comment()).thenReturn(comments);
    when(mockCatalog.type()).thenReturn(type);
    when(mockCatalog.properties()).thenReturn(properties);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockCatalog.auditInfo()).thenReturn(mockAudit);
    return mockCatalog;
  }
}
