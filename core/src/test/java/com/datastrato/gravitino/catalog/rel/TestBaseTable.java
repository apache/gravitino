/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.rel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.transforms.Transform;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class BaseTableExtension extends BaseTable {

  private BaseTableExtension() {}

  public static class Builder extends BaseTableBuilder<Builder, BaseTableExtension> {

    @Override
    protected BaseTableExtension internalBuild() {
      BaseTableExtension table = new BaseTableExtension();
      table.name = name;
      table.comment = comment;
      table.properties = properties;
      table.auditInfo = auditInfo;
      table.columns = columns;
      table.partitions = partitions;
      return table;
    }
  }
}

public class TestBaseTable {

  @Test
  void testTableFields() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("Justin").withCreateTime(Instant.now()).build();

    BaseTable table =
        new BaseTableExtension.Builder()
            .withName("testTableName")
            .withComment("testTableComment")
            .withColumns(new Column[0])
            .withProperties(properties)
            .withPartitions(new Transform[0])
            .withAuditInfo(auditInfo)
            .build();

    assertEquals("testTableName", table.name());
    assertEquals("testTableComment", table.comment());
    assertEquals(table.properties, table.properties());
    assertEquals(table.auditInfo, table.auditInfo());
    assertArrayEquals(new Column[0], table.columns);
    assertArrayEquals(new Transform[0], table.partitions);
  }
}
