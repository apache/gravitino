/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.connector.BaseTable;
import com.datastrato.gravitino.connector.TableOperations;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class BaseTableExtension extends BaseTable {

  private BaseTableExtension() {}

  @Override
  protected TableOperations newOps() {
    throw new UnsupportedOperationException("BaseTableExtension does not support TableOperations.");
  }

  public static class Builder extends BaseTableBuilder<Builder, BaseTableExtension> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected BaseTableExtension internalBuild() {
      BaseTableExtension table = new BaseTableExtension();
      table.name = name;
      table.comment = comment;
      table.properties = properties;
      table.auditInfo = auditInfo;
      table.columns = columns;
      table.partitioning = partitioning;
      return table;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}

public class TestBaseTable {

  @Test
  void testTableFields() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("Justin").withCreateTime(Instant.now()).build();

    BaseTable table =
        BaseTableExtension.builder()
            .withName("testTableName")
            .withComment("testTableComment")
            .withColumns(new Column[0])
            .withProperties(properties)
            .withPartitioning(new Transform[0])
            .withAuditInfo(auditInfo)
            .build();

    assertEquals("testTableName", table.name());
    assertEquals("testTableComment", table.comment());
    assertEquals(properties, table.properties());
    assertEquals(auditInfo, table.auditInfo());
    assertArrayEquals(new Column[0], table.columns());
    assertArrayEquals(new Transform[0], table.partitioning());
  }
}
