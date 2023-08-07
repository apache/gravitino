/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta.rel;

import static org.junit.jupiter.api.Assertions.*;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Column;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BaseTableTest {

  private final class BaseTableExtension extends BaseTable {
    @Override
    public void validate() {}
  }

  @Test
  void testTableFields() {
    BaseTable table = new BaseTableExtension();
    table.id = 1L;
    table.schemaId = 2L;
    table.name = "testTableName";
    table.comment = "testTableComment";
    table.properties = new HashMap<>();
    table.properties.put("key1", "value1");
    table.properties.put("key2", "value2");
    table.auditInfo =
        new AuditInfo.Builder().withCreator("Justin").withCreateTime(Instant.now()).build();
    table.columns = new Column[0];

    Map<Field, Object> expectedFields = Maps.newHashMap();
    expectedFields.put(BaseTable.ID, 1L);
    expectedFields.put(BaseTable.SCHEMA_ID, 2L);
    expectedFields.put(BaseTable.NAME, "testTableName");
    expectedFields.put(BaseTable.COMMENT, "testTableComment");
    expectedFields.put(BaseTable.PROPERTIES, table.properties);
    expectedFields.put(BaseTable.AUDIT_INFO, table.auditInfo);
    expectedFields.put(BaseTable.COLUMNS, table.columns);

    assertEquals(1L, table.fields().get(BaseTable.ID));
    assertEquals(2L, table.fields().get(BaseTable.SCHEMA_ID));
    assertEquals("testTableName", table.fields().get(BaseTable.NAME));
    assertEquals("testTableComment", table.fields().get(BaseTable.COMMENT));
    assertEquals(table.properties, table.fields().get(BaseTable.PROPERTIES));
    assertEquals(table.auditInfo, table.fields().get(BaseTable.AUDIT_INFO));
    assertArrayEquals(table.columns, (Column[]) table.fields().get(BaseTable.COLUMNS));
  }

  @Test
  void testTableType() {
    BaseTable table = new BaseTableExtension();

    assertEquals(Entity.EntityType.TABLE, table.type());
  }

  @Test
  void testEqualsAndHashCode() {
    Instant now = Instant.now();
    BaseTable table1 = new BaseTableExtension();
    table1.id = 1L;
    table1.schemaId = 2L;
    table1.name = "testTableName";
    table1.comment = "testTableComment";
    table1.properties = new HashMap<>();
    table1.properties.put("key1", "value1");
    table1.properties.put("key2", "value2");
    table1.auditInfo = new AuditInfo.Builder().withCreator("Justin").withCreateTime(now).build();
    table1.columns = new Column[0];

    BaseTable table2 = new BaseTableExtension();
    table2.id = 1L;
    table2.schemaId = 2L;
    table2.name = "testTableName";
    table2.comment = "testTableComment";
    table2.properties = new HashMap<>();
    table2.properties.put("key1", "value1");
    table2.properties.put("key2", "value2");
    table2.auditInfo = new AuditInfo.Builder().withCreator("Justin").withCreateTime(now).build();
    table2.columns = new Column[0];

    assertEquals(table1, table2);
    assertEquals(table1.hashCode(), table2.hashCode());
  }
}
